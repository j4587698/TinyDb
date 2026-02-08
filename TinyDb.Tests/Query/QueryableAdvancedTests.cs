using System;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Tests.Utils;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryableAdvancedTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly ITinyCollection<Product> _products;

    public QueryableAdvancedTests()
    {
        _dbPath = $"query_advanced_{Guid.NewGuid()}.db";
        _engine = new TinyDbEngine(_dbPath);
        _products = _engine.GetCollection<Product>();
        
        // Seed data
        _products.Insert(new Product { Id = ObjectId.NewObjectId(), Name = "Laptop", Category = "Electronics", Price = 1000, InStock = true });
        _products.Insert(new Product { Id = ObjectId.NewObjectId(), Name = "Mouse", Category = "Electronics", Price = 50, InStock = true });
        _products.Insert(new Product { Id = ObjectId.NewObjectId(), Name = "Keyboard", Category = "Electronics", Price = 80, InStock = false });
        _products.Insert(new Product { Id = ObjectId.NewObjectId(), Name = "Chair", Category = "Furniture", Price = 150, InStock = true });
        _products.Insert(new Product { Id = ObjectId.NewObjectId(), Name = "Desk", Category = "Furniture", Price = 200, InStock = true });
    }

    [Test]
    public async Task GroupBy_Should_Group_By_Category()
    {
        var groups = _products.Query()
            .GroupBy(p => p.Category)
            .Select(g => new CategoryGroupSummary { Category = g.Key == null ? "" : g.Key.ToString(), Count = g.Count(), TotalPrice = g.Sum(p => p.Price) })
            .OrderBy(x => x.Category)
            .ToList();

        await Assert.That(groups.Count).IsEqualTo(2);
        await Assert.That(groups[0].Category).IsEqualTo("Electronics");
        await Assert.That(groups[0].Count).IsEqualTo(3);
        await Assert.That(groups[0].TotalPrice).IsEqualTo(1130);
        
        await Assert.That(groups[1].Category).IsEqualTo("Furniture");
        await Assert.That(groups[1].Count).IsEqualTo(2);
        await Assert.That(groups[1].TotalPrice).IsEqualTo(350);
    }

    [Test]
    public async Task Complex_Projection_Should_Work()
    {
        var dtos = _products.Query()
            .Where(p => p.Price > 100)
            .OrderByDescending(p => p.Price)
            .Select(p => new ProductDto { DisplayName = p.Category + ": " + p.Name, Cost = p.Price })
            .ToList();

        await Assert.That(dtos.Count).IsEqualTo(3);
        await Assert.That(dtos[0].DisplayName).IsEqualTo("Electronics: Laptop");
        await Assert.That(dtos[0].Cost).IsEqualTo(1000);
        await Assert.That(dtos[1].DisplayName).IsEqualTo("Furniture: Desk");
        await Assert.That(dtos[2].DisplayName).IsEqualTo("Furniture: Chair");
    }

    [Test]
    public async Task Distinct_Should_Return_Distinct_Categories()
    {
        var categories = _products.Query()
            .Select(p => p.Category)
            .Distinct()
            .OrderBy(c => c)
            .ToList();

        await Assert.That(categories.Count).IsEqualTo(2);
        await Assert.That(categories[0]).IsEqualTo("Electronics");
        await Assert.That(categories[1]).IsEqualTo("Furniture");
    }

    [Test]
    public async Task Any_With_Predicate_Should_Work()
    {
        bool hasExpensiveElectronics = _products.Query()
            .Any(p => p.Category == "Electronics" && p.Price > 900);
            
        await Assert.That(hasExpensiveElectronics).IsTrue();
        
        bool hasCheapFurniture = _products.Query()
            .Any(p => p.Category == "Furniture" && p.Price < 10);
            
        await Assert.That(hasCheapFurniture).IsFalse();
    }

    [Test]
    public async Task All_With_Predicate_Should_Work()
    {
        bool allPositivePrice = _products.Query().All(p => p.Price > 0);
        await Assert.That(allPositivePrice).IsTrue();
        
        bool allInStock = _products.Query().All(p => p.InStock);
        await Assert.That(allInStock).IsFalse();
    }

    [Test]
    public async Task OrderBy_ThenBy_Should_Sort_Correctly()
    {
        // 验证 ThenBy 是否能被正确重写为 Enumerable.ThenBy
        var sorted = _products.Query()
            .OrderBy(p => p.Category)
            .ThenByDescending(p => p.Price)
            .ToList();

        await Assert.That(sorted.Count).IsEqualTo(5);
        
        // Electronics (3 items): Laptop(1000), Keyboard(80), Mouse(50)
        await Assert.That(sorted[0].Name).IsEqualTo("Laptop");
        await Assert.That(sorted[1].Name).IsEqualTo("Keyboard");
        await Assert.That(sorted[2].Name).IsEqualTo("Mouse");
        
        // Furniture (2 items): Desk(200), Chair(150)
        await Assert.That(sorted[3].Name).IsEqualTo("Desk");
        await Assert.That(sorted[4].Name).IsEqualTo("Chair");
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (System.IO.File.Exists(_dbPath))
        {
            try { System.IO.File.Delete(_dbPath); } catch { }
        }
    }

    [Entity]
    public class Product
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = "";
        public string Category { get; set; } = "";
        public decimal Price { get; set; }
        public bool InStock { get; set; }
    }

    public class ProductDto
    {
        public string DisplayName { get; set; } = "";
        public decimal Cost { get; set; }
    }

    public class CategoryGroupSummary
    {
        public string Category { get; set; } = "";
        public int Count { get; set; }
        public decimal TotalPrice { get; set; }
    }
}
