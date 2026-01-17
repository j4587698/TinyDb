using System;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Tests.Utils;
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
            .Select(g => new { Category = g.Key, Count = g.Count(), TotalPrice = g.Sum(p => p.Price) })
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

    public void Dispose()
    {
        _engine?.Dispose();
        if (System.IO.File.Exists(_dbPath))
        {
            try { System.IO.File.Delete(_dbPath); } catch { }
        }
    }

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
}
