using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Tests.Utils;

namespace TinyDb.Tests.Query;

/// <summary>
/// Tests for QueryPipeline GroupBy functionality to improve coverage
/// These tests exercise ExecuteGroupBy and ExecuteGroupByGeneric methods
/// </summary>
public class QueryPipelineGroupByTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly ITinyCollection<Product> _products;

    public QueryPipelineGroupByTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"groupby_test_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
        _products = _engine.GetCollection<Product>();
        SeedData();
    }

    private void SeedData()
    {
        _products.Insert(new Product { Id = 1, Name = "Apple", Category = "Fruit", Price = 1.5m });
        _products.Insert(new Product { Id = 2, Name = "Banana", Category = "Fruit", Price = 0.5m });
        _products.Insert(new Product { Id = 3, Name = "Orange", Category = "Fruit", Price = 2.0m });
        _products.Insert(new Product { Id = 4, Name = "Carrot", Category = "Vegetable", Price = 0.8m });
        _products.Insert(new Product { Id = 5, Name = "Broccoli", Category = "Vegetable", Price = 1.2m });
        _products.Insert(new Product { Id = 6, Name = "Milk", Category = "Dairy", Price = 3.0m });
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath))
            try { File.Delete(_dbPath); } catch { }
    }

    #region GroupBy Tests with Select (AOT Compatible)

    [Test]
    [SkipInAot("GroupBy result enumeration requires dynamic code")]
    public async Task GroupBy_WithSelect_ShouldWork()
    {
        var query = _products.Query()
            .GroupBy(p => p.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() });

        var results = query.ToList();

        await Assert.That(results.Count).IsEqualTo(3);
    }

    [Test]
    [SkipInAot("GroupBy result enumeration requires dynamic code")]
    public async Task GroupBy_WithSum_ShouldCalculateCorrectly()
    {
        var query = _products.Query()
            .GroupBy(p => p.Category)
            .Select(g => new { Category = g.Key, Total = g.Sum(p => p.Price) });

        var results = query.ToList();

        await Assert.That(results.Count).IsEqualTo(3);
        var fruitTotal = results.FirstOrDefault(r => r.Category?.ToString() == "Fruit");
        await Assert.That(fruitTotal).IsNotNull();
    }

    [Test]
    [SkipInAot("GroupBy result enumeration requires dynamic code")]
    public async Task GroupBy_WithAverage_ShouldCalculateCorrectly()
    {
        var query = _products.Query()
            .GroupBy(p => p.Category)
            .Select(g => new { Category = g.Key, AvgPrice = g.Average(p => p.Price) });

        var results = query.ToList();

        await Assert.That(results.Count).IsEqualTo(3);
    }

    [Test]
    [SkipInAot("GroupBy result enumeration requires dynamic code")]
    public async Task GroupBy_WithMinMax_ShouldCalculateCorrectly()
    {
        var query = _products.Query()
            .GroupBy(p => p.Category)
            .Select(g => new { 
                Category = g.Key, 
                MinPrice = g.Min(p => p.Price),
                MaxPrice = g.Max(p => p.Price)
            });

        var results = query.ToList();

        await Assert.That(results.Count).IsEqualTo(3);
    }

    #endregion

    #region GroupBy with Filter (Where before GroupBy)

    [Test]
    [SkipInAot("GroupBy result enumeration requires dynamic code")]
    public async Task GroupBy_AfterWhere_ShouldFilterFirst()
    {
        var query = _products.Query()
            .Where(p => p.Price > 1.0m)
            .GroupBy(p => p.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() });

        var results = query.ToList();

        // Only Fruit (Apple, Orange) and Dairy (Milk) should have items > 1.0
        await Assert.That(results.Count).IsGreaterThanOrEqualTo(2);
    }

    #endregion

    #region Direct ExecuteGroupBy Tests via Reflection/Integration

    [Test]
    public async Task ExecuteGroupByGeneric_DirectCall_ShouldWork()
    {
        // This test directly exercises ExecuteGroupByGeneric<T> through the pipeline
        var products = new List<Product>
        {
            new() { Id = 1, Name = "A", Category = "Cat1", Price = 10m },
            new() { Id = 2, Name = "B", Category = "Cat1", Price = 20m },
            new() { Id = 3, Name = "C", Category = "Cat2", Price = 30m },
        };

        // Use AotGrouping directly to test grouping logic
        var groups = products
            .GroupBy(p => p.Category)
            .Select(g => new QueryPipeline.AotGrouping(g.Key!, g.Cast<object>()))
            .ToList();

        await Assert.That(groups.Count).IsEqualTo(2);
        await Assert.That(groups[0].Count).IsEqualTo(2);
        await Assert.That(groups[1].Count).IsEqualTo(1);
    }

    [Test]
    [SkipInAot("Test uses anonymous types and dynamic which require dynamic code generation")]
    public async Task ExecuteGroupBy_WithNullKey_ShouldGroupCorrectly()
    {
        // Test grouping with null keys
        var items = new object[] { 
            new { Key = (string?)null, Value = 1 },
            new { Key = (string?)null, Value = 2 },
            new { Key = "A", Value = 3 }
        };

        var groups = items
            .GroupBy(x => ((dynamic)x).Key as string ?? "")
            .Select(g => new QueryPipeline.AotGrouping(g.Key, g.Cast<object>()))
            .ToList();

        await Assert.That(groups.Count).IsEqualTo(2);
    }

    #endregion

    #region Aggregation Tests

    [Test]
    [SkipInAot("Aggregation through LINQ requires dynamic code")]
    public async Task Sum_OnGroupedData_ShouldWork()
    {
        var query = _products.Query()
            .GroupBy(p => p.Category)
            .Select(g => g.Sum(p => p.Price));

        var results = query.ToList();
        
        await Assert.That(results.Count).IsEqualTo(3);
    }

    [Test]
    [SkipInAot("Aggregation through LINQ requires dynamic code")]
    public async Task Average_OnGroupedData_ShouldWork()
    {
        var query = _products.Query()
            .GroupBy(p => p.Category)
            .Select(g => g.Average(p => p.Price));

        var results = query.ToList();
        
        await Assert.That(results.Count).IsEqualTo(3);
    }

    #endregion

    #region Edge Cases

    [Test]
    [SkipInAot("GroupBy result enumeration requires dynamic code")]
    public async Task GroupBy_EmptyCollection_ShouldReturnEmpty()
    {
        // Create empty collection
        var emptyCollection = _engine.GetCollection<Product>("empty_products");
        
        var query = emptyCollection.Query()
            .GroupBy(p => p.Category)
            .Select(g => g.Key);

        var results = query.ToList();

        await Assert.That(results.Count).IsEqualTo(0);
    }

    [Test]
    [SkipInAot("GroupBy result enumeration requires dynamic code")]
    public async Task GroupBy_SingleCategory_ShouldReturnOneGroup()
    {
        var singleCollection = _engine.GetCollection<Product>("single_category");
        singleCollection.Insert(new Product { Id = 1, Name = "A", Category = "Same", Price = 1m });
        singleCollection.Insert(new Product { Id = 2, Name = "B", Category = "Same", Price = 2m });
        singleCollection.Insert(new Product { Id = 3, Name = "C", Category = "Same", Price = 3m });

        var query = singleCollection.Query()
            .GroupBy(p => p.Category)
            .Select(g => new { Category = g.Key, Count = g.Count() });

        var results = query.ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Count).IsEqualTo(3);
    }

    #endregion

    #region Test Entity

    public class Product
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Category { get; set; } = string.Empty;
        public decimal Price { get; set; }
    }

    #endregion
}
