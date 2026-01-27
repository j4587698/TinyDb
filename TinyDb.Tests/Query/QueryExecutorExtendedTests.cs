using System.Linq.Expressions;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Attributes;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExecutorExtendedTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryExecutorExtendedTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "TinyDbQueryExtTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
        _testDbPath = Path.Combine(_testDirectory, "test.db");

        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        try { if (Directory.Exists(_testDirectory)) Directory.Delete(_testDirectory, true); } catch { }
    }

    [Test]
    public async Task Execute_With_Or_Condition_Should_Work()
    {
        var col = _engine.GetCollectionWithName<QueryExtendedProduct>("Products");
        col.Insert(new QueryExtendedProduct { Name = "A", Category = "Cat1" });
        col.Insert(new QueryExtendedProduct { Name = "B", Category = "Cat2" });
        col.Insert(new QueryExtendedProduct { Name = "C", Category = "Cat3" });

        var results = _executor.Execute<QueryExtendedProduct>("Products", p => p.Category == "Cat1" || p.Category == "Cat2").ToList();

        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results.Any(p => p.Name == "A")).IsTrue();
        await Assert.That(results.Any(p => p.Name == "B")).IsTrue();
    }

    [Test]
    public async Task Execute_With_NotEqual_Should_Work()
    {
        var col = _engine.GetCollectionWithName<QueryExtendedProduct>("Products");
        col.Insert(new QueryExtendedProduct { Name = "A", Stock = 10 });
        col.Insert(new QueryExtendedProduct { Name = "B", Stock = 0 });

        var results = _executor.Execute<QueryExtendedProduct>("Products", p => p.Stock != 0).ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("A");
    }

    [Test]
    public async Task Execute_With_LessThan_Should_Work()
    {
        var col = _engine.GetCollectionWithName<QueryExtendedProduct>("Products");
        col.Insert(new QueryExtendedProduct { Name = "A", Price = 10 });
        col.Insert(new QueryExtendedProduct { Name = "B", Price = 20 });

        var results = _executor.Execute<QueryExtendedProduct>("Products", p => p.Price < 15).ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("A");
    }

    [Test]
    public async Task Execute_With_LessThanOrEqual_Should_Work()
    {
        var col = _engine.GetCollectionWithName<QueryExtendedProduct>("Products");
        col.Insert(new QueryExtendedProduct { Name = "A", Price = 10 });
        col.Insert(new QueryExtendedProduct { Name = "B", Price = 20 });

        var results = _executor.Execute<QueryExtendedProduct>("Products", p => p.Price <= 10).ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("A");
    }

    [Test]
    public async Task Execute_With_String_StartsWith_Should_Work()
    {
        var col = _engine.GetCollectionWithName<QueryExtendedProduct>("Products");
        col.Insert(new QueryExtendedProduct { Name = "Apple" });
        col.Insert(new QueryExtendedProduct { Name = "Banana" });

        var results = _executor.Execute<QueryExtendedProduct>("Products", p => p.Name!.StartsWith("App")).ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("Apple");
    }

    [Test]
    public async Task Execute_With_String_EndsWith_Should_Work()
    {
        var col = _engine.GetCollectionWithName<QueryExtendedProduct>("Products");
        col.Insert(new QueryExtendedProduct { Name = "Apple" });
        col.Insert(new QueryExtendedProduct { Name = "Banana" });

        var results = _executor.Execute<QueryExtendedProduct>("Products", p => p.Name!.EndsWith("nan a")).ToList(); // Typo in string? No "nan a" -> "nana"

        var results2 = _executor.Execute<QueryExtendedProduct>("Products", p => p.Name!.EndsWith("nana")).ToList();
        await Assert.That(results2.Count).IsEqualTo(1);
        await Assert.That(results2[0].Name).IsEqualTo("Banana");
    }

    [Test]
    public async Task Execute_With_String_Contains_Should_Work()
    {
        var col = _engine.GetCollectionWithName<QueryExtendedProduct>("Products");
        col.Insert(new QueryExtendedProduct { Name = "Pineapple" });
        col.Insert(new QueryExtendedProduct { Name = "Banana" });

        var results = _executor.Execute<QueryExtendedProduct>("Products", p => p.Name!.Contains("eapp")).ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("Pineapple");
    }

    [Test]
    public async Task Execute_With_List_Contains_Should_Work()
    {
        var col = _engine.GetCollectionWithName<QueryExtendedProduct>("Products");
        col.Insert(new QueryExtendedProduct { Name = "A", Tags = new List<string> { "Red", "Fruit" } });
        col.Insert(new QueryExtendedProduct { Name = "B", Tags = new List<string> { "Yellow", "Fruit" } });

        // Note: Linq Expression for List.Contains might be compiled to instance method call
        var results = _executor.Execute<QueryExtendedProduct>("Products", p => p.Tags!.Contains("Red")).ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("A");
    }
}

[Entity("Products")]
public class QueryExtendedProduct
{
    public ObjectId? Id { get; set; }
    public string? Name { get; set; }
    public string? Category { get; set; }
    public decimal Price { get; set; }
    public int Stock { get; set; }
    public bool IsActive { get; set; }
    public List<string>? Tags { get; set; }
}
