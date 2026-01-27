using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Index;

namespace TinyDb.Tests.Query;

public class ComplexQueryTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public ComplexQueryTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "TinyDbComplexQueryTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
        _testDbPath = Path.Combine(_testDirectory, "test.db");

        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (Directory.Exists(_testDirectory))
        {
            try { Directory.Delete(_testDirectory, true); } catch { }
        }
    }

    [Test]
    public async Task Query_With_String_Functions_Should_Work()
    {
        var collection = _engine.GetCollection<ComplexQueryProduct>();
        collection.Insert(new ComplexQueryProduct { Id = 1, Name = "Apple iPhone", Category = "Electronics" });
        collection.Insert(new ComplexQueryProduct { Id = 2, Name = "Samsung Galaxy", Category = "Electronics" });
        collection.Insert(new ComplexQueryProduct { Id = 3, Name = "Banana", Category = "Food" });

        // StartsWith
        var startsWith = _executor.Execute<ComplexQueryProduct>("Products", p => p.Name.StartsWith("Apple")).ToList();
        await Assert.That(startsWith.Count).IsEqualTo(1);
        await Assert.That(startsWith[0].Name).IsEqualTo("Apple iPhone");

        // EndsWith
        var endsWith = _executor.Execute<ComplexQueryProduct>("Products", p => p.Name.EndsWith("Galaxy")).ToList();
        await Assert.That(endsWith.Count).IsEqualTo(1);
        await Assert.That(endsWith[0].Name).IsEqualTo("Samsung Galaxy");

        // Contains
        var contains = _executor.Execute<ComplexQueryProduct>("Products", p => p.Category.Contains("Electro")).ToList();
        await Assert.That(contains.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Query_With_Or_Condition_Should_Work()
    {
        var collection = _engine.GetCollection<ComplexQueryProduct>();
        collection.Insert(new ComplexQueryProduct { Id = 1, Price = 100 });
        collection.Insert(new ComplexQueryProduct { Id = 2, Price = 200 });
        collection.Insert(new ComplexQueryProduct { Id = 3, Price = 300 });

        var results = _executor.Execute<ComplexQueryProduct>("Products", p => p.Price < 150 || p.Price > 250).ToList();
        await Assert.That(results.Count).IsEqualTo(2); // 100 and 300
    }

    [Test]
    public async Task Query_With_Index_Should_Use_Index()
    {
        // This test verifies that queries work correctly when an index is present.
        // Verifying that the index is ACTUALLY used requires checking internal state or mocking,
        // but here we ensure the logic path for IndexScan/Seek is exercised and correct.

        var collection = _engine.GetCollection<ComplexQueryProduct>();

        // Create index on Category manually
        collection.GetIndexManager().CreateIndex("CategoryIdx", new[] { "Category" }, false);

        collection.Insert(new ComplexQueryProduct { Id = 1, Name = "A", Category = "Electronics" });
        collection.Insert(new ComplexQueryProduct { Id = 2, Name = "B", Category = "Food" });
        collection.Insert(new ComplexQueryProduct { Id = 3, Name = "C", Category = "Electronics" });

        // This query should use the index on Category
        var results = _executor.Execute<ComplexQueryProduct>("Products", p => p.Category == "Electronics").ToList();

        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results.All(p => p.Category == "Electronics")).IsTrue();
    }

    [Test]
    public async Task Query_With_Nested_Logic_Should_Work()
    {
        var collection = _engine.GetCollection<ComplexQueryProduct>();
        collection.Insert(new ComplexQueryProduct { Id = 1, Name = "A", Category = "Electronics", Price = 1000 });
        collection.Insert(new ComplexQueryProduct { Id = 2, Name = "B", Category = "Electronics", Price = 500 });
        collection.Insert(new ComplexQueryProduct { Id = 3, Name = "C", Category = "Food", Price = 10 });

        // (Category == "Electronics" && Price < 800) || Category == "Food"
        // Should match Product 2 and 3
        var results = _executor.Execute<ComplexQueryProduct>("Products",
            p => (p.Category == "Electronics" && p.Price < 800) || p.Category == "Food").ToList();

        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results.Any(p => p.Name == "B")).IsTrue();
        await Assert.That(results.Any(p => p.Name == "C")).IsTrue();
    }

    [Test]
    public async Task Query_With_Unsupported_Expression_Should_Throw()
    {
        var collection = _engine.GetCollection<ComplexQueryProduct>();
        collection.Insert(new ComplexQueryProduct { Id = 1, Name = "A" });

        // Using a method that is likely not supported (e.g., GetHashCode())
        // Parser accepts it, but Evaluator throws NotSupportedException because it's not in the allowlist.
        await Assert.That(() => _executor.Execute<ComplexQueryProduct>("Products", p => p.Name.GetHashCode() == 0).ToList())
            .Throws<NotSupportedException>();
    }
}

[Entity("Products")]
public class ComplexQueryProduct
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public string Category { get; set; } = "";
    public decimal Price { get; set; }
    public bool InStock { get; set; }
    public List<string> Tags { get; set; } = new();
}
