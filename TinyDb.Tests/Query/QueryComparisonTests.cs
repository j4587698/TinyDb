using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryComparisonTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryComparisonTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"query_comp_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Compare_Int_And_Double_Should_Work()
    {
        var collection = _engine.GetCollection<QueryComparisonMixedTypeEntity>();
        collection.Insert(new QueryComparisonMixedTypeEntity { Id = 1, Value = 10 });      // int
        collection.Insert(new QueryComparisonMixedTypeEntity { Id = 2, Value = 10.5 });    // double

        // Query: Value > 10.0 (double)
        // Compare(10.5, 10.0) -> 1. True.

        var results = _executor.Execute<QueryComparisonMixedTypeEntity>("MixedTypes", x => (double)x.Value! > 10.0).ToList();
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(2);
    }

    [Test]
    public async Task Compare_String_And_Number_Should_Fallback_To_String()
    {
        var collection = _engine.GetCollection<QueryComparisonMixedTypeEntity>();
        collection.Insert(new QueryComparisonMixedTypeEntity { Id = 1, Value = "10" });
        collection.Insert(new QueryComparisonMixedTypeEntity { Id = 2, Value = "2" });

        var results = _executor.Execute<QueryComparisonMixedTypeEntity>("MixedTypes", x => (string)x.Value! == "10").ToList();
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Id).IsEqualTo(1);
    }

    [Test]
    public async Task Compare_Decimal_Should_Work()
    {
        var collection = _engine.GetCollection<QueryComparisonMixedTypeEntity>();
        collection.Insert(new QueryComparisonMixedTypeEntity { Id = 1, Value = 100m });

        var results = _executor.Execute<QueryComparisonMixedTypeEntity>("MixedTypes", x => (decimal)x.Value! == 100m).ToList();
        await Assert.That(results.Count).IsEqualTo(1);
    }
}

[Entity("MixedTypes")]
public class QueryComparisonMixedTypeEntity
{
    public int Id { get; set; }
    public object? Value { get; set; }
}
