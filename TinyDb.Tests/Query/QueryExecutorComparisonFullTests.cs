using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExecutorComparisonFullTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryExecutorComparisonFullTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"q_comp_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Entity("Items")]
    public class Item
    {
        public int Id { get; set; }
        public object? Value { get; set; }
    }

    [Test]
    public async Task Compare_Numeric_Combinations()
    {
        var col = _engine.GetCollection<Item>();
        col.Insert(new Item { Id = 1, Value = 10 }); // int
        col.Insert(new Item { Id = 2, Value = 20.5 }); // double
        col.Insert(new Item { Id = 3, Value = 30m }); // decimal
        
        // We use Find with filter which uses QueryExecutor internally
        await Assert.That(col.Find(x => x.Value != null && (decimal)x.Value > 15m).Count()).IsEqualTo(2);
        await Assert.That(col.Find(x => x.Value != null && (double)x.Value < 25.0).Count()).IsEqualTo(2);
    }

    [Test]
    public async Task Compare_Nulls()
    {
        var col = _engine.GetCollection<Item>();
        col.Insert(new Item { Id = 1, Value = null });
        col.Insert(new Item { Id = 2, Value = "A" });
        
        // Note: x.Value == null in Expression might be parsed as Constant(null)
        await Assert.That(col.Find(x => x.Value == null).Count()).IsEqualTo(1);
        await Assert.That(col.Find(x => x.Value != null).Count()).IsEqualTo(1);
    }
}
