using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryArithmeticTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryArithmeticTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"query_arith_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Entity("Math")]
    public class MathItem
    {
        public int Id { get; set; }
        public int A { get; set; }
    }

    [Test]
    public async Task Arithmetic_Operations_Should_Throw_NotSupported()
    {
        // QueryExecutor does not support arithmetic evaluation yet
        var col = _engine.GetCollection<MathItem>();
        col.Insert(new MathItem { Id = 1, A = 10 });

        // Add
        await Assert.That(() => _executor.Execute<MathItem>("Math", x => x.A + 5 == 15).ToList())
            .Throws<NotSupportedException>();

        // Subtract
        await Assert.That(() => _executor.Execute<MathItem>("Math", x => x.A - 5 == 5).ToList())
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task Logical_NotEqual_Should_Work()
    {
        var col = _engine.GetCollection<MathItem>();
        col.Insert(new MathItem { Id = 1, A = 10 });
        col.Insert(new MathItem { Id = 2, A = 20 });

        var res = _executor.Execute<MathItem>("Math", x => x.A != 10).ToList();
        await Assert.That(res.Count).IsEqualTo(1);
        await Assert.That(res[0].Id).IsEqualTo(2);
    }
}
