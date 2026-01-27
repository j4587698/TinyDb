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

    [Test]
    public async Task Arithmetic_Operations_Should_Work()
    {
        // QueryExecutor now supports arithmetic evaluation
        var col = _engine.GetCollection<QueryArithmeticMathItem>();
        col.Insert(new QueryArithmeticMathItem { Id = 1, A = 10 });

        // Add: 10 + 5 == 15
        var resAdd = _executor.Execute<QueryArithmeticMathItem>("Math", x => x.A + 5 == 15).ToList();
        await Assert.That(resAdd.Count).IsEqualTo(1);
        await Assert.That(resAdd[0].Id).IsEqualTo(1);

        // Subtract: 10 - 5 == 5
        var resSub = _executor.Execute<QueryArithmeticMathItem>("Math", x => x.A - 5 == 5).ToList();
        await Assert.That(resSub.Count).IsEqualTo(1);
        await Assert.That(resSub[0].Id).IsEqualTo(1);
    }

    [Test]
    public async Task Logical_NotEqual_Should_Work()
    {
        var col = _engine.GetCollection<QueryArithmeticMathItem>();
        col.Insert(new QueryArithmeticMathItem { Id = 1, A = 10 });
        col.Insert(new QueryArithmeticMathItem { Id = 2, A = 20 });

        var res = _executor.Execute<QueryArithmeticMathItem>("Math", x => x.A != 10).ToList();
        await Assert.That(res.Count).IsEqualTo(1);
        await Assert.That(res[0].Id).IsEqualTo(2);
    }
}

[Entity("Math")]
public class QueryArithmeticMathItem
{
    public int Id { get; set; }
    public int A { get; set; }
}
