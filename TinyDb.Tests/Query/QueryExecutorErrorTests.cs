using TinyDb.Core;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Attributes;

namespace TinyDb.Tests.Query;

public class QueryExecutorErrorTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryExecutorErrorTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"query_err_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Entity("Data")]
    public class Item
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task Execute_With_Null_CollectionName_Should_Throw()
    {
        await Assert.That(() => _executor.Execute<Item>(null!)).Throws<ArgumentException>();
    }

    [Test]
    public async Task Execute_With_Empty_CollectionName_Should_Throw()
    {
        await Assert.That(() => _executor.Execute<Item>("")).Throws<ArgumentException>();
        await Assert.That(() => _executor.Execute<Item>("   ")).Throws<ArgumentException>();
    }
}
