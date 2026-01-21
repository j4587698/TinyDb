using System;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExecutorFallbackTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;

    public QueryExecutorFallbackTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qexec_fallback_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Test]
    public async Task Execute_UnsupportedExpression_ShouldFallbackToMemory()
    {
        var col = _engine.GetCollectionWithName<TestEntity>("test");
        col.Insert(new TestEntity { Id = 1, Name = "A" });
        col.Insert(new TestEntity { Id = 2, Name = "B" });
        
        var executor = new QueryExecutor(_engine);
        
        // Unsupported expression: ternary operator
        Expression<Func<TestEntity, bool>> expr = x => (x.Id > 1 ? true : false);
        
        // This calls QueryExecutor.Execute -> FullTableScan -> Parser.Parse (throws) -> Catch -> Memory Filter
        var result = executor.Execute("test", expr);
        
        var list = result.ToList();
        await Assert.That(list.Count).IsEqualTo(1);
        await Assert.That(list[0].Id).IsEqualTo(2);
    }
}
