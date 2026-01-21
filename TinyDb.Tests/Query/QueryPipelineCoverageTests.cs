using System;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryPipelineCoverageTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;

    public QueryPipelineCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qpipe_test_{Guid.NewGuid()}.db");
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
    }

    [Test]
    public async Task Execute_NoSource_ShouldReturnQueryResult()
    {
        var col = _engine.GetCollectionWithName<TestEntity>("test");
        col.Insert(new TestEntity { Id = 1 });
        
        var executor = new QueryExecutor(_engine);
        
        // Expression that is NOT based on Queryable source
        // e.g. simply return true?
        // But QueryPipeline expects to execute something?
        // If we pass Constant(true), PredicateExtractor returns null predicate.
        // QueryExecutor.Execute(null) returns all docs.
        // Pipeline returns that IEnumerable.
        
        var expr = Expression.Constant(true);
        var result = QueryPipeline.Execute(executor, "test", typeof(TestEntity), expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsAssignableTo<System.Collections.Generic.IEnumerable<TestEntity>>();
        var list = (System.Collections.Generic.IEnumerable<TestEntity>)result!;
        // It should contain the inserted document
        int count = 0;
        foreach(var item in list) count++;
        await Assert.That(count).IsEqualTo(1);
    }
}
