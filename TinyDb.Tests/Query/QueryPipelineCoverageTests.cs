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
        var col = _engine.GetCollection<TestEntity>("test");
        col.Insert(new TestEntity { Id = 1 });
        
        var executor = new QueryExecutor(_engine);
        var expr = Expression.Constant(true);
        var result = QueryPipeline.Execute<TestEntity>(executor, "test", expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsAssignableTo<System.Collections.Generic.IEnumerable<TestEntity>>();
    }

    [Test]
    public async Task Execute_WithWhere_ShouldFilter()
    {
        var col = _engine.GetCollection<TestEntity>("test");
        col.Insert(new TestEntity { Id = 1 });
        col.Insert(new TestEntity { Id = 2 });
        
        var executor = new QueryExecutor(_engine);
        // Create a real Queryable to get a valid Expression source
        var q = new TinyDb.Query.Queryable<TestEntity>(executor, "test");
        // x => x.Id == 1
        var query = q.Where(x => x.Id == 1);
        
        // Execute the pipeline with this expression
        var result = QueryPipeline.Execute<TestEntity>(executor, "test", query.Expression);
        
        await Assert.That(result).IsNotNull();
        var list = ((System.Collections.Generic.IEnumerable<TestEntity>)result!).ToList();
        await Assert.That(list).HasCount().EqualTo(1);
        await Assert.That(list[0].Id).IsEqualTo(1);
    }

    [Test]
    public async Task Execute_WithSelect_ShouldProject()
    {
        var col = _engine.GetCollection<TestEntity>("test");
        col.Insert(new TestEntity { Id = 10 });
        
        var executor = new QueryExecutor(_engine);
        var q = new TinyDb.Query.Queryable<TestEntity>(executor, "test");
        // Select Id
        var query = q.Select(x => x.Id);
        
        var result = QueryPipeline.Execute<TestEntity>(executor, "test", query.Expression);
        
        // Result is IEnumerable<int> (or list of objects if AOT untyped? No, CreatesTypedEnumerable)
        // Check dynamic type behavior
        await Assert.That(result).IsNotNull();
        var list = ((System.Collections.IEnumerable)result!).Cast<int>().ToList();
        await Assert.That(list).HasCount().EqualTo(1);
        await Assert.That(list[0]).IsEqualTo(10);
    }

    [Test]
    public async Task Execute_WithTerminalCount_ShouldReturnInt()
    {
        var col = _engine.GetCollection<TestEntity>("test");
        col.Insert(new TestEntity { Id = 1 });
        col.Insert(new TestEntity { Id = 2 });
        
        var executor = new QueryExecutor(_engine);
        var q = new TinyDb.Query.Queryable<TestEntity>(executor, "test");
        
        // Count() is a terminal operation. The Expression will be a MethodCall(Count, source)
        // Since we can't easily call .Count() on our Queryable without triggering Provider.Execute immediately,
        // we construct the expression or use the creating query.
        // Actually, q.Provider.CreateQuery(expression) returns IQueryable.
        // But Count returns int.
        // We can manually build the expression.
        
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "Count",
            new Type[] { typeof(TestEntity) },
            q.Expression);

        var result = QueryPipeline.Execute<TestEntity>(executor, "test", call);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsEqualTo(2);
    }
}
