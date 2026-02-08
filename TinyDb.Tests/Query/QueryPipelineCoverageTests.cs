using System;
using System.Collections;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Core;
using TinyDb.Attributes;
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

    [Entity]
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
        await Assert.That(list).Count().IsEqualTo(1);
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
        await Assert.That(list).Count().IsEqualTo(1);
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
        
        var result = q.Count();

        await Assert.That(result).IsEqualTo(2);
    }

    [Test]
    public async Task ExtractAot_ShouldDetectMultiplePredicates()
    {
        var executor = new QueryExecutor(_engine);
        var q = new TinyDb.Query.Queryable<TestEntity>(executor, "test");
        var query = q.Where(x => x.Id > 0).Where(x => x.Id < 5);

        var extractorType = Type.GetType("TinyDb.Query.PredicateExtractor, TinyDb");
        await Assert.That(extractorType).IsNotNull();

        var method = extractorType!.GetMethod("ExtractAot", System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public);
        await Assert.That(method).IsNotNull();

        var resultObj = method!.Invoke(null, new object[] { query.Expression, typeof(TestEntity) });
        var result = (ValueTuple<Expression?, System.Linq.Expressions.ConstantExpression?, bool>)resultObj!;

        await Assert.That(result.Item1).IsNotNull();
        await Assert.That(result.Item2).IsNotNull();
        await Assert.That(result.Item3).IsTrue();
    }

    [Test]
    public async Task ExecuteAot_WithMultipleWhere_ShouldFilterInMemory()
    {
        var executor = new QueryExecutor(_engine);
        var q = new TinyDb.Query.Queryable<TestEntity>(executor, "test");
        var query = q.Where(x => x.Id > 0).Where(x => x.Id < 5);

        var data = new[]
        {
            new TestEntity { Id = 1 },
            new TestEntity { Id = 10 }
        };

        var result = QueryPipeline.ExecuteAotForTests<TestEntity>(query.Expression, data, null);

        await Assert.That(result).IsNotNull();
        var list = ((System.Collections.Generic.IEnumerable<TestEntity>)result!).ToList();
        await Assert.That(list.Count).IsEqualTo(1);
        await Assert.That(list[0].Id).IsEqualTo(1);
    }

    [Test]
    public async Task ExecuteAot_GroupBy_Sum_ByKey()
    {
        var provider = new RecordingQueryProvider();
        var q = new RecordingQueryable<TestEntity>(provider);

        _ = q.GroupBy(x => x.Id).Sum(g => g.Key);
        await Assert.That(provider.LastExpression).IsNotNull();

        var data = new[]
        {
            new TestEntity { Id = 1 },
            new TestEntity { Id = 1 },
            new TestEntity { Id = 2 }
        };

        var result = QueryPipeline.ExecuteAotForTests<TestEntity>(provider.LastExpression!, data, null);

        await Assert.That(result).IsEqualTo(3m);
    }

    [Test]
    public async Task ExecuteAot_Sum_WithoutSelector_ShouldAggregate()
    {
        var provider = new RecordingQueryProvider();
        var q = new RecordingQueryable<TestEntity>(provider);

        _ = q.Select(x => x.Id).Sum();
        await Assert.That(provider.LastExpression).IsNotNull();

        var data = new[]
        {
            new TestEntity { Id = 1 },
            new TestEntity { Id = 2 },
            new TestEntity { Id = 3 }
        };

        var result = QueryPipeline.ExecuteAotForTests<TestEntity>(provider.LastExpression!, data, null);

        await Assert.That(result).IsEqualTo(6m);
    }

    private sealed class RecordingQueryProvider : IQueryProvider
    {
        public Expression? LastExpression { get; private set; }

        public IQueryable CreateQuery(Expression expression)
        {
            return new RecordingQueryable<object>(this, expression);
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new RecordingQueryable<TElement>(this, expression);
        }

        public object? Execute(Expression expression)
        {
            LastExpression = expression;
            return null;
        }

        public TResult Execute<TResult>(Expression expression)
        {
            LastExpression = expression;
            return default!;
        }
    }

    private sealed class RecordingQueryable<T> : IQueryable<T>
    {
        public RecordingQueryable(RecordingQueryProvider provider)
            : this(provider, Expression.Constant(null, typeof(IQueryable<T>)))
        {
        }

        public RecordingQueryable(RecordingQueryProvider provider, Expression expression)
        {
            Provider = provider;
            Expression = expression;
        }

        public Type ElementType => typeof(T);
        public Expression Expression { get; }
        public IQueryProvider Provider { get; }
        public IEnumerator<T> GetEnumerator() => throw new NotSupportedException();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
