using System;
using System.Collections;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

[NotInParallel]
public sealed class QueryableInternalBranchesCoverageTests
{
    private string _testDirectory = null!;
    private string _testDbPath = null!;
    private TinyDbEngine _engine = null!;
    private QueryExecutor _executor = null!;

    [Before(Test)]
    public void Setup()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "TinyDbQueryableInternalBranchesCoverageTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_testDirectory);
        _testDbPath = Path.Combine(_testDirectory, "test.db");

        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (Directory.Exists(_testDirectory))
        {
            try { Directory.Delete(_testDirectory, true); } catch { }
        }
    }

    private sealed class StubProvider : IQueryProvider
    {
        private readonly object? _result;

        public StubProvider(object? result) => _result = result;

        public IQueryable CreateQuery(Expression expression) => throw new NotSupportedException();
        public IQueryable<TElement> CreateQuery<TElement>(Expression expression) => throw new NotSupportedException();
        public object? Execute(Expression expression) => _result;
        public TResult Execute<TResult>(Expression expression) => (TResult)_result!;
    }

    [Test]
    public async Task UntypedQueryable_Ctor_WithNullArguments_ShouldThrow()
    {
        var provider = new StubProvider(Array.Empty<object>());
        var expr = Expression.Constant(0);

        await Assert.That(() => new UntypedQueryable<TestProduct>(null!, expr, typeof(object))).Throws<ArgumentNullException>();
        await Assert.That(() => new UntypedQueryable<TestProduct>(provider, null!, typeof(object))).Throws<ArgumentNullException>();
        await Assert.That(() => new UntypedQueryable<TestProduct>(provider, expr, null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task UntypedQueryable_GetEnumerator_ShouldHandleNullAndEnumerableResults()
    {
        var expr = Expression.Constant(0);

        var withNull = (IEnumerable)new UntypedQueryable<TestProduct>(new StubProvider(null), expr, typeof(object));
        var count1 = 0;
        foreach (var _ in withNull) count1++;
        await Assert.That(count1).IsEqualTo(0);

        var withEnumerable = (IEnumerable)new UntypedQueryable<TestProduct>(new StubProvider(new object[] { 1, 2 }), expr, typeof(object));
        var count2 = 0;
        foreach (var _ in withEnumerable) count2++;
        await Assert.That(count2).IsEqualTo(2);
    }

    [Test]
    public async Task QueryProvider_Ctor_WithInvalidArgs_ShouldThrow()
    {
        await Assert.That(() => new QueryProvider<TestProduct, TestProduct>(null!, "c")).Throws<ArgumentNullException>();
        await Assert.That(() => new QueryProvider<TestProduct, TestProduct>(_executor, " ")).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task QueryProvider_CreateQuery_ShouldHandleGenericArrayAndDefaultExpressionTypes()
    {
        var queryable = new Queryable<TestProduct>(_executor, "products");
        var provider = queryable.Provider;

        var q1 = provider.CreateQuery(Expression.Constant(new List<string>()));
        await Assert.That(q1.ElementType).IsEqualTo(typeof(string));

        var q2 = provider.CreateQuery(Expression.Constant(Array.Empty<int>()));
        await Assert.That(q2.ElementType).IsEqualTo(typeof(int));

        var q3 = provider.CreateQuery(Expression.Constant(123));
        await Assert.That(q3.ElementType).IsEqualTo(typeof(object));
    }

    [Test]
    public async Task QueryProvider_ExecuteEnumerable_WhenResultNotEnumerable_ShouldReturnEmptySequence()
    {
        var collectionName = $"p_{Guid.NewGuid():N}";
        var collection = _engine.GetCollection<TestProduct>(collectionName);
        collection.Insert(new TestProduct { Name = "A", Price = 1m, CreatedAt = DateTime.UtcNow });

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        var p = Expression.Parameter(typeof(TestProduct), "p");
        var selector = Expression.Lambda<Func<TestProduct, decimal>>(Expression.Property(p, nameof(TestProduct.Price)), p);
        var sumExpr = Expression.Call(typeof(System.Linq.Queryable), "Sum", new[] { typeof(TestProduct) }, queryable.Expression, selector);

        var result = queryable.Provider.Execute<IEnumerable<TestProduct>>(sumExpr);
        var list = result.ToList();
        await Assert.That(list.Count).IsEqualTo(0);
    }

    [Test]
    public async Task QueryPipeline_DistinctGeneric_And_SkipFallback_ShouldCoverBranches()
    {
        var collectionName = $"p_{Guid.NewGuid():N}";
        var collection = _engine.GetCollection<TestProduct>(collectionName);
        collection.Insert(new TestProduct { Name = "A", Price = 1m, CreatedAt = DateTime.UtcNow });
        collection.Insert(new TestProduct { Name = "B", Price = 2m, CreatedAt = DateTime.UtcNow });
        collection.Insert(new TestProduct { Name = "C", Price = 3m, CreatedAt = DateTime.UtcNow });

        var queryable = new Queryable<TestProduct>(_executor, collectionName);

        var distinct = queryable.Distinct().ToList();
        await Assert.That(distinct.Count).IsEqualTo(3);

        var skip1 = queryable.Select(p => p.Name).Skip(1).ToList();
        await Assert.That(skip1.Count).IsEqualTo(2);

        var skipTooMuch = queryable.Select(p => p.Name).Skip(10).ToList();
        await Assert.That(skipTooMuch.Count).IsEqualTo(0);
    }
}
