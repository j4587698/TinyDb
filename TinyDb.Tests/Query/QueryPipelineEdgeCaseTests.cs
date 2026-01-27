using System;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

/// <summary>
/// QueryPipeline 边界情况和额外操作测试
/// </summary>
public class QueryPipelineEdgeCaseTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;
    private readonly QueryExecutor _executor;

    public QueryPipelineEdgeCaseTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qpipe_edge_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
        _executor = new QueryExecutor(_engine);
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
        public int Score { get; set; }
        public string Category { get; set; } = "";
    }

    private void SeedData()
    {
        var col = _engine.GetCollection<TestEntity>("test");
        col.Insert(new TestEntity { Id = 1, Name = "Alice", Score = 100, Category = "A" });
        col.Insert(new TestEntity { Id = 2, Name = "Bob", Score = 80, Category = "B" });
        col.Insert(new TestEntity { Id = 3, Name = "Charlie", Score = 90, Category = "A" });
        col.Insert(new TestEntity { Id = 4, Name = "Diana", Score = 70, Category = "B" });
        col.Insert(new TestEntity { Id = 5, Name = "Eve", Score = 95, Category = "A" });
    }

    [Test]
    public async Task Execute_Skip_ShouldSkipItems()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        var query = q.OrderBy(x => x.Id).Skip(2);
        
        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", query.Expression);
        var list = ((System.Collections.Generic.IEnumerable<TestEntity>)result!).ToList();
        
        await Assert.That(list.Count).IsEqualTo(3);
        await Assert.That(list[0].Id).IsEqualTo(3);
    }

    [Test]
    public async Task Execute_Take_ShouldLimitItems()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        var query = q.OrderBy(x => x.Id).Take(2);
        
        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", query.Expression);
        var list = ((System.Collections.Generic.IEnumerable<TestEntity>)result!).ToList();
        
        await Assert.That(list.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Execute_SkipAndTake_ShouldPaginate()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        var query = q.OrderBy(x => x.Id).Skip(1).Take(2);
        
        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", query.Expression);
        var list = ((System.Collections.Generic.IEnumerable<TestEntity>)result!).ToList();
        
        await Assert.That(list.Count).IsEqualTo(2);
        await Assert.That(list[0].Id).IsEqualTo(2);
        await Assert.That(list[1].Id).IsEqualTo(3);
    }

    [Test]
    public async Task Execute_Distinct_ShouldRemoveDuplicates()
    {
        var col = _engine.GetCollection<TestEntity>("distinct_test");
        col.Insert(new TestEntity { Id = 1, Category = "A" });
        col.Insert(new TestEntity { Id = 2, Category = "A" });
        col.Insert(new TestEntity { Id = 3, Category = "B" });
        
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "distinct_test");
        var query = q.Select(x => x.Category).Distinct();
        
        var result = QueryPipeline.Execute<TestEntity>(_executor, "distinct_test", query.Expression);
        var list = ((System.Collections.IEnumerable)result!).Cast<object>().ToList();
        
        await Assert.That(list.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Execute_OrderByDescending_ShouldSortDescending()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        var query = q.OrderByDescending(x => x.Score);
        
        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", query.Expression);
        var list = ((System.Collections.Generic.IEnumerable<TestEntity>)result!).ToList();
        
        await Assert.That(list[0].Score).IsEqualTo(100);
        await Assert.That(list[1].Score).IsEqualTo(95);
    }

    [Test]
    public async Task Execute_ThenBy_ShouldSecondarySort()
    {
        var col = _engine.GetCollection<TestEntity>("thenby_test");
        col.Insert(new TestEntity { Id = 1, Category = "A", Score = 90 });
        col.Insert(new TestEntity { Id = 2, Category = "A", Score = 80 });
        col.Insert(new TestEntity { Id = 3, Category = "B", Score = 100 });
        
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "thenby_test");
        var query = q.OrderBy(x => x.Category).ThenBy(x => x.Score);
        
        var result = QueryPipeline.Execute<TestEntity>(_executor, "thenby_test", query.Expression);
        var list = ((System.Collections.Generic.IEnumerable<TestEntity>)result!).ToList();
        
        // Category A items should be first, then sorted by Score ascending
        await Assert.That(list[0].Category).IsEqualTo("A");
        await Assert.That(list[0].Score).IsEqualTo(80);
        await Assert.That(list[1].Score).IsEqualTo(90);
    }

    [Test]
    public async Task Execute_ThenByDescending_ShouldSecondarySortDesc()
    {
        var col = _engine.GetCollection<TestEntity>("thenbydesc_test");
        col.Insert(new TestEntity { Id = 1, Category = "A", Score = 80 });
        col.Insert(new TestEntity { Id = 2, Category = "A", Score = 90 });
        col.Insert(new TestEntity { Id = 3, Category = "B", Score = 100 });
        
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "thenbydesc_test");
        var query = q.OrderBy(x => x.Category).ThenByDescending(x => x.Score);
        
        var result = QueryPipeline.Execute<TestEntity>(_executor, "thenbydesc_test", query.Expression);
        var list = ((System.Collections.Generic.IEnumerable<TestEntity>)result!).ToList();
        
        // Category A items should be first, then sorted by Score descending
        await Assert.That(list[0].Category).IsEqualTo("A");
        await Assert.That(list[0].Score).IsEqualTo(90);
    }

    [Test]
    public async Task Execute_First_ShouldReturnFirstItem()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "First",
            new Type[] { typeof(TestEntity) },
            q.OrderBy(x => x.Id).Expression);

        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", call);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(((TestEntity)result!).Id).IsEqualTo(1);
    }

    [Test]
    public async Task Execute_FirstOrDefault_WithEmptyResult_ShouldReturnNull()
    {
        var col = _engine.GetCollection<TestEntity>("empty_test");
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "empty_test");
        
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "FirstOrDefault",
            new Type[] { typeof(TestEntity) },
            q.Expression);

        var result = QueryPipeline.Execute<TestEntity>(_executor, "empty_test", call);
        
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Execute_Last_ShouldReturnLastItem()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "Last",
            new Type[] { typeof(TestEntity) },
            q.OrderBy(x => x.Id).Expression);

        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", call);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(((TestEntity)result!).Id).IsEqualTo(5);
    }

    [Test]
    public async Task Execute_LastOrDefault_WithEmptyResult_ShouldReturnNull()
    {
        var col = _engine.GetCollection<TestEntity>("empty_last");
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "empty_last");
        
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "LastOrDefault",
            new Type[] { typeof(TestEntity) },
            q.Expression);

        var result = QueryPipeline.Execute<TestEntity>(_executor, "empty_last", call);
        
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Execute_Any_WithData_ShouldReturnTrue()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "Any",
            new Type[] { typeof(TestEntity) },
            q.Expression);

        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", call);
        
        await Assert.That(result).IsEqualTo(true);
    }

    [Test]
    public async Task Execute_Any_WithEmptyCollection_ShouldReturnFalse()
    {
        var col = _engine.GetCollection<TestEntity>("empty_any");
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "empty_any");
        
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "Any",
            new Type[] { typeof(TestEntity) },
            q.Expression);

        var result = QueryPipeline.Execute<TestEntity>(_executor, "empty_any", call);
        
        await Assert.That(result).IsEqualTo(false);
    }

    [Test]
    public async Task Execute_AnyWithPredicate_ShouldFilterFirst()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        // Any(x => x.Score > 90)
        Expression<Func<TestEntity, bool>> predicate = x => x.Score > 90;
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "Any",
            new Type[] { typeof(TestEntity) },
            q.Expression,
            Expression.Quote(predicate));

        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", call);
        
        await Assert.That(result).IsEqualTo(true);
    }

    [Test]
    public async Task Execute_LongCount_ShouldReturnLong()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "LongCount",
            new Type[] { typeof(TestEntity) },
            q.Expression);

        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", call);
        
        await Assert.That(result).IsEqualTo(5L);
    }

    [Test]
    public async Task Execute_Single_WithOneItem_ShouldReturnItem()
    {
        var col = _engine.GetCollection<TestEntity>("single_test");
        col.Insert(new TestEntity { Id = 42 });
        
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "single_test");
        
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "Single",
            new Type[] { typeof(TestEntity) },
            q.Expression);

        var result = QueryPipeline.Execute<TestEntity>(_executor, "single_test", call);
        
        await Assert.That(((TestEntity)result!).Id).IsEqualTo(42);
    }

    [Test]
    public async Task Execute_SingleOrDefault_WithEmpty_ShouldReturnNull()
    {
        var col = _engine.GetCollection<TestEntity>("single_empty");
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "single_empty");
        
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "SingleOrDefault",
            new Type[] { typeof(TestEntity) },
            q.Expression);

        var result = QueryPipeline.Execute<TestEntity>(_executor, "single_empty", call);
        
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Execute_All_ShouldCheckAllItems()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        // All(x => x.Score > 0)
        Expression<Func<TestEntity, bool>> predicate = x => x.Score > 0;
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "All",
            new Type[] { typeof(TestEntity) },
            q.Expression,
            Expression.Quote(predicate));

        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", call);
        
        await Assert.That(result).IsEqualTo(true);
    }

    [Test]
    public async Task Execute_All_WithFailingPredicate_ShouldReturnFalse()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        // All(x => x.Score > 90) - not all scores are > 90
        Expression<Func<TestEntity, bool>> predicate = x => x.Score > 90;
        var call = Expression.Call(
            typeof(System.Linq.Queryable),
            "All",
            new Type[] { typeof(TestEntity) },
            q.Expression,
            Expression.Quote(predicate));

        var result = QueryPipeline.Execute<TestEntity>(_executor, "test", call);
        
        await Assert.That(result).IsEqualTo(false);
    }

    [Test]
    public async Task Queryable_ToString_ShouldReturnDescription()
    {
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        var str = q.ToString();
        
        await Assert.That(str).Contains("Queryable");
        await Assert.That(str).Contains("TestEntity");
        await Assert.That(str).Contains("test");
    }

    [Test]
    public async Task Queryable_ElementType_ShouldReturnCorrectType()
    {
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        await Assert.That(q.ElementType).IsEqualTo(typeof(TestEntity));
    }

    [Test]
    public async Task Queryable_Expression_ShouldNotBeNull()
    {
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        await Assert.That(q.Expression).IsNotNull();
    }

    [Test]
    public async Task Queryable_Provider_ShouldNotBeNull()
    {
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        await Assert.That(q.Provider).IsNotNull();
    }

    [Test]
    public async Task Queryable_GetEnumerator_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        var count = 0;
        foreach (var item in q)
        {
            count++;
        }
        
        await Assert.That(count).IsEqualTo(5);
    }

    [Test]
    public async Task Queryable_NonGenericGetEnumerator_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestEntity>(_executor, "test");
        
        var count = 0;
        var enumerable = (System.Collections.IEnumerable)q;
        foreach (var item in enumerable)
        {
            count++;
        }
        
        await Assert.That(count).IsEqualTo(5);
    }

    [Test]
    public async Task Queryable_Constructor_NullExecutor_ShouldThrow()
    {
        await Assert.That(() => new TinyDb.Query.Queryable<TestEntity>(null!, "test"))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Queryable_Constructor_NullCollectionName_ShouldThrow()
    {
        await Assert.That(() => new TinyDb.Query.Queryable<TestEntity>(_executor, null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Queryable_Constructor_EmptyCollectionName_ShouldThrow()
    {
        await Assert.That(() => new TinyDb.Query.Queryable<TestEntity>(_executor, ""))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Queryable_Constructor_WhitespaceCollectionName_ShouldThrow()
    {
        await Assert.That(() => new TinyDb.Query.Queryable<TestEntity>(_executor, "   "))
            .Throws<ArgumentNullException>();
    }
}
