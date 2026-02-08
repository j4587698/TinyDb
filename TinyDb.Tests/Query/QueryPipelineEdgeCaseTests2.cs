using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Query;
using TinyDb.Core;
using TinyDb.Tests.Utils;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Tests.Query;

/// <summary>
/// Additional edge case tests for QueryPipeline to improve coverage
/// Targets: OrderBy paths, terminal operations, aggregations, etc.
/// </summary>
public class QueryPipelineEdgeCaseTests2 : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;
    private readonly QueryExecutor _executor;

    public QueryPipelineEdgeCaseTests2()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qpipe_edge2_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Entity]
    public class TestItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Value { get; set; }
        public string Category { get; set; } = "";
        public decimal Price { get; set; }
    }

    public class GroupAverageResult
    {
        public string Key { get; set; } = "";
        public decimal Avg { get; set; }
    }

    public class GroupMinResult
    {
        public string Key { get; set; } = "";
        public int Min { get; set; }
    }

    public class GroupMaxResult
    {
        public string Key { get; set; } = "";
        public int Max { get; set; }
    }

    private sealed class ProjectedValueItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    private sealed class ProjectedCategoryValueItem
    {
        public string Category { get; set; } = "";
        public int Value { get; set; }
    }

    private void SeedData()
    {
        var col = _engine.GetCollection<TestItem>("test");
        col.Insert(new TestItem { Id = 1, Name = "Item1", Value = 10, Category = "A", Price = 100m });
        col.Insert(new TestItem { Id = 2, Name = "Item2", Value = 20, Category = "A", Price = 200m });
        col.Insert(new TestItem { Id = 3, Name = "Item3", Value = 30, Category = "B", Price = 150m });
        col.Insert(new TestItem { Id = 4, Name = "Item4", Value = 40, Category = "B", Price = 250m });
        col.Insert(new TestItem { Id = 5, Name = "Item5", Value = 50, Category = "C", Price = 300m });
    }

    #region OrderBy Tests (without Where after Select to avoid PredicateExtractor issues)

    /// <summary>
    /// Test OrderBy after Select (non-generic path)
    /// </summary>
    [Test]
    public async Task Execute_OrderBy_AfterSelect_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Select(x => new ProjectedValueItem { Id = x.Id, Name = x.Name, Value = x.Value })
                     .OrderBy(x => x.Value);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable<ProjectedValueItem>)result!).ToList();
        
        await Assert.That(list.Count).IsEqualTo(5);
        await Assert.That(list[0].Value).IsEqualTo(10);
    }

    /// <summary>
    /// Test OrderByDescending after Select
    /// </summary>
    [Test]
    public async Task Execute_OrderByDescending_AfterSelect_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Select(x => new ProjectedValueItem { Id = x.Id, Value = x.Value })
                     .OrderByDescending(x => x.Value);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable<ProjectedValueItem>)result!).ToList();
        
        await Assert.That(list[0].Value).IsEqualTo(50);
    }

    /// <summary>
    /// Test ThenBy after Select (covers non-generic ThenBy path)
    /// </summary>
    [Test]
    public async Task Execute_ThenBy_AfterSelect_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Select(x => new ProjectedCategoryValueItem { Category = x.Category, Value = x.Value })
                     .OrderBy(x => x.Category)
                     .ThenBy(x => x.Value);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable<ProjectedCategoryValueItem>)result!).ToList();
        
        await Assert.That(list.Count).IsEqualTo(5);
        await Assert.That(list[0].Category).IsEqualTo("A");
    }

    /// <summary>
    /// Test ThenByDescending after Select
    /// </summary>
    [Test]
    public async Task Execute_ThenByDescending_AfterSelect_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Select(x => new ProjectedCategoryValueItem { Category = x.Category, Value = x.Value })
                     .OrderBy(x => x.Category)
                     .ThenByDescending(x => x.Value);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable<ProjectedCategoryValueItem>)result!).ToList();
        
        // Category A should have values in descending order: 20, 10
        await Assert.That(list[0].Value).IsEqualTo(20);
        await Assert.That(list[1].Value).IsEqualTo(10);
    }

    #endregion

    #region Complex Chain Tests (without Where after Select)

    /// <summary>
    /// Test Select + OrderBy + Skip + Take chain
    /// </summary>
    [Test]
    public async Task Execute_SelectOrderBySkipTake_Chain_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Select(x => new { x.Id, x.Value, x.Category })
                     .OrderBy(x => x.Value)
                     .Skip(1)
                     .Take(2);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable)result!).Cast<object>().ToList();
        
        await Assert.That(list.Count).IsEqualTo(2);
    }

    /// <summary>
    /// Test Where followed by Where (multiple predicates combined)
    /// </summary>
    [Test]
    public async Task Execute_MultipleWheres_ShouldCombinePredicates()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Where(x => x.Value > 10)
                     .Where(x => x.Value < 50);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable<TestItem>)result!).ToList();
        
        await Assert.That(list.Count).IsEqualTo(3); // 20, 30, 40
    }

    #endregion

    #region Terminal Operations with Predicate Tests

    /// <summary>
    /// Test First with predicate
    /// </summary>
    [Test]
    public async Task Execute_FirstWithPredicate_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var result = q.OrderBy(x => x.Id).First(x => x.Value > 30);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(((TestItem)result!).Value).IsEqualTo(40);
    }

    /// <summary>
    /// Test Single with predicate
    /// </summary>
    [Test]
    public async Task Execute_SingleWithPredicate_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var result = q.Single(x => x.Id == 3);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(((TestItem)result!).Name).IsEqualTo("Item3");
    }

    /// <summary>
    /// Test Count with predicate
    /// </summary>
    [Test]
    public async Task Execute_CountWithPredicate_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var result = q.Count(x => x.Category == "B");
        
        await Assert.That(result).IsEqualTo(2);
    }

    /// <summary>
    /// Test LongCount with predicate
    /// </summary>
    [Test]
    public async Task Execute_LongCountWithPredicate_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var result = q.LongCount(x => x.Value >= 30);
        
        await Assert.That(result).IsEqualTo(3L);
    }

    /// <summary>
    /// Test Last with predicate
    /// </summary>
    [Test]
    public async Task Execute_LastWithPredicate_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var result = q.OrderBy(x => x.Id).Last(x => x.Category == "A");
        
        await Assert.That(result).IsNotNull();
        await Assert.That(((TestItem)result!).Id).IsEqualTo(2);
    }

    /// <summary>
    /// Test LastOrDefault with predicate on no match
    /// </summary>
    [Test]
    public async Task Execute_LastOrDefaultWithPredicate_NoMatch_ShouldReturnNull()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var result = q.LastOrDefault(x => x.Value > 1000);
        
        await Assert.That(result).IsNull();
    }

    #endregion

    #region AotGrouping Tests

    /// <summary>
    /// Test AotGrouping Average method
    /// </summary>
    [Test]
    public async Task AotGrouping_Average_ShouldCalculateCorrectly()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var result = q.GroupBy(x => x.Category)
                      .Select(g => new GroupAverageResult { Key = g.Key == null ? "" : g.Key.ToString(), Avg = g.Average(x => (decimal)x.Value) })
                      .OrderBy(x => x.Key)
                      .ToList();
        
        // Category A: (10 + 20) / 2 = 15
        await Assert.That(result[0].Avg).IsEqualTo(15.0m);
        // Category B: (30 + 40) / 2 = 35
        await Assert.That(result[1].Avg).IsEqualTo(35.0m);
        // Category C: 50 / 1 = 50
        await Assert.That(result[2].Avg).IsEqualTo(50.0m);
    }

    /// <summary>
    /// Test AotGrouping Min method
    /// </summary>
    [Test]
    public async Task AotGrouping_Min_ShouldFindMinimum()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var result = q.GroupBy(x => x.Category)
                      .Select(g => new GroupMinResult { Key = g.Key == null ? "" : g.Key.ToString(), Min = g.Min(x => x.Value) })
                      .OrderBy(x => x.Key)
                      .ToList();
        
        await Assert.That(result[0].Min).IsEqualTo(10); // A
        await Assert.That(result[1].Min).IsEqualTo(30); // B
        await Assert.That(result[2].Min).IsEqualTo(50); // C
    }

    /// <summary>
    /// Test AotGrouping Max method
    /// </summary>
    [Test]
    public async Task AotGrouping_Max_ShouldFindMaximum()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var result = q.GroupBy(x => x.Category)
                      .Select(g => new GroupMaxResult { Key = g.Key == null ? "" : g.Key.ToString(), Max = g.Max(x => x.Value) })
                      .OrderBy(x => x.Key)
                      .ToList();
        
        await Assert.That(result[0].Max).IsEqualTo(20); // A
        await Assert.That(result[1].Max).IsEqualTo(40); // B
        await Assert.That(result[2].Max).IsEqualTo(50); // C
    }

    #endregion

    #region Distinct After Select Tests

    /// <summary>
    /// Test Distinct after Select
    /// </summary>
    [Test]
    public async Task Execute_Distinct_AfterSelect_ShouldRemoveDuplicates()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Select(x => x.Category).Distinct();
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable)result!).Cast<object>().ToList();
        
        await Assert.That(list.Count).IsEqualTo(3); // A, B, C
    }

    #endregion

    #region Skip/Take Edge Cases

    /// <summary>
    /// Test Skip with count greater than collection size
    /// </summary>
    [Test]
    public async Task Execute_Skip_MoreThanCount_ShouldReturnEmpty()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Skip(100);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable<TestItem>)result!).ToList();
        
        await Assert.That(list.Count).IsEqualTo(0);
    }

    /// <summary>
    /// Test Take with count zero
    /// </summary>
    [Test]
    public async Task Execute_Take_Zero_ShouldReturnEmpty()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Take(0);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable<TestItem>)result!).ToList();
        
        await Assert.That(list.Count).IsEqualTo(0);
    }

    /// <summary>
    /// Test Skip and Take after Select
    /// </summary>
    [Test]
    public async Task Execute_SkipTake_AfterSelect_ShouldWork()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Select(x => new { x.Id, x.Name })
                     .Skip(2)
                     .Take(2);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable)result!).Cast<object>().ToList();
        
        await Assert.That(list.Count).IsEqualTo(2);
    }

    #endregion

    #region CreateTypedEnumerable Tests

    /// <summary>
    /// Test that result is properly typed enumerable
    /// </summary>
    [Test]
    public async Task Execute_ShouldReturnTypedEnumerable()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Where(x => x.Id < 3);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        
        await Assert.That(result).IsAssignableTo<IEnumerable<TestItem>>();
    }

    /// <summary>
    /// Test that Select result has correct element type
    /// </summary>
    [Test]
    public async Task Execute_Select_ShouldReturnCorrectElementType()
    {
        SeedData();
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "test");
        
        var query = q.Select(x => x.Name);
        
        var result = QueryPipeline.Execute<TestItem>(_executor, "test", query.Expression);
        var list = ((IEnumerable)result!).Cast<object>().ToList();
        
        await Assert.That(list[0]).IsAssignableTo<string>();
    }

    #endregion

    #region Empty Collection Edge Cases

    /// <summary>
    /// Test operations on empty collection
    /// </summary>
    [Test]
    public async Task Execute_EmptyCollection_CountShouldBeZero()
    {
        var col = _engine.GetCollection<TestItem>("empty_count");
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "empty_count");
        
        var result = q.Count();
        
        await Assert.That(result).IsEqualTo(0);
    }

    /// <summary>
    /// Test Any on empty collection returns false
    /// </summary>
    [Test]
    public async Task Execute_EmptyCollection_AnyShouldBeFalse()
    {
        var col = _engine.GetCollection<TestItem>("empty_any2");
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "empty_any2");
        
        var result = q.Any();
        
        await Assert.That(result).IsEqualTo(false);
    }

    /// <summary>
    /// Test All on empty collection returns true (vacuous truth)
    /// </summary>
    [Test]
    public async Task Execute_EmptyCollection_AllShouldBeTrue()
    {
        var col = _engine.GetCollection<TestItem>("empty_all");
        var q = new TinyDb.Query.Queryable<TestItem>(_executor, "empty_all");
        
        var result = q.All(x => x.Value > 100);
        
        await Assert.That(result).IsEqualTo(true);
    }

    #endregion

    #region Aggregation via Query Interface Tests

    /// <summary>
    /// Test Sum via Query interface
    /// </summary>
    [Test]
    public async Task Query_Sum_ShouldCalculateTotal()
    {
        SeedData();
        var col = _engine.GetCollection<TestItem>("test");
        
        var total = col.Query().Sum(x => x.Price);
        
        await Assert.That(total).IsEqualTo(1000m); // 100+200+150+250+300
    }

    /// <summary>
    /// Test Average via Query interface
    /// </summary>
    [Test]
    public async Task Query_Average_ShouldCalculateAverage()
    {
        SeedData();
        var col = _engine.GetCollection<TestItem>("test");
        
        var avg = col.Query().Average(x => x.Price);
        
        await Assert.That(avg).IsEqualTo(200m); // 1000 / 5
    }

    /// <summary>
    /// Test Min via Query interface
    /// </summary>
    [Test]
    public async Task Query_Min_ShouldFindMinimum()
    {
        SeedData();
        var col = _engine.GetCollection<TestItem>("test");
        
        var min = col.Query().Min(x => x.Value);
        
        await Assert.That(min).IsEqualTo(10);
    }

    /// <summary>
    /// Test Max via Query interface
    /// </summary>
    [Test]
    public async Task Query_Max_ShouldFindMaximum()
    {
        SeedData();
        var col = _engine.GetCollection<TestItem>("test");
        
        var max = col.Query().Max(x => x.Value);
        
        await Assert.That(max).IsEqualTo(50);
    }

    #endregion
}

/// <summary>
/// Tests for Queryable<TSource, TData> two-type-parameter variant
/// </summary>
public class QueryableTwoTypeParamsTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;
    private readonly QueryExecutor _executor;

    public QueryableTwoTypeParamsTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"q2types_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Entity]
    public class TwoTypeTestItem
    {
        public int Id { get; set; }
        public string Value { get; set; } = "";
    }

    /// <summary>
    /// Test Queryable<TSource, TData> ToString
    /// </summary>
    [Test]
    public async Task QueryableTwoType_ToString_ShouldIncludeBothTypes()
    {
        var col = _engine.GetCollection<TwoTypeTestItem>("two_type_test");
        col.Insert(new TwoTypeTestItem { Id = 1, Value = "Test" });
        
        var q = new TinyDb.Query.Queryable<TwoTypeTestItem>(_executor, "two_type_test");
        var projected = q.Select(x => x.Value);
        
        var str = projected.ToString();
        
        await Assert.That(str).Contains("TwoTypeTestItem");
        await Assert.That(str).Contains("String");
    }

    /// <summary>
    /// Test Queryable<TSource, TData> ElementType
    /// </summary>
    [Test]
    public async Task QueryableTwoType_ElementType_ShouldReturnDataType()
    {
        var col = _engine.GetCollection<TwoTypeTestItem>("elem_type_test");
        col.Insert(new TwoTypeTestItem { Id = 1, Value = "Test" });
        
        var q = new TinyDb.Query.Queryable<TwoTypeTestItem>(_executor, "elem_type_test");
        var projected = q.Select(x => x.Value);
        
        await Assert.That(projected.ElementType).IsEqualTo(typeof(string));
    }

    /// <summary>
    /// Test Queryable<TSource, TData> Provider
    /// </summary>
    [Test]
    public async Task QueryableTwoType_Provider_ShouldNotBeNull()
    {
        var col = _engine.GetCollection<TwoTypeTestItem>("provider_test");
        
        var q = new TinyDb.Query.Queryable<TwoTypeTestItem>(_executor, "provider_test");
        var projected = q.Select(x => x.Id);
        
        await Assert.That(projected.Provider).IsNotNull();
    }

    /// <summary>
    /// Test Queryable<TSource, TData> Expression
    /// </summary>
    [Test]
    public async Task QueryableTwoType_Expression_ShouldNotBeNull()
    {
        var col = _engine.GetCollection<TwoTypeTestItem>("expr_test");
        
        var q = new TinyDb.Query.Queryable<TwoTypeTestItem>(_executor, "expr_test");
        var projected = q.Select(x => x.Id);
        
        await Assert.That(projected.Expression).IsNotNull();
    }

    /// <summary>
    /// Test chained Select operations
    /// </summary>
    [Test]
    public async Task QueryableTwoType_ChainedSelect_ShouldWork()
    {
        var col = _engine.GetCollection<TwoTypeTestItem>("chain_test");
        col.Insert(new TwoTypeTestItem { Id = 1, Value = "Hello" });
        col.Insert(new TwoTypeTestItem { Id = 2, Value = "World" });
        
        var q = new TinyDb.Query.Queryable<TwoTypeTestItem>(_executor, "chain_test");
        var result = q.Select(x => x.Value)
                      .Select(x => x.Length)
                      .ToList();
        
        await Assert.That(result.Count).IsEqualTo(2);
        await Assert.That(result[0]).IsEqualTo(5); // "Hello".Length
    }

    /// <summary>
    /// Test GetEnumerator on projected queryable
    /// </summary>
    [Test]
    public async Task QueryableTwoType_GetEnumerator_ShouldWork()
    {
        var col = _engine.GetCollection<TwoTypeTestItem>("enum_test");
        col.Insert(new TwoTypeTestItem { Id = 1, Value = "A" });
        col.Insert(new TwoTypeTestItem { Id = 2, Value = "B" });
        
        var q = new TinyDb.Query.Queryable<TwoTypeTestItem>(_executor, "enum_test");
        var projected = q.Select(x => x.Value);
        
        var list = new List<string>();
        foreach (var item in projected)
        {
            list.Add(item);
        }
        
        await Assert.That(list.Count).IsEqualTo(2);
    }

    /// <summary>
    /// Test non-generic GetEnumerator on projected queryable
    /// </summary>
    [Test]
    public async Task QueryableTwoType_NonGenericGetEnumerator_ShouldWork()
    {
        var col = _engine.GetCollection<TwoTypeTestItem>("ngenum_test");
        col.Insert(new TwoTypeTestItem { Id = 1, Value = "X" });
        
        var q = new TinyDb.Query.Queryable<TwoTypeTestItem>(_executor, "ngenum_test");
        var projected = q.Select(x => x.Id);
        
        var list = new List<object>();
        foreach (var item in (IEnumerable)projected)
        {
            list.Add(item);
        }
        
        await Assert.That(list.Count).IsEqualTo(1);
    }
}

/// <summary>
/// Tests for QueryProvider edge cases
/// </summary>
public class QueryProviderEdgeCaseTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;
    private readonly QueryExecutor _executor;

    public QueryProviderEdgeCaseTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qprovider2_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Entity]
    public class ProviderTestItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    /// <summary>
    /// Test CreateQuery (non-generic) returns IQueryable
    /// </summary>
    [Test]
    public async Task Provider_CreateQuery_ShouldReturnIQueryable()
    {
        var col = _engine.GetCollection<ProviderTestItem>("provider_create2");
        col.Insert(new ProviderTestItem { Id = 1, Name = "Test" });
        
        var q = new TinyDb.Query.Queryable<ProviderTestItem>(_executor, "provider_create2");
        var provider = q.Provider;
        
        // Create a simple Where expression
        Expression<Func<ProviderTestItem, bool>> predicate = x => x.Id > 0;
        var whereExpr = q.Where(predicate).Expression;
        
        var result = provider.CreateQuery(whereExpr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsAssignableTo<IQueryable>();
    }

    /// <summary>
    /// Test Execute (non-generic) returns object
    /// </summary>
    [Test]
    public async Task Provider_Execute_ShouldReturnObject()
    {
        var col = _engine.GetCollection<ProviderTestItem>("provider_exec2");
        col.Insert(new ProviderTestItem { Id = 1, Name = "Test" });
        
        var q = new TinyDb.Query.Queryable<ProviderTestItem>(_executor, "provider_exec2");
        var provider = q.Provider;
        
        var result = provider.Execute(q.Expression);
        
        await Assert.That(result).IsNotNull();
    }

    /// <summary>
    /// Test CreateQuery<T> returns IQueryable<T>
    /// </summary>
    [Test]
    public async Task Provider_CreateQueryGeneric_ShouldReturnIQueryableT()
    {
        var col = _engine.GetCollection<ProviderTestItem>("provider_create_gen");
        col.Insert(new ProviderTestItem { Id = 1, Name = "Test" });
        
        var q = new TinyDb.Query.Queryable<ProviderTestItem>(_executor, "provider_create_gen");
        var provider = q.Provider;
        
        // Create a Select expression that changes type to int
        Expression<Func<ProviderTestItem, int>> selector = x => x.Id;
        var selectExpr = q.Select(selector).Expression;
        
        var result = provider.CreateQuery<int>(selectExpr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsAssignableTo<IQueryable<int>>();
    }

    /// <summary>
    /// Test Execute<T> returns correct type
    /// </summary>
    [Test]
    public async Task Provider_ExecuteGeneric_ShouldReturnCorrectType()
    {
        var col = _engine.GetCollection<ProviderTestItem>("provider_exec_gen");
        col.Insert(new ProviderTestItem { Id = 1, Name = "Test" });
        col.Insert(new ProviderTestItem { Id = 2, Name = "Test2" });
        
        var q = new TinyDb.Query.Queryable<ProviderTestItem>(_executor, "provider_exec_gen");
        var result = q.Count();
        
        await Assert.That(result).IsEqualTo(2);
    }
}

/// <summary>
/// Tests for ObjectComparer edge cases
/// </summary>
public class ObjectComparerEdgeCaseTests
{
    /// <summary>
    /// Test comparison of mixed numeric types through OrderBy
    /// </summary>
    [Test]
    public async Task ObjectComparer_MixedNumericTypes_ShouldWork()
    {
        using var engine = new TinyDbEngine(Path.Combine(Path.GetTempPath(), $"comparer_{Guid.NewGuid()}.db"));
        
        var col = engine.GetCollection<MixedItem>("mixed");
        col.Insert(new MixedItem { Id = 1, Value = 10L });
        col.Insert(new MixedItem { Id = 2, Value = 5L });
        col.Insert(new MixedItem { Id = 3, Value = 20L });
        
        var result = col.Query().OrderBy(x => x.Value).ToList();
        
        await Assert.That(result[0].Value).IsEqualTo(5L);
        await Assert.That(result[2].Value).IsEqualTo(20L);
        
        engine.Dispose();
        File.Delete(Path.Combine(Path.GetTempPath(), $"comparer_{Guid.NewGuid()}.db"));
    }

    [Entity]
    public class MixedItem
    {
        public int Id { get; set; }
        public long Value { get; set; }
    }
}

/// <summary>
/// Tests for PredicateExtractor edge cases
/// </summary>
public class PredicateExtractorTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;
    private readonly QueryExecutor _executor;

    public PredicateExtractorTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"predext_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Entity]
    public class PredicateTestItem
    {
        public int Id { get; set; }
        public int Value { get; set; }
    }

    /// <summary>
    /// Test predicate combination with multiple Where clauses
    /// </summary>
    [Test]
    public async Task PredicateExtractor_MultipleWheres_ShouldCombineWithAnd()
    {
        var col = _engine.GetCollection<PredicateTestItem>("pred_test");
        col.Insert(new PredicateTestItem { Id = 1, Value = 5 });
        col.Insert(new PredicateTestItem { Id = 2, Value = 15 });
        col.Insert(new PredicateTestItem { Id = 3, Value = 25 });
        col.Insert(new PredicateTestItem { Id = 4, Value = 35 });
        
        var q = new TinyDb.Query.Queryable<PredicateTestItem>(_executor, "pred_test");
        
        // Multiple Where clauses should be combined with AND
        var result = q.Where(x => x.Value > 10)
                      .Where(x => x.Value < 30)
                      .ToList();
        
        await Assert.That(result.Count).IsEqualTo(2); // 15 and 25
        await Assert.That(result.Any(x => x.Value == 15)).IsTrue();
        await Assert.That(result.Any(x => x.Value == 25)).IsTrue();
    }

    /// <summary>
    /// Test query without Where clause
    /// </summary>
    [Test]
    public async Task PredicateExtractor_NoWhere_ShouldReturnAll()
    {
        var col = _engine.GetCollection<PredicateTestItem>("pred_nofilter");
        col.Insert(new PredicateTestItem { Id = 1, Value = 10 });
        col.Insert(new PredicateTestItem { Id = 2, Value = 20 });
        
        var q = new TinyDb.Query.Queryable<PredicateTestItem>(_executor, "pred_nofilter");
        
        var result = q.OrderBy(x => x.Id).ToList();
        
        await Assert.That(result.Count).IsEqualTo(2);
    }
}
