using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryPipelineTerminalOrderingAggregateAdditionalCoverageTests
{
    public sealed class Item
    {
        public int Id { get; set; }
        public string Category { get; set; } = "";
    }

    [Test]
    public async Task ExecuteAot_SkipTakeDistinct_OrderByThenByDescending_ShouldWork()
    {
        var items = new List<Item>
        {
            new() { Id = 3, Category = "B" },
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "A" },
            new() { Id = 4, Category = "C" }
        };

        var orderedSliceQuery = TestQueryables.InMemory(items)
            .OrderBy(x => x.Id)
            .Skip(1)
            .Take(2);

        var orderedSlice = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(orderedSliceQuery.Expression, items, extractedPredicate: null)!;
        var orderedIds = orderedSlice.Cast<Item>().Select(x => x.Id).ToList();
        await Assert.That(orderedIds.SequenceEqual(new[] { 2, 3 })).IsTrue();

        var distinctCategoryQuery = TestQueryables.InMemory(items)
            .Select(x => x.Category)
            .Distinct();

        var distinctCategories = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(distinctCategoryQuery.Expression, items, extractedPredicate: null)!;
        var categories = distinctCategories.Cast<object>().Select(x => (string)x!).OrderBy(x => x).ToList();
        await Assert.That(categories.SequenceEqual(new[] { "A", "B", "C" })).IsTrue();

        var thenByQuery = TestQueryables.InMemory(items)
            .OrderBy(x => x.Category)
            .ThenByDescending(x => x.Id);

        var thenByResult = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(thenByQuery.Expression, items, extractedPredicate: null)!;
        var pairs = thenByResult.Cast<Item>().Select(x => (x.Category, x.Id)).ToList();
        await Assert.That(pairs.SequenceEqual(new[] { ("A", 2), ("A", 1), ("B", 3), ("C", 4) })).IsTrue();
    }

    [Test]
    public async Task ExecuteAot_ThenBy_WithoutExistingOrder_ShouldFallbackToOrderBy()
    {
        var items = new List<Item>
        {
            new() { Id = 3, Category = "B" },
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "A" }
        };

        var orderedQueryable = TestQueryables.InMemory(items).OrderBy(x => x.Id);
        var sourceExpr = Expression.Constant(orderedQueryable, typeof(IOrderedQueryable<Item>));
        Expression<Func<Item, int>> keySelector = x => x.Id;

        var thenByMethod =
            ((MethodCallExpression)((Expression<Func<IOrderedQueryable<Item>, Expression<Func<Item, int>>, IOrderedQueryable<Item>>>)
                ((q, key) => Queryable.ThenBy(q, key))).Body).Method;
        var thenByExpr = Expression.Call(thenByMethod, sourceExpr, Expression.Quote(keySelector));

        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(thenByExpr, items, extractedPredicate: null)!;
        var ids = result.Cast<Item>().Select(x => x.Id).ToList();
        await Assert.That(ids.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task ExecuteAot_Terminals_ShouldWork_And_AllMalformedPredicate_ShouldReturnNull()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" }
        };

        var empty = new List<Item>();

        var sourceExpr = TestQueryables.InMemory(items).Expression;
        var emptySourceExpr = TestQueryables.InMemory(empty).Expression;

        Expression<Func<Item, bool>> idGreaterThan1 = x => x.Id > 1;
        Expression<Func<Item, bool>> allIdGreaterThan0 = x => x.Id > 0;
        Expression<Func<Item, bool>> allIdGreaterThan1 = x => x.Id > 1;

        var countMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, int>>)(q => Queryable.Count(q))).Body).Method;
        var countExpr = Expression.Call(countMethod, sourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(countExpr, items, extractedPredicate: null)).IsEqualTo(2);

        var longCountMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, long>>)(q => Queryable.LongCount(q))).Body).Method;
        var longCountExpr = Expression.Call(longCountMethod, sourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(longCountExpr, items, extractedPredicate: null)).IsEqualTo(2L);

        var anyMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, bool>>)(q => Queryable.Any(q))).Body).Method;
        var anyExpr = Expression.Call(anyMethod, sourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(anyExpr, items, extractedPredicate: null)).IsEqualTo(true);

        var anyOnEmptyExpr = Expression.Call(anyMethod, emptySourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(anyOnEmptyExpr, empty, extractedPredicate: null)).IsEqualTo(false);

        var anyPredicateMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, bool>>)
                ((q, pred) => Queryable.Any(q, pred))).Body).Method;
        var anyPredicateExpr = Expression.Call(anyPredicateMethod, sourceExpr, Expression.Quote(idGreaterThan1));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(anyPredicateExpr, items, extractedPredicate: null)).IsEqualTo(true);

        var allMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, bool>>)
                ((q, pred) => Queryable.All(q, pred))).Body).Method;
        var allTrueExpr = Expression.Call(allMethod, sourceExpr, Expression.Quote(allIdGreaterThan0));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(allTrueExpr, items, extractedPredicate: null)).IsEqualTo(true);

        var allFalseExpr = Expression.Call(allMethod, sourceExpr, Expression.Quote(allIdGreaterThan1));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(allFalseExpr, items, extractedPredicate: null)).IsEqualTo(false);

        var firstMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Item>>)(q => Queryable.First(q))).Body).Method;
        var firstExpr = Expression.Call(firstMethod, sourceExpr);
        var first = (Item)QueryPipeline.ExecuteAotForTests<Item>(firstExpr, items, extractedPredicate: null)!;
        await Assert.That(first.Id).IsEqualTo(1);

        var firstOrDefaultMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Item?>>)(q => Queryable.FirstOrDefault(q))).Body).Method;
        var firstOrDefaultEmptyExpr = Expression.Call(firstOrDefaultMethod, emptySourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(firstOrDefaultEmptyExpr, empty, extractedPredicate: null)).IsNull();

        var singleMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, Item>>)
                ((q, pred) => Queryable.Single(q, pred))).Body).Method;
        var singlePredicateExpr = Expression.Call(singleMethod, sourceExpr, Expression.Quote(idGreaterThan1));
        var single = (Item)QueryPipeline.ExecuteAotForTests<Item>(singlePredicateExpr, items, extractedPredicate: null)!;
        await Assert.That(single.Id).IsEqualTo(2);

        var singleOrDefaultMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, Item?>>)
                ((q, pred) => Queryable.SingleOrDefault(q, pred))).Body).Method;
        var singleOrDefaultPredicateExpr = Expression.Call(singleOrDefaultMethod, sourceExpr, Expression.Quote((Expression<Func<Item, bool>>)(x => x.Id > 10)));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(singleOrDefaultPredicateExpr, items, extractedPredicate: null)).IsNull();

        var lastMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Item>>)(q => Queryable.Last(q))).Body).Method;
        var lastExpr = Expression.Call(lastMethod, sourceExpr);
        var last = (Item)QueryPipeline.ExecuteAotForTests<Item>(lastExpr, items, extractedPredicate: null)!;
        await Assert.That(last.Id).IsEqualTo(2);

        var lastOrDefaultMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Item?>>)(q => Queryable.LastOrDefault(q))).Body).Method;
        var lastOrDefaultEmptyExpr = Expression.Call(lastOrDefaultMethod, emptySourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(lastOrDefaultEmptyExpr, empty, extractedPredicate: null)).IsNull();

        var elementAtMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, int, Item>>)
                ((q, index) => Queryable.ElementAt(q, index))).Body).Method;
        var elementAtExpr = Expression.Call(elementAtMethod, sourceExpr, Expression.Constant(1));
        var elementAt = (Item)QueryPipeline.ExecuteAotForTests<Item>(elementAtExpr, items, extractedPredicate: null)!;
        await Assert.That(elementAt.Id).IsEqualTo(2);

        var elementAtOrDefaultMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, int, Item?>>)
                ((q, index) => Queryable.ElementAtOrDefault(q, index))).Body).Method;
        var elementAtOrDefaultOutOfRangeExpr = Expression.Call(elementAtOrDefaultMethod, sourceExpr, Expression.Constant(10));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(elementAtOrDefaultOutOfRangeExpr, items, extractedPredicate: null)).IsNull();

        // Malformed: "All" is considered terminal, but the predicate isn't quoted lambda.
        // This drives ExecuteTerminal's default return branch.
        var malformedPredicate = (Expression<Func<Item, bool>>)(x => true);
        var malformedAllExpr = Expression.Call(allMethod, sourceExpr, Expression.Constant(malformedPredicate));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(malformedAllExpr, items, extractedPredicate: null)).IsNull();
    }

    [Test]
    public async Task ExecuteAot_GroupBy_Aggregations_ShouldWork()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "A" },
            new() { Id = 3, Category = "B" },
            new() { Id = 4, Category = "B" }
        };

        var empty = new List<Item>();

        var source = TestQueryables.InMemory(items);
        var emptySource = TestQueryables.InMemory(empty);

        Expression<Func<Item, string>> groupKey = x => x.Category;

        var groupByMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, string>>, IQueryable<IGrouping<string, Item>>>>)
                ((q, key) => Queryable.GroupBy(q, key))).Body).Method;
        var groupByExpr = Expression.Call(groupByMethod, source.Expression, Expression.Quote(groupKey));

        var emptyGroupByExpr = Expression.Call(groupByMethod, emptySource.Expression, Expression.Quote(groupKey));

        Expression<Func<IGrouping<string, Item>, decimal>> countAsDecimal = g => (decimal)g.Count();
        Expression<Func<IGrouping<string, Item>, string>> keySelector = g => g.Key;

        var sumMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<IGrouping<string, Item>>, Expression<Func<IGrouping<string, Item>, decimal>>, decimal>>)
                ((q, selector) => Queryable.Sum(q, selector))).Body).Method;
        var sumExpr = Expression.Call(sumMethod, groupByExpr, Expression.Quote(countAsDecimal));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(sumExpr, items, extractedPredicate: null)).IsEqualTo(4m);

        var averageMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<IGrouping<string, Item>>, Expression<Func<IGrouping<string, Item>, decimal>>, decimal>>)
                ((q, selector) => Queryable.Average(q, selector))).Body).Method;
        var avgExpr = Expression.Call(averageMethod, groupByExpr, Expression.Quote(countAsDecimal));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(avgExpr, items, extractedPredicate: null)).IsEqualTo(2m);

        var minMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<IGrouping<string, Item>>, Expression<Func<IGrouping<string, Item>, string>>, string?>>)
                ((q, selector) => Queryable.Min(q, selector))).Body).Method;
        var minKeyExpr = Expression.Call(minMethod, groupByExpr, Expression.Quote(keySelector));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(minKeyExpr, items, extractedPredicate: null)).IsEqualTo("A");

        var maxMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<IGrouping<string, Item>>, Expression<Func<IGrouping<string, Item>, string>>, string?>>)
                ((q, selector) => Queryable.Max(q, selector))).Body).Method;
        var maxKeyExpr = Expression.Call(maxMethod, groupByExpr, Expression.Quote(keySelector));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(maxKeyExpr, items, extractedPredicate: null)).IsEqualTo("B");

        var avgEmptyExpr = Expression.Call(averageMethod, emptyGroupByExpr, Expression.Quote(countAsDecimal));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(avgEmptyExpr, empty, extractedPredicate: null)).IsEqualTo(0m);

        var minEmptyExpr = Expression.Call(minMethod, emptyGroupByExpr, Expression.Quote(keySelector));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(minEmptyExpr, empty, extractedPredicate: null)).IsNull();

        var maxEmptyExpr = Expression.Call(maxMethod, emptyGroupByExpr, Expression.Quote(keySelector));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(maxEmptyExpr, empty, extractedPredicate: null)).IsNull();
    }

    [Test]
    public async Task ExecuteAot_NonGenericOrderBy_ThenBy_And_Fallback_ShouldCoverBranches()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "B" },
            new() { Id = 2, Category = "A" },
            new() { Id = 3, Category = "C" }
        };

        // Select -> OrderBy -> ThenBy (non-generic ordered branch)
        var queryThenBy = TestQueryables.InMemory(items)
            .Select(x => x.Id)
            .OrderBy(x => x)
            .ThenBy(x => x);

        var thenByResult = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(queryThenBy.Expression, items, extractedPredicate: null)!;
        var ids = thenByResult.Cast<object>().Select(x => (int)x!).ToList();
        await Assert.That(ids.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();

        // Select -> OrderBy(constant) -> ThenByDescending (non-generic ordered branch)
        var queryThenByDesc = TestQueryables.InMemory(items)
            .Select(x => x.Id)
            .OrderBy(_ => 0)
            .ThenByDescending(x => x);

        var thenByDescResult = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(queryThenByDesc.Expression, items, extractedPredicate: null)!;
        var idsDesc = thenByDescResult.Cast<object>().Select(x => (int)x!).ToList();
        await Assert.That(idsDesc.SequenceEqual(new[] { 3, 2, 1 })).IsTrue();

        // ThenBy on an un-ordered in-memory source should hit the generic fallback (OrderBy) branch.
        var orderedPlaceholder = TestQueryables.InMemory(items).OrderBy(x => x.Id);
        var thenByMethod =
            ((MethodCallExpression)((Expression<Func<IOrderedQueryable<Item>, Expression<Func<Item, int>>, IOrderedQueryable<Item>>>)
                ((q, key) => Queryable.ThenBy(q, key))).Body).Method;
        var thenByFallbackExpr = Expression.Call(thenByMethod, Expression.Constant(orderedPlaceholder), Expression.Quote((Expression<Func<Item, int>>)(x => x.Id)));

        var thenByFallbackResult = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(thenByFallbackExpr, items, extractedPredicate: null)!;
        var fallbackIds = thenByFallbackResult.Cast<object>().Select(x => ((Item)x!).Id).ToList();
        await Assert.That(fallbackIds.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
    }
}
