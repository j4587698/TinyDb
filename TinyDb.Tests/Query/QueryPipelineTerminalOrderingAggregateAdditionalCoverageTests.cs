using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
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

        var orderedSliceQuery = items.AsQueryable()
            .OrderBy(x => x.Id)
            .Skip(1)
            .Take(2);

        var orderedSlice = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(orderedSliceQuery.Expression, items, extractedPredicate: null)!;
        var orderedIds = orderedSlice.Cast<Item>().Select(x => x.Id).ToList();
        await Assert.That(orderedIds.SequenceEqual(new[] { 2, 3 })).IsTrue();

        var distinctCategoryQuery = items.AsQueryable()
            .Select(x => x.Category)
            .Distinct();

        var distinctCategories = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(distinctCategoryQuery.Expression, items, extractedPredicate: null)!;
        var categories = distinctCategories.Cast<object>().Select(x => (string)x!).OrderBy(x => x).ToList();
        await Assert.That(categories.SequenceEqual(new[] { "A", "B", "C" })).IsTrue();

        var thenByQuery = items.AsQueryable()
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

        var orderedQueryable = items.AsQueryable().OrderBy(x => x.Id);
        var sourceExpr = Expression.Constant(orderedQueryable, typeof(IOrderedQueryable<Item>));
        Expression<Func<Item, int>> keySelector = x => x.Id;

        var thenByExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.ThenBy),
            new[] { typeof(Item), typeof(int) },
            sourceExpr,
            Expression.Quote(keySelector));

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

        var sourceExpr = items.AsQueryable().Expression;
        var emptySourceExpr = empty.AsQueryable().Expression;

        Expression<Func<Item, bool>> idGreaterThan1 = x => x.Id > 1;
        Expression<Func<Item, bool>> allIdGreaterThan0 = x => x.Id > 0;
        Expression<Func<Item, bool>> allIdGreaterThan1 = x => x.Id > 1;

        var countExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Count), new[] { typeof(Item) }, sourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(countExpr, items, extractedPredicate: null)).IsEqualTo(2);

        var longCountExpr = Expression.Call(typeof(Queryable), nameof(Queryable.LongCount), new[] { typeof(Item) }, sourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(longCountExpr, items, extractedPredicate: null)).IsEqualTo(2L);

        var anyExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Any), new[] { typeof(Item) }, sourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(anyExpr, items, extractedPredicate: null)).IsEqualTo(true);

        var anyOnEmptyExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Any), new[] { typeof(Item) }, emptySourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(anyOnEmptyExpr, empty, extractedPredicate: null)).IsEqualTo(false);

        var anyPredicateExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Any), new[] { typeof(Item) }, sourceExpr, Expression.Quote(idGreaterThan1));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(anyPredicateExpr, items, extractedPredicate: null)).IsEqualTo(true);

        var allTrueExpr = Expression.Call(typeof(Queryable), nameof(Queryable.All), new[] { typeof(Item) }, sourceExpr, Expression.Quote(allIdGreaterThan0));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(allTrueExpr, items, extractedPredicate: null)).IsEqualTo(true);

        var allFalseExpr = Expression.Call(typeof(Queryable), nameof(Queryable.All), new[] { typeof(Item) }, sourceExpr, Expression.Quote(allIdGreaterThan1));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(allFalseExpr, items, extractedPredicate: null)).IsEqualTo(false);

        var firstExpr = Expression.Call(typeof(Queryable), nameof(Queryable.First), new[] { typeof(Item) }, sourceExpr);
        var first = (Item)QueryPipeline.ExecuteAotForTests<Item>(firstExpr, items, extractedPredicate: null)!;
        await Assert.That(first.Id).IsEqualTo(1);

        var firstOrDefaultEmptyExpr = Expression.Call(typeof(Queryable), nameof(Queryable.FirstOrDefault), new[] { typeof(Item) }, emptySourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(firstOrDefaultEmptyExpr, empty, extractedPredicate: null)).IsNull();

        var singlePredicateExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Single), new[] { typeof(Item) }, sourceExpr, Expression.Quote(idGreaterThan1));
        var single = (Item)QueryPipeline.ExecuteAotForTests<Item>(singlePredicateExpr, items, extractedPredicate: null)!;
        await Assert.That(single.Id).IsEqualTo(2);

        var singleOrDefaultPredicateExpr = Expression.Call(typeof(Queryable), nameof(Queryable.SingleOrDefault), new[] { typeof(Item) }, sourceExpr, Expression.Quote((Expression<Func<Item, bool>>)(x => x.Id > 10)));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(singleOrDefaultPredicateExpr, items, extractedPredicate: null)).IsNull();

        var lastExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Last), new[] { typeof(Item) }, sourceExpr);
        var last = (Item)QueryPipeline.ExecuteAotForTests<Item>(lastExpr, items, extractedPredicate: null)!;
        await Assert.That(last.Id).IsEqualTo(2);

        var lastOrDefaultEmptyExpr = Expression.Call(typeof(Queryable), nameof(Queryable.LastOrDefault), new[] { typeof(Item) }, emptySourceExpr);
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(lastOrDefaultEmptyExpr, empty, extractedPredicate: null)).IsNull();

        var elementAtExpr = Expression.Call(typeof(Queryable), nameof(Queryable.ElementAt), new[] { typeof(Item) }, sourceExpr, Expression.Constant(1));
        var elementAt = (Item)QueryPipeline.ExecuteAotForTests<Item>(elementAtExpr, items, extractedPredicate: null)!;
        await Assert.That(elementAt.Id).IsEqualTo(2);

        var elementAtOrDefaultOutOfRangeExpr = Expression.Call(typeof(Queryable), nameof(Queryable.ElementAtOrDefault), new[] { typeof(Item) }, sourceExpr, Expression.Constant(10));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(elementAtOrDefaultOutOfRangeExpr, items, extractedPredicate: null)).IsNull();

        // Malformed: "All" is considered terminal, but the predicate isn't quoted lambda.
        // This drives ExecuteTerminal's default return branch.
        var malformedPredicate = (Expression<Func<Item, bool>>)(x => true);
        var malformedAllExpr = Expression.Call(typeof(Queryable), nameof(Queryable.All), new[] { typeof(Item) }, sourceExpr, Expression.Constant(malformedPredicate));
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

        var source = items.AsQueryable();
        var emptySource = empty.AsQueryable();

        Expression<Func<Item, string>> groupKey = x => x.Category;
        var groupType = typeof(IGrouping<,>).MakeGenericType(typeof(string), typeof(Item));

        var groupByExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.GroupBy),
            new[] { typeof(Item), typeof(string) },
            source.Expression,
            Expression.Quote(groupKey));

        var emptyGroupByExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.GroupBy),
            new[] { typeof(Item), typeof(string) },
            emptySource.Expression,
            Expression.Quote(groupKey));

        Expression<Func<IGrouping<string, Item>, decimal>> countAsDecimal = g => (decimal)g.Count();
        Expression<Func<IGrouping<string, Item>, string>> keySelector = g => g.Key;

        var sumExpr = Expression.Call(typeof(Queryable), "Sum", new[] { groupType }, groupByExpr, Expression.Quote(countAsDecimal));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(sumExpr, items, extractedPredicate: null)).IsEqualTo(4m);

        var avgExpr = Expression.Call(typeof(Queryable), "Average", new[] { groupType }, groupByExpr, Expression.Quote(countAsDecimal));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(avgExpr, items, extractedPredicate: null)).IsEqualTo(2m);

        var minKeyExpr = Expression.Call(typeof(Queryable), "Min", new[] { groupType, typeof(string) }, groupByExpr, Expression.Quote(keySelector));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(minKeyExpr, items, extractedPredicate: null)).IsEqualTo("A");

        var maxKeyExpr = Expression.Call(typeof(Queryable), "Max", new[] { groupType, typeof(string) }, groupByExpr, Expression.Quote(keySelector));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(maxKeyExpr, items, extractedPredicate: null)).IsEqualTo("B");

        var avgEmptyExpr = Expression.Call(typeof(Queryable), "Average", new[] { groupType }, emptyGroupByExpr, Expression.Quote(countAsDecimal));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(avgEmptyExpr, empty, extractedPredicate: null)).IsEqualTo(0m);

        var minEmptyExpr = Expression.Call(typeof(Queryable), "Min", new[] { groupType, typeof(string) }, emptyGroupByExpr, Expression.Quote(keySelector));
        await Assert.That(QueryPipeline.ExecuteAotForTests<Item>(minEmptyExpr, empty, extractedPredicate: null)).IsNull();

        var maxEmptyExpr = Expression.Call(typeof(Queryable), "Max", new[] { groupType, typeof(string) }, emptyGroupByExpr, Expression.Quote(keySelector));
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
        var queryThenBy = items.AsQueryable()
            .Select(x => x.Id)
            .OrderBy(x => x)
            .ThenBy(x => x);

        var thenByResult = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(queryThenBy.Expression, items, extractedPredicate: null)!;
        var ids = thenByResult.Cast<object>().Select(x => (int)x!).ToList();
        await Assert.That(ids.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();

        // Select -> OrderBy(constant) -> ThenByDescending (non-generic ordered branch)
        var queryThenByDesc = items.AsQueryable()
            .Select(x => x.Id)
            .OrderBy(_ => 0)
            .ThenByDescending(x => x);

        var thenByDescResult = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(queryThenByDesc.Expression, items, extractedPredicate: null)!;
        var idsDesc = thenByDescResult.Cast<object>().Select(x => (int)x!).ToList();
        await Assert.That(idsDesc.SequenceEqual(new[] { 3, 2, 1 })).IsTrue();

        // ThenBy on an un-ordered in-memory source should hit the generic fallback (OrderBy) branch.
        var orderedPlaceholder = items.AsQueryable().OrderBy(x => x.Id);
        var thenByFallbackExpr = Expression.Call(typeof(Queryable), "ThenBy", new[] { typeof(Item), typeof(int) }, Expression.Constant(orderedPlaceholder), Expression.Quote((Expression<Func<Item, int>>)(x => x.Id)));

        var thenByFallbackResult = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(thenByFallbackExpr, items, extractedPredicate: null)!;
        var fallbackIds = thenByFallbackResult.Cast<object>().Select(x => ((Item)x!).Id).ToList();
        await Assert.That(fallbackIds.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
    }
}
