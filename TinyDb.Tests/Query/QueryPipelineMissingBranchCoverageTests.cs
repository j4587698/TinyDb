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

public class QueryPipelineMissingBranchCoverageTests
{
    public sealed class Item
    {
        public int Id { get; set; }
        public string Category { get; set; } = "";
    }

    [Test]
    public async Task ExecuteAot_WhenExtractedPredicateProvided_ShouldSkipWhere()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" }
        };

        var query = TestQueryables.InMemory(items).Where(x => x.Id > 1);

        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: Expression.Constant(true))!;
        var list = result.Cast<Item>().Select(x => x.Id).OrderBy(x => x).ToList();
        await Assert.That(list.SequenceEqual(new[] { 1, 2 })).IsTrue();
    }

    [Test]
    public async Task ExecuteAot_SkipTake_WithNonConstantArgument_ShouldBeIgnored()
    {
        var items = new List<Item>
        {
            new() { Id = 3, Category = "C" },
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" }
        };

        var orderedQuery = TestQueryables.InMemory(items).OrderBy(x => x.Id);
        var sourceExpr = orderedQuery.Expression;

        var holder = new Holder { Value = 1 };
        var countExpr = Expression.Property(Expression.Constant(holder), ExpressionMemberInfo.Property<Holder, int>(x => x.Value));

        var skipMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, int, IQueryable<Item>>>)
                ((q, count) => Queryable.Skip(q, count))).Body).Method;
        var takeMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, int, IQueryable<Item>>>)
                ((q, count) => Queryable.Take(q, count))).Body).Method;

        var skipExpr = Expression.Call(skipMethod, sourceExpr, countExpr);
        var takeExpr = Expression.Call(takeMethod, skipExpr, countExpr);

        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(takeExpr, items, extractedPredicate: null)!;
        var ids = result.Cast<Item>().Select(x => x.Id).ToList();

        // AOT mode only applies Skip/Take when argument is a ConstantExpression.
        await Assert.That(ids.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task ExecuteAot_WhereGeneric_ShouldHandleNullItemsAndFalsePredicates()
    {
        var items = new List<Item>
        {
            null!,
            new Item { Id = 1, Category = "A" },
            new Item { Id = 2, Category = "B" }
        };

        var query = TestQueryables.InMemory(items).Where(x => x.Id > 1);

        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null)!;
        var list = result.Cast<Item>().Where(x => x != null).Select(x => x.Id).ToList();

        await Assert.That(list.SequenceEqual(new[] { 2 })).IsTrue();
    }

    [Test]
    public async Task ExecuteAot_WhereAfterSelect_ShouldUseNonGenericWhereLambda_AndSkipNullItems()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" },
            new() { Id = 3, Category = null! }
        };

        var query = TestQueryables.InMemory(items)
            .Select(x => x.Category)
            .Where(x => x == "A");

        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null)!;
        var categories = result.Cast<object?>().OfType<string>().ToList();

        await Assert.That(categories.SequenceEqual(new[] { "A" })).IsTrue();
    }

    [Test]
    public async Task ExecuteAot_UnsupportedOperation_ShouldThrow()
    {
        var items = new List<Item> { new() { Id = 1, Category = "A" } };
        var sourceExpr = TestQueryables.InMemory(items).Expression;

        var reverseMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, IQueryable<Item>>>)(q => Queryable.Reverse(q))).Body).Method;
        var reverseExpr = Expression.Call(reverseMethod, sourceExpr);
        await Assert.That(() => QueryPipeline.ExecuteAotForTests<Item>(reverseExpr, items, extractedPredicate: null))
            .ThrowsExactly<NotSupportedException>();
    }

    [Test]
    public async Task ExecuteAot_OrderByThenByDescending_ShouldUseThenByDescendingBranch()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "B" },
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "C" }
        };

        var query = TestQueryables.InMemory(items)
            .OrderBy(x => x.Id)
            .ThenByDescending(x => x.Category);

        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null)!;
        var ordered = result.Cast<Item>().Select(x => $"{x.Id}:{x.Category}").ToList();

        await Assert.That(ordered.SequenceEqual(new[] { "1:B", "1:A", "2:C" })).IsTrue();
    }

    [Test]
    public async Task ExecuteAot_TerminalOperations_ShouldCoverTerminalSwitchCases()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" },
            new() { Id = 3, Category = "B" }
        };

        var sourceExpr = TestQueryables.InMemory(items).Expression;

        object Execute(Expression expr) =>
            QueryPipeline.ExecuteAotForTests<Item>(expr, items, extractedPredicate: null)!;

        var countMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, int>>)(q => Queryable.Count(q))).Body).Method;
        var countPredicateMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, int>>)
                ((q, predicate) => Queryable.Count(q, predicate))).Body).Method;
        var longCountMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, long>>)(q => Queryable.LongCount(q))).Body).Method;
        var anyMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, bool>>)(q => Queryable.Any(q))).Body).Method;
        var anyPredicateMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, bool>>)
                ((q, predicate) => Queryable.Any(q, predicate))).Body).Method;
        var allMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, bool>>)
                ((q, predicate) => Queryable.All(q, predicate))).Body).Method;
        var firstMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Item>>)(q => Queryable.First(q))).Body).Method;
        var firstOrDefaultPredicateMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, Item?>>)
                ((q, predicate) => Queryable.FirstOrDefault(q, predicate))).Body).Method;
        var singlePredicateMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, Item>>)
                ((q, predicate) => Queryable.Single(q, predicate))).Body).Method;
        var singleOrDefaultPredicateMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, Item?>>)
                ((q, predicate) => Queryable.SingleOrDefault(q, predicate))).Body).Method;
        var lastMethod = ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Item>>)(q => Queryable.Last(q))).Body).Method;
        var lastOrDefaultPredicateMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, bool>>, Item?>>)
                ((q, predicate) => Queryable.LastOrDefault(q, predicate))).Body).Method;
        var elementAtMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, int, Item>>)
                ((q, index) => Queryable.ElementAt(q, index))).Body).Method;
        var elementAtOrDefaultMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, int, Item?>>)
                ((q, index) => Queryable.ElementAtOrDefault(q, index))).Body).Method;

        var countExpr = Expression.Call(countMethod, sourceExpr);
        await Assert.That((int)Execute(countExpr)).IsEqualTo(3);

        Expression<Func<Item, bool>> isCategoryB = x => x.Category == "B";
        var countWhereExpr = Expression.Call(countPredicateMethod, sourceExpr, Expression.Quote(isCategoryB));
        await Assert.That((int)Execute(countWhereExpr)).IsEqualTo(2);

        var longCountExpr = Expression.Call(longCountMethod, sourceExpr);
        await Assert.That((long)Execute(longCountExpr)).IsEqualTo(3L);

        var anyExpr = Expression.Call(anyMethod, sourceExpr);
        await Assert.That((bool)Execute(anyExpr)).IsTrue();

        var anyWhereExpr = Expression.Call(anyPredicateMethod, sourceExpr, Expression.Quote(isCategoryB));
        await Assert.That((bool)Execute(anyWhereExpr)).IsTrue();

        Expression<Func<Item, bool>> allIdsPositive = x => x.Id > 0;
        var allExpr = Expression.Call(allMethod, sourceExpr, Expression.Quote(allIdsPositive));
        await Assert.That((bool)Execute(allExpr)).IsTrue();

        Expression<Func<Item, bool>> allCategoryA = x => x.Category == "A";
        var allFalseExpr = Expression.Call(allMethod, sourceExpr, Expression.Quote(allCategoryA));
        await Assert.That((bool)Execute(allFalseExpr)).IsFalse();

        var firstExpr = Expression.Call(firstMethod, sourceExpr);
        await Assert.That(((Item)Execute(firstExpr)).Id).IsEqualTo(1);

        var firstOrDefaultExpr = Expression.Call(firstOrDefaultPredicateMethod, sourceExpr, Expression.Quote((Expression<Func<Item, bool>>)(x => x.Id == 999)));
        await Assert.That(Execute(firstOrDefaultExpr)).IsNull();

        var singleExpr = Expression.Call(singlePredicateMethod, sourceExpr, Expression.Quote((Expression<Func<Item, bool>>)(x => x.Id == 1)));
        await Assert.That(((Item)Execute(singleExpr)).Id).IsEqualTo(1);

        var singleOrDefaultExpr = Expression.Call(singleOrDefaultPredicateMethod, sourceExpr, Expression.Quote((Expression<Func<Item, bool>>)(x => x.Id == 999)));
        await Assert.That(Execute(singleOrDefaultExpr)).IsNull();

        var lastExpr = Expression.Call(lastMethod, sourceExpr);
        await Assert.That(((Item)Execute(lastExpr)).Id).IsEqualTo(3);

        var lastOrDefaultExpr = Expression.Call(lastOrDefaultPredicateMethod, sourceExpr, Expression.Quote((Expression<Func<Item, bool>>)(x => x.Id == 999)));
        await Assert.That(Execute(lastOrDefaultExpr)).IsNull();

        var elementAtExpr = Expression.Call(elementAtMethod, sourceExpr, Expression.Constant(1));
        await Assert.That(((Item)Execute(elementAtExpr)).Id).IsEqualTo(2);

        var elementAtOrDefaultExpr = Expression.Call(elementAtOrDefaultMethod, sourceExpr, Expression.Constant(99));
        await Assert.That(Execute(elementAtOrDefaultExpr)).IsNull();
    }

    [Test]
    public async Task IsTerminal_ShouldRecognizeAllKnownTerminalOperations()
    {
        foreach (var name in new[]
                 {
                     "Count", "LongCount", "Any", "All", "First", "FirstOrDefault", "Single", "SingleOrDefault",
                     "Last", "LastOrDefault", "ElementAt", "ElementAtOrDefault"
                 })
        {
            await Assert.That(QueryPipeline.IsTerminalForTests(name)).IsTrue();
        }

        await Assert.That(QueryPipeline.IsTerminalForTests("Where")).IsFalse();
    }

    [Test]
    public async Task ExecuteWhereHelpers_WhenPredicateEvaluatesToNonBool_ShouldCoverBranches()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" }
        };

        // ExecuteWhereGeneric<T>: provide a LambdaExpression that returns a non-bool value (Id)
        var queryable = TestQueryables.InMemory(items);
        Expression<Func<Item, int>> selector = x => x.Id;
        var selectMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, Expression<Func<Item, int>>, IQueryable<int>>>)
                ((q, sel) => Queryable.Select(q, sel))).Body).Method;
        var dummyCall = Expression.Call(selectMethod, queryable.Expression, Expression.Quote(selector));

        var filteredGeneric = QueryPipeline.ExecuteWhereGenericForTests(items, dummyCall);
        await Assert.That(filteredGeneric.Any()).IsFalse();

        // ExecuteWhereLambda: same idea for the non-generic path
        var source = new object?[] { items[0], null, items[1] };
        var filtered = QueryPipeline.ExecuteWhereLambdaForTests(source, selector);
        await Assert.That(filtered.Cast<object>().Any()).IsFalse();
    }

    private sealed class Holder
    {
        public int Value { get; set; }
    }
}
