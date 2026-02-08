using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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

        var query = items.AsQueryable().Where(x => x.Id > 1);

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

        var orderedQuery = items.AsQueryable().OrderBy(x => x.Id);
        var sourceExpr = orderedQuery.Expression;

        var holder = new Holder { Value = 1 };
        var countExpr = Expression.Property(Expression.Constant(holder), ExpressionMemberInfo.Property<Holder, int>(x => x.Value));

        var skipExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Skip), new[] { typeof(Item) }, sourceExpr, countExpr);
        var takeExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Take), new[] { typeof(Item) }, skipExpr, countExpr);

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

        var query = items.AsQueryable().Where(x => x.Id > 1);

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

        var query = items.AsQueryable()
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
        var sourceExpr = items.AsQueryable().Expression;

        var reverseExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Reverse), new[] { typeof(Item) }, sourceExpr);
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

        var query = items.AsQueryable()
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

        var sourceExpr = items.AsQueryable().Expression;

        object Execute(Expression expr) =>
            QueryPipeline.ExecuteAotForTests<Item>(expr, items, extractedPredicate: null)!;

        var countExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Count), new[] { typeof(Item) }, sourceExpr);
        await Assert.That((int)Execute(countExpr)).IsEqualTo(3);

        Expression<Func<Item, bool>> isCategoryB = x => x.Category == "B";
        var countWhereExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Count),
            new[] { typeof(Item) },
            sourceExpr,
            Expression.Quote(isCategoryB));
        await Assert.That((int)Execute(countWhereExpr)).IsEqualTo(2);

        var longCountExpr = Expression.Call(typeof(Queryable), nameof(Queryable.LongCount), new[] { typeof(Item) }, sourceExpr);
        await Assert.That((long)Execute(longCountExpr)).IsEqualTo(3L);

        var anyExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Any), new[] { typeof(Item) }, sourceExpr);
        await Assert.That((bool)Execute(anyExpr)).IsTrue();

        var anyWhereExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Any),
            new[] { typeof(Item) },
            sourceExpr,
            Expression.Quote(isCategoryB));
        await Assert.That((bool)Execute(anyWhereExpr)).IsTrue();

        Expression<Func<Item, bool>> allIdsPositive = x => x.Id > 0;
        var allExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.All),
            new[] { typeof(Item) },
            sourceExpr,
            Expression.Quote(allIdsPositive));
        await Assert.That((bool)Execute(allExpr)).IsTrue();

        Expression<Func<Item, bool>> allCategoryA = x => x.Category == "A";
        var allFalseExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.All),
            new[] { typeof(Item) },
            sourceExpr,
            Expression.Quote(allCategoryA));
        await Assert.That((bool)Execute(allFalseExpr)).IsFalse();

        var firstExpr = Expression.Call(typeof(Queryable), nameof(Queryable.First), new[] { typeof(Item) }, sourceExpr);
        await Assert.That(((Item)Execute(firstExpr)).Id).IsEqualTo(1);

        var firstOrDefaultExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.FirstOrDefault),
            new[] { typeof(Item) },
            sourceExpr,
            Expression.Quote((Expression<Func<Item, bool>>)(x => x.Id == 999)));
        await Assert.That(Execute(firstOrDefaultExpr)).IsNull();

        var singleExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Single),
            new[] { typeof(Item) },
            sourceExpr,
            Expression.Quote((Expression<Func<Item, bool>>)(x => x.Id == 1)));
        await Assert.That(((Item)Execute(singleExpr)).Id).IsEqualTo(1);

        var singleOrDefaultExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.SingleOrDefault),
            new[] { typeof(Item) },
            sourceExpr,
            Expression.Quote((Expression<Func<Item, bool>>)(x => x.Id == 999)));
        await Assert.That(Execute(singleOrDefaultExpr)).IsNull();

        var lastExpr = Expression.Call(typeof(Queryable), nameof(Queryable.Last), new[] { typeof(Item) }, sourceExpr);
        await Assert.That(((Item)Execute(lastExpr)).Id).IsEqualTo(3);

        var lastOrDefaultExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.LastOrDefault),
            new[] { typeof(Item) },
            sourceExpr,
            Expression.Quote((Expression<Func<Item, bool>>)(x => x.Id == 999)));
        await Assert.That(Execute(lastOrDefaultExpr)).IsNull();

        var elementAtExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.ElementAt),
            new[] { typeof(Item) },
            sourceExpr,
            Expression.Constant(1));
        await Assert.That(((Item)Execute(elementAtExpr)).Id).IsEqualTo(2);

        var elementAtOrDefaultExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.ElementAtOrDefault),
            new[] { typeof(Item) },
            sourceExpr,
            Expression.Constant(99));
        await Assert.That(Execute(elementAtOrDefaultExpr)).IsNull();
    }

    [Test]
    public async Task IsTerminal_ShouldRecognizeAllKnownTerminalOperations()
    {
        var method = typeof(QueryPipeline).GetMethod("IsTerminal", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        foreach (var name in new[]
                 {
                     "Count", "LongCount", "Any", "All", "First", "FirstOrDefault", "Single", "SingleOrDefault",
                     "Last", "LastOrDefault", "ElementAt", "ElementAtOrDefault"
                 })
        {
            var isTerminal = (bool)method!.Invoke(null, new object[] { name })!;
            await Assert.That(isTerminal).IsTrue();
        }

        await Assert.That((bool)method!.Invoke(null, new object[] { "Where" })!).IsFalse();
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
        var queryable = items.AsQueryable();
        Expression<Func<Item, int>> selector = x => x.Id;
        var dummyCall = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.Select),
            new[] { typeof(Item), typeof(int) },
            queryable.Expression,
            Expression.Quote(selector));

        var whereGeneric = typeof(QueryPipeline).GetMethod("ExecuteWhereGeneric", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(whereGeneric).IsNotNull();

        var filteredGeneric = (IEnumerable<Item>)whereGeneric!.MakeGenericMethod(typeof(Item))
            .Invoke(null, new object[] { items, dummyCall })!;
        await Assert.That(filteredGeneric.Any()).IsFalse();

        // ExecuteWhereLambda: same idea for the non-generic path
        var whereLambda = typeof(QueryPipeline).GetMethod("ExecuteWhereLambda", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(whereLambda).IsNotNull();

        var source = new object?[] { items[0], null, items[1] };
        var filtered = (IEnumerable)whereLambda!.Invoke(null, new object[] { source, selector })!;
        await Assert.That(filtered.Cast<object>().Any()).IsFalse();
    }

    private sealed class Holder
    {
        public int Value { get; set; }
    }
}
