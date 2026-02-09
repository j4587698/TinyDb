using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryPipelineAdditionalBranchCoverageTests
{
    [Test]
    public async Task ExecuteAot_WhenExtractedPredicateProvided_ShouldSkipInMemoryWhere()
    {
        var data = Seed();
        var source = TestQueryables.InMemory(data);

        var expression = source.Where(x => x.Category.PadLeft(2) == "ZZ").Expression;

        var result = (IEnumerable<Item>)QueryPipeline.ExecuteAotForTests<Item>(
            expression,
            data,
            extractedPredicate: Expression.Constant(true))!;

        await Assert.That(result.Count()).IsEqualTo(data.Count);
    }

    [Test]
    public async Task ExecuteAot_WhenExtractedPredicateNull_ShouldApplyInMemoryWhere()
    {
        var data = Seed();
        var source = TestQueryables.InMemory(data);

        var expression = source.Where(x => x.InStock).Expression;

        var result = (IEnumerable<Item>)QueryPipeline.ExecuteAotForTests<Item>(
            expression,
            data,
            extractedPredicate: null)!;

        await Assert.That(result.Select(x => x.Id).SequenceEqual(new[] { 1, 2, 4, 5 })).IsTrue();
    }

    [Test]
    public async Task ExecuteAot_SkipTake_WithNonConstant_ShouldNoOp()
    {
        var data = Seed();
        var source = TestQueryables.InMemory(data);
        var holder = new Holder { Count = 2 };
        var countExpr = Expression.Property(Expression.Constant(holder), ExpressionMemberInfo.Property<Holder, int>(x => x.Count));

        var skipMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, int, IQueryable<Item>>>)
                ((q, count) => Queryable.Skip(q, count))).Body).Method;
        var takeMethod =
            ((MethodCallExpression)((Expression<Func<IQueryable<Item>, int, IQueryable<Item>>>)
                ((q, count) => Queryable.Take(q, count))).Body).Method;

        var skip = Expression.Call(skipMethod, source.Expression, countExpr);
        var take = Expression.Call(takeMethod, skip, countExpr);

        var result = (IEnumerable<Item>)QueryPipeline.ExecuteAotForTests<Item>(
            take,
            data,
            extractedPredicate: null)!;

        await Assert.That(result.Select(x => x.Id).SequenceEqual(new[] { 1, 2, 3, 4, 5 })).IsTrue();
    }

    private static List<Item> Seed()
    {
        return new List<Item>
        {
            new() { Id = 1, Category = "A", Value = 10, InStock = true },
            new() { Id = 2, Category = "A", Value = 20, InStock = true },
            new() { Id = 3, Category = "A", Value = 30, InStock = false },
            new() { Id = 4, Category = "B", Value = 40, InStock = true },
            new() { Id = 5, Category = "B", Value = 50, InStock = true }
        };
    }

    private sealed class Holder
    {
        public int Count { get; set; }
    }

    private sealed class Item
    {
        public int Id { get; set; }
        public string Category { get; set; } = "";
        public int Value { get; set; }
        public bool InStock { get; set; }
    }
}
