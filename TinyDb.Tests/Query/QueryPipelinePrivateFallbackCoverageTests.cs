using System.Collections;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryPipelinePrivateFallbackCoverageTests
{
    public static int Median(IEnumerable<int> items) => 0;

    [Test]
    public async Task ExecuteOrderBy_WhenThenByOnUnorderedSource_ShouldUseFallbackOrderBy()
    {
        var source = new[] { 3, 1, 2 };

        Expression<Func<int, int>> keySelector = x => x;

        var thenByExpr = Expression.Call(
            typeof(Queryable),
            nameof(Queryable.ThenBy),
            new[] { typeof(int), typeof(int) },
            Expression.Constant(null, typeof(IOrderedQueryable<int>)),
            Expression.Quote(keySelector));

        var executeOrderBy = typeof(QueryPipeline).GetMethod(
            "ExecuteOrderBy",
            BindingFlags.NonPublic | BindingFlags.Static,
            binder: null,
            types: new[] { typeof(IEnumerable), typeof(MethodCallExpression) },
            modifiers: null);

        await Assert.That(executeOrderBy == null).IsFalse();

        var result = (IEnumerable)executeOrderBy!.Invoke(null, new object?[] { source, thenByExpr })!;
        var sorted = result.Cast<object>().Select(x => (int)x!).ToList();
        await Assert.That(sorted.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task ExecuteAggregation_WhenUnsupportedName_ShouldThrow()
    {
        var source = new[] { 1, 2, 3 };
        var expr = Expression.Call(
            typeof(QueryPipelinePrivateFallbackCoverageTests),
            nameof(Median),
            Type.EmptyTypes,
            Expression.Constant(source, typeof(IEnumerable<int>)));

        var executeAggregation = typeof(QueryPipeline).GetMethod(
            "ExecuteAggregation",
            BindingFlags.NonPublic | BindingFlags.Static,
            binder: null,
            types: new[] { typeof(IEnumerable), typeof(MethodCallExpression) },
            modifiers: null);

        await Assert.That(executeAggregation == null).IsFalse();

        await Assert.That(() => executeAggregation!.Invoke(null, new object?[] { source, expr }))
            .Throws<TargetInvocationException>();
    }
}
