using System.Collections;
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

        var thenByMethod =
            ((MethodCallExpression)((Expression<Func<IOrderedQueryable<int>, Expression<Func<int, int>>, IOrderedQueryable<int>>>)
                ((q, key) => Queryable.ThenBy(q, key))).Body).Method;
        var thenByExpr = Expression.Call(
            thenByMethod,
            Expression.Constant(null, typeof(IOrderedQueryable<int>)),
            Expression.Quote(keySelector));

        var result = QueryPipelineOrdering.Execute(source, thenByExpr);
        var sorted = result.Cast<object>().Select(x => (int)x!).ToList();
        await Assert.That(sorted.SequenceEqual(new[] { 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task ExecuteAggregation_WhenUnsupportedName_ShouldThrow()
    {
        var source = new[] { 1, 2, 3 };
        var medianMethod =
            ((MethodCallExpression)((Expression<Func<IEnumerable<int>, int>>)(items => Median(items))).Body).Method;
        var expr = Expression.Call(medianMethod, Expression.Constant(source, typeof(IEnumerable<int>)));

        await Assert.That(() => QueryPipelineAggregation.Execute(source, expr))
            .Throws<NotSupportedException>();
    }
}
