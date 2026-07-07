using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

[SkipInAot]
public sealed class QueryShapeExtractorEdgeCoverageTests
{
    [Test]
    public async Task Extract_OrderByAndThenByInvalidSort_ShouldStopAndClearSortFields()
    {
        var q = TestQueryables.InMemory(Array.Empty<Row>());

        var invalidOrderExpr = q.OrderBy(x => x.A + 1).Expression;
        var invalidOrderResult = QueryShapeExtractor.Extract<Row>(invalidOrderExpr);
        await Assert.That(invalidOrderResult.Shape.HasTypeShapingOperator).IsTrue();
        await Assert.That(invalidOrderResult.Shape.Sort.Count).IsEqualTo(0);

        var invalidThenByExpr = q.OrderBy(x => x.A).ThenBy(x => x.A + 1).Expression;
        var invalidThenByResult = QueryShapeExtractor.Extract<Row>(invalidThenByExpr);
        await Assert.That(invalidThenByResult.Shape.HasTypeShapingOperator).IsTrue();
        await Assert.That(invalidThenByResult.Shape.Sort.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Extract_OrderByWithConvert_ShouldUnwrapUnaryConvert()
    {
        var q = TestQueryables.InMemory(Array.Empty<Row>());
        var expr = q.OrderBy(x => (object)x.A).Expression;

        var result = QueryShapeExtractor.Extract<Row>(expr);
        await Assert.That(result.Shape.Sort.Count).IsEqualTo(1);
        await Assert.That(result.Shape.Sort[0].FieldName).IsEqualTo("a");
    }

    [Test]
    public async Task TryGetLambdaArgument_ShouldCoverLambdaAndInvalidArgumentBranches()
    {
        var lambda = (Expression<Func<Row, bool>>)(x => x.A > 0);
        var directLambdaCall = (MethodCallExpression)((Expression<Func<IEnumerable<Row>, IEnumerable<Row>>>)
            (source => source.Where(x => x.A > 0))).Body;

        var ok1 = QueryExpressionCallHelpers.TryGetLambdaArgument(directLambdaCall, out var lambda1);
        await Assert.That(ok1).IsTrue();
        await Assert.That(lambda1).IsNotNull();

        var sourceExpr = TestQueryables.InMemory(Array.Empty<Row>()).Expression;
        var fakeQueryableWhere = ((MethodCallExpression)((Expression<Func<IQueryable<Row>, Expression<Func<Row, bool>>, IQueryable<Row>>>)
            ((source, predicate) => FakeQueryableWhere(source, predicate))).Body).Method;
        var invalidArg = Expression.Constant(lambda, typeof(Expression<Func<Row, bool>>));
        var invalidCall = Expression.Call(fakeQueryableWhere, sourceExpr, invalidArg);
        var ok2 = QueryExpressionCallHelpers.TryGetLambdaArgument(invalidCall, out var lambda2);
        await Assert.That(ok2).IsFalse();
        await Assert.That(lambda2).IsNull();
    }

    [Test]
    public async Task ReplaceParameter_WhenParameterDoesNotMatch_ShouldReturnOriginalExpression()
    {
        var from = Expression.Parameter(typeof(Row), "from");
        var to = Expression.Parameter(typeof(Row), "to");
        var other = Expression.Parameter(typeof(Row), "other");

        var visited = QueryExpressionCallHelpers.ReplaceParameter(other, from, to);

        await Assert.That(object.ReferenceEquals(visited, other)).IsTrue();
    }

    private static IQueryable<Row> FakeQueryableWhere(IQueryable<Row> source, Expression<Func<Row, bool>> predicate) =>
        throw new NotSupportedException();

    public sealed class Row
    {
        public int A { get; set; }
    }
}
