using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
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
        IQueryable<Row> q = new List<Row>().AsQueryable();

        var invalidOrderExpr = q.OrderBy(x => x.A + 1).Expression;
        var invalidOrderResult = InvokeExtract<Row>(invalidOrderExpr);
        await Assert.That(invalidOrderResult.Shape.HasTypeShapingOperator).IsTrue();
        await Assert.That(invalidOrderResult.Shape.Sort.Count).IsEqualTo(0);

        var invalidThenByExpr = q.OrderBy(x => x.A).ThenBy(x => x.A + 1).Expression;
        var invalidThenByResult = InvokeExtract<Row>(invalidThenByExpr);
        await Assert.That(invalidThenByResult.Shape.HasTypeShapingOperator).IsTrue();
        await Assert.That(invalidThenByResult.Shape.Sort.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Extract_OrderByWithConvert_ShouldUnwrapUnaryConvert()
    {
        IQueryable<Row> q = new List<Row>().AsQueryable();
        var expr = q.OrderBy(x => (object)x.A).Expression;

        var result = InvokeExtract<Row>(expr);
        await Assert.That(result.Shape.Sort.Count).IsEqualTo(1);
        await Assert.That(result.Shape.Sort[0].FieldName).IsEqualTo("a");
    }

    [Test]
    public async Task TryGetUnaryQuotedLambda_ShouldCoverLambdaAndInvalidArgumentBranches()
    {
        var method = typeof(QueryShapeExtractor).GetMethod("TryGetUnaryQuotedLambda", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var sourceExpr = Expression.Constant(new List<Row>().AsQueryable());

        var enumerableWhere = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m =>
            {
                if (m.Name != "Where") return false;
                var p = m.GetParameters();
                return p.Length == 2 && p[1].ParameterType.IsGenericType && p[1].ParameterType.GetGenericTypeDefinition() == typeof(Func<,>);
            })
            .MakeGenericMethod(typeof(Row));

        var lambda = (Expression<Func<Row, bool>>)(x => x.A > 0);

        var directLambdaCall = Expression.Call(
            enumerableWhere,
            Expression.Constant(new List<Row>()),
            lambda);

        var args1 = new object?[] { directLambdaCall, null };
        var ok1 = (bool)method!.Invoke(null, args1)!;
        await Assert.That(ok1).IsTrue();
        await Assert.That(args1[1]).IsNotNull();

        var queryableWhere = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m =>
            {
                if (m.Name != "Where") return false;
                var p = m.GetParameters();
                if (p.Length != 2 || !p[1].ParameterType.IsGenericType || p[1].ParameterType.GetGenericTypeDefinition() != typeof(Expression<>))
                {
                    return false;
                }

                var inner = p[1].ParameterType.GetGenericArguments()[0];
                return inner.IsGenericType && inner.GetGenericTypeDefinition() == typeof(Func<,>);
            })
            .MakeGenericMethod(typeof(Row));

        var invalidArg = Expression.Constant(lambda, typeof(Expression<Func<Row, bool>>));
        var invalidCall = Expression.Call(queryableWhere, sourceExpr, invalidArg);
        var args2 = new object?[] { invalidCall, null };
        var ok2 = (bool)method.Invoke(null, args2)!;
        await Assert.That(ok2).IsFalse();
        await Assert.That(args2[1]).IsNull();
    }

    [Test]
    public async Task ParameterReplaceVisitor_WhenParameterDoesNotMatch_ShouldReturnBaseVisitResult()
    {
        var visitorType = typeof(QueryShapeExtractor).GetNestedType("ParameterReplaceVisitor", BindingFlags.NonPublic)
            ?? throw new MissingMemberException(typeof(QueryShapeExtractor).FullName, "ParameterReplaceVisitor");

        var from = Expression.Parameter(typeof(Row), "from");
        var to = Expression.Parameter(typeof(Row), "to");
        var other = Expression.Parameter(typeof(Row), "other");

        var visitor = Activator.CreateInstance(
            visitorType,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            args: new object[] { from, to },
            culture: null);

        await Assert.That(visitor).IsNotNull();

        var visit = typeof(ExpressionVisitor).GetMethod(nameof(ExpressionVisitor.Visit), new[] { typeof(Expression) })
            ?? throw new MissingMethodException(typeof(ExpressionVisitor).FullName, nameof(ExpressionVisitor.Visit));
        var visited = (Expression)visit.Invoke(visitor, new object[] { other })!;

        await Assert.That(object.ReferenceEquals(visited, other)).IsTrue();
    }

    private static (QueryShape<T> Shape, System.Linq.Expressions.ConstantExpression? Source) InvokeExtract<T>(Expression expression)
        where T : class, new()
    {
        var method = typeof(QueryShapeExtractor).GetMethod("Extract", BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(typeof(T));
        return ((QueryShape<T> Shape, System.Linq.Expressions.ConstantExpression? Source))method.Invoke(null, new object[] { expression })!;
    }

    public sealed class Row
    {
        public int A { get; set; }
    }
}
