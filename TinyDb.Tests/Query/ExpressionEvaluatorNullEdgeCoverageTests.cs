using System;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using BinaryExpression = TinyDb.Query.BinaryExpression;
using ConstantExpression = TinyDb.Query.ConstantExpression;

namespace TinyDb.Tests.Query;

public class ExpressionEvaluatorNullEdgeCoverageTests
{
    private sealed class DummyEntity
    {
    }

    [Test]
    public async Task Evaluate_ConstantExpression_Null_ShouldThrow()
    {
        var entity = new DummyEntity();
        await Assert.That(() => ExpressionEvaluator.Evaluate(new ConstantExpression(null), entity))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task EvaluateBinary_OrElse_AndAlso_ShouldCoverFalseBranches()
    {
        var entity = new DummyEntity();

        var orElseFalse = new BinaryExpression(ExpressionType.OrElse, new ConstantExpression(false), new ConstantExpression(false));
        await Assert.That(ExpressionEvaluator.Evaluate(orElseFalse, entity)).IsFalse();

        var andAlsoFalse = new BinaryExpression(ExpressionType.AndAlso, new ConstantExpression(true), new ConstantExpression(false));
        await Assert.That(ExpressionEvaluator.EvaluateValue(andAlsoFalse, entity)).IsEqualTo(false);

        var orElseLeftNonBool = new BinaryExpression(ExpressionType.OrElse, new ConstantExpression(1), new ConstantExpression(false));
        await Assert.That(ExpressionEvaluator.EvaluateValue(orElseLeftNonBool, entity)).IsEqualTo(false);
    }

    [Test]
    public async Task BuildSelector_DefaultBranch_ShouldHandleNullAndNonNullArgs()
    {
        var method = typeof(ExpressionEvaluator).GetMethod("BuildSelector", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var selectorNull = (Func<object, object>)method!.Invoke(null, new object?[] { new object?[] { null } })!;
        await Assert.That(selectorNull(123)).IsEqualTo(123);

        var selectorConstant = (Func<object, object>)method.Invoke(null, new object?[] { new object?[] { 42 } })!;
        await Assert.That(selectorConstant(123)).IsEqualTo(42);
    }
}

