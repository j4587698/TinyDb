using System;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using QueryBinaryExpression = TinyDb.Query.BinaryExpression;
using QueryConstantExpression = TinyDb.Query.ConstantExpression;
using QueryUnaryExpression = TinyDb.Query.UnaryExpression;

namespace TinyDb.Tests.Query;

public class ExpressionParserConstantEvaluationAdditionalCoverageTests
{
    private static class ThrowingStatics
    {
        // ReSharper disable once UnusedMember.Local
        public static int Boom => throw new InvalidOperationException("boom");
    }

    public static long AddIntLong(int a, long b) => a + b;
    public static long AddLongInt(long a, int b) => a + b;
    public static long SubIntLong(int a, long b) => a - b;
    public static long SubLongInt(long a, int b) => a - b;
    public static long MulLongInt(long a, int b) => a * b;
    public static long MulIntLong(int a, long b) => a * b;
    public static long DivIntLong(int a, long b) => a / b;
    public static long DivLongInt(long a, int b) => a / b;

    [Test]
    public async Task ParseExpression_NoParameter_MethodCall_UsesCompile()
    {
        var expr = (Expression<Func<string>>)(() => Guid.Empty.ToString());
        var parser = new ExpressionParser();

        var parsed = parser.ParseExpression(expr.Body);

        await Assert.That(parsed).IsTypeOf<QueryConstantExpression>();
        await Assert.That(((QueryConstantExpression)parsed).Value).IsEqualTo(Guid.Empty.ToString());
    }

    [Test]
    public async Task ParseExpression_WhenConstantEvaluationThrows_FallsBackToNormalParsing()
    {
        var max = decimal.MaxValue;
        var expr = (Expression<Func<decimal>>)(() => max + max);
        var parser = new ExpressionParser();

        var parsed = parser.ParseExpression(expr.Body);

        await Assert.That(parsed).IsTypeOf<QueryBinaryExpression>();
        await Assert.That(((QueryBinaryExpression)parsed).NodeType).IsEqualTo(ExpressionType.Add);
    }

    [Test]
    public async Task ParseExpression_TryEvaluateNew_ReturnsNull_WhenArgumentCannotBeEvaluated()
    {
        var expr = (Expression<Func<Tuple<int, Guid>>>)(() => new Tuple<int, Guid>(1, Guid.NewGuid()));
        var parser = new ExpressionParser();

        var parsed = parser.ParseExpression(expr.Body);

        await Assert.That(parsed).IsTypeOf<QueryConstantExpression>();
        await Assert.That(((QueryConstantExpression)parsed).Value).IsTypeOf<Tuple<int, Guid>>();
    }

    [Test]
    public async Task ParseExpression_TryEvaluateBinary_ArithmeticMatrix_ShouldProduceConstants()
    {
        var parser = new ExpressionParser();

        var addIntInt = Expression.Add(Expression.Constant(1), Expression.Constant(2));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(addIntInt)).Value).IsEqualTo(3);

        var addLongLong = Expression.Add(Expression.Constant(1L), Expression.Constant(2L));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(addLongLong)).Value).IsEqualTo(3L);

        var addDoubleDouble = Expression.Add(Expression.Constant(1d), Expression.Constant(2d));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(addDoubleDouble)).Value).IsEqualTo(3d);

        var addDecimalDecimal = Expression.Add(Expression.Constant(1m), Expression.Constant(2m));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(addDecimalDecimal)).Value).IsEqualTo(3m);

        var addIntLong = Expression.Add(
            Expression.Constant(1),
            Expression.Constant(2L),
            typeof(ExpressionParserConstantEvaluationAdditionalCoverageTests).GetMethod(nameof(AddIntLong))!);
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(addIntLong)).Value).IsEqualTo(3L);

        var addLongInt = Expression.Add(
            Expression.Constant(1L),
            Expression.Constant(2),
            typeof(ExpressionParserConstantEvaluationAdditionalCoverageTests).GetMethod(nameof(AddLongInt))!);
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(addLongInt)).Value).IsEqualTo(3L);

        var subIntLong = Expression.Subtract(
            Expression.Constant(10),
            Expression.Constant(3L),
            typeof(ExpressionParserConstantEvaluationAdditionalCoverageTests).GetMethod(nameof(SubIntLong))!);
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(subIntLong)).Value).IsEqualTo(7L);

        var mulLongInt = Expression.Multiply(
            Expression.Constant(6L),
            Expression.Constant(7),
            typeof(ExpressionParserConstantEvaluationAdditionalCoverageTests).GetMethod(nameof(MulLongInt))!);
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(mulLongInt)).Value).IsEqualTo(42L);

        var divIntLong = Expression.Divide(
            Expression.Constant(10),
            Expression.Constant(2L),
            typeof(ExpressionParserConstantEvaluationAdditionalCoverageTests).GetMethod(nameof(DivIntLong))!);
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(divIntLong)).Value).IsEqualTo(5L);
    }

    [Test]
    public async Task ParseExpression_TryEvaluateBinary_SubtractMultiplyDivide_Matrix_ShouldProduceConstants_And_ZeroDivision_ShouldFallback()
    {
        var parser = new ExpressionParser();

        var subIntInt = Expression.Subtract(Expression.Constant(5), Expression.Constant(2));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(subIntInt)).Value).IsEqualTo(3);

        var subLongLong = Expression.Subtract(Expression.Constant(5L), Expression.Constant(2L));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(subLongLong)).Value).IsEqualTo(3L);

        var subDoubleDouble = Expression.Subtract(Expression.Constant(5d), Expression.Constant(2d));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(subDoubleDouble)).Value).IsEqualTo(3d);

        var subDecimalDecimal = Expression.Subtract(Expression.Constant(5m), Expression.Constant(2m));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(subDecimalDecimal)).Value).IsEqualTo(3m);

        var subLongInt = Expression.Subtract(
            Expression.Constant(10L),
            Expression.Constant(3),
            typeof(ExpressionParserConstantEvaluationAdditionalCoverageTests).GetMethod(nameof(SubLongInt))!);
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(subLongInt)).Value).IsEqualTo(7L);

        var mulIntInt = Expression.Multiply(Expression.Constant(6), Expression.Constant(7));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(mulIntInt)).Value).IsEqualTo(42);

        var mulLongLong = Expression.Multiply(Expression.Constant(6L), Expression.Constant(7L));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(mulLongLong)).Value).IsEqualTo(42L);

        var mulDoubleDouble = Expression.Multiply(Expression.Constant(6d), Expression.Constant(7d));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(mulDoubleDouble)).Value).IsEqualTo(42d);

        var mulDecimalDecimal = Expression.Multiply(Expression.Constant(6m), Expression.Constant(7m));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(mulDecimalDecimal)).Value).IsEqualTo(42m);

        var mulIntLong = Expression.Multiply(
            Expression.Constant(6),
            Expression.Constant(7L),
            typeof(ExpressionParserConstantEvaluationAdditionalCoverageTests).GetMethod(nameof(MulIntLong))!);
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(mulIntLong)).Value).IsEqualTo(42L);

        var divIntInt = Expression.Divide(Expression.Constant(10), Expression.Constant(2));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(divIntInt)).Value).IsEqualTo(5);

        var divLongLong = Expression.Divide(Expression.Constant(10L), Expression.Constant(2L));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(divLongLong)).Value).IsEqualTo(5L);

        var divDoubleDouble = Expression.Divide(Expression.Constant(5d), Expression.Constant(2d));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(divDoubleDouble)).Value).IsEqualTo(2.5d);

        var divDecimalDecimal = Expression.Divide(Expression.Constant(5m), Expression.Constant(2m));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(divDecimalDecimal)).Value).IsEqualTo(2.5m);

        var divLongInt = Expression.Divide(
            Expression.Constant(10L),
            Expression.Constant(2),
            typeof(ExpressionParserConstantEvaluationAdditionalCoverageTests).GetMethod(nameof(DivLongInt))!);
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(divLongInt)).Value).IsEqualTo(5L);

        var divByZero = Expression.Divide(Expression.Constant(1), Expression.Constant(0));
        var parsed = parser.ParseExpression(divByZero);
        await Assert.That(parsed).IsTypeOf<QueryBinaryExpression>();
        await Assert.That(((QueryBinaryExpression)parsed).NodeType).IsEqualTo(ExpressionType.Divide);
    }

    [Test]
    public async Task ParseExpression_TryEvaluateConvert_And_ArrayLength_ShouldWork()
    {
        var parser = new ExpressionParser();

        var convert = Expression.Convert(Expression.Constant(123), typeof(double));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(convert)).Value).IsEqualTo(123d);

        var length = Expression.ArrayLength(Expression.Constant(new[] { 1, 2, 3 }));
        await Assert.That(((QueryConstantExpression)parser.ParseExpression(length)).Value).IsEqualTo(3);
    }

    private sealed class Entity
    {
        // ReSharper disable once UnusedAutoPropertyAccessor.Local
        public string Name { get; set; } = "";
    }

    [Test]
    public async Task ParseExpression_ToString_OnCapturedValue_ShouldFoldToConstant()
    {
        var captured = 123;
        var expr = (Expression<Func<Entity, bool>>)(e => e.Name == captured.ToString());

        var parser = new ExpressionParser();
        var parsed = (QueryBinaryExpression)parser.ParseExpression(expr.Body);

        await Assert.That(parsed.Right).IsTypeOf<QueryConstantExpression>();
        await Assert.That(((QueryConstantExpression)parsed.Right).Value).IsEqualTo("123");
    }

    [Test]
    public async Task ParseExpression_ArrayLength_NullArray_ShouldFallbackToUnaryExpression()
    {
        var parser = new ExpressionParser();

        var length = Expression.ArrayLength(Expression.Constant(null, typeof(int[])));
        var parsed = parser.ParseExpression(length);

        await Assert.That(parsed).IsTypeOf<QueryUnaryExpression>();
        await Assert.That(((QueryUnaryExpression)parsed).NodeType).IsEqualTo(ExpressionType.ArrayLength);
    }

    [Test]
    public async Task ParseExpression_StaticMember_WhenEvaluationFails_ShouldThrowNotSupported()
    {
        var member = Expression.Property(null, ExpressionMemberInfo.Property(() => ThrowingStatics.Boom));
        var parser = new ExpressionParser();

        await Assert.That(() => parser.ParseExpression(member))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task TryEvaluateBinary_WhenNodeTypeUnsupported_ShouldReturnNull()
    {
        var method = typeof(ExpressionParser).GetMethod(
            "TryEvaluateBinary",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var modulo = Expression.Modulo(Expression.Constant(10), Expression.Constant(3));
        var result = method!.Invoke(null, new object[] { modulo });

        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task TryEvaluateBinary_ShouldCover_AllSupportedArithmeticNodeTypes()
    {
        var method = typeof(ExpressionParser).GetMethod(
            "TryEvaluateBinary",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        object? Invoke(System.Linq.Expressions.BinaryExpression expr) => method!.Invoke(null, new object[] { expr });

        await Assert.That(Invoke(Expression.Add(Expression.Constant(1), Expression.Constant(2)))).IsEqualTo(3);
        await Assert.That(Invoke(Expression.Subtract(Expression.Constant(10), Expression.Constant(3)))).IsEqualTo(7);
        await Assert.That(Invoke(Expression.Multiply(Expression.Constant(6), Expression.Constant(7)))).IsEqualTo(42);
        await Assert.That(Invoke(Expression.Divide(Expression.Constant(10), Expression.Constant(2)))).IsEqualTo(5);
        await Assert.That(Invoke(Expression.Modulo(Expression.Constant(10), Expression.Constant(3)))).IsNull();
        await Assert.That(Invoke(Expression.SubtractChecked(Expression.Constant(10), Expression.Constant(3)))).IsNull();
        await Assert.That(Invoke(Expression.MultiplyChecked(Expression.Constant(6), Expression.Constant(7)))).IsNull();

        var unevaluable = Expression.Add(Expression.Parameter(typeof(int), "x"), Expression.Constant(1));
        await Assert.That(Invoke(unevaluable)).IsNull();

        var unevaluableRight = Expression.Add(Expression.Constant(1), Expression.Parameter(typeof(int), "x"));
        await Assert.That(Invoke(unevaluableRight)).IsNull();
    }
}
