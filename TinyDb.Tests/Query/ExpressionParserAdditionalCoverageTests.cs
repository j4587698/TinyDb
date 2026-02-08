using System;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using BinaryExpression = TinyDb.Query.BinaryExpression;
using ConstantExpression = TinyDb.Query.ConstantExpression;
using ConstructorExpression = TinyDb.Query.ConstructorExpression;
using FunctionExpression = TinyDb.Query.FunctionExpression;
using UnaryExpression = TinyDb.Query.UnaryExpression;

namespace TinyDb.Tests.Query;

[NotInParallel]
public class ExpressionParserAdditionalCoverageTests
{
    private static class ThrowingHelpers
    {
        public static int ThrowingMethod() => throw new InvalidOperationException("boom");
        public static int ThrowingProperty => throw new InvalidOperationException("boom");
    }

    private sealed class Container
    {
        public int Value { get; set; } = 42;
    }

    private static Container GetContainer() => new();

    private sealed class CtorOk
    {
        public int Value { get; }
        public CtorOk(int value) => Value = value;
    }

    private sealed class CtorThrows
    {
        public CtorThrows() => throw new InvalidOperationException("boom");
    }

    private sealed class CtorWithNullableString
    {
        public string? Value { get; }
        public CtorWithNullableString(string? value) => Value = value;
    }

    private sealed class SampleEntity
    {
        public string Number { get; set; } = "123";
        public string Text { get; set; } = "";
    }

    [Test]
    public async Task ParseExpression_ShouldEvaluateConstantsViaCompile_WhenManualEvalNotSupported()
    {
        var parser = new ExpressionParser();

        var expr = Expression.Call(typeof(Guid), nameof(Guid.NewGuid), Type.EmptyTypes);
        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsed).Value).IsTypeOf<Guid>();
    }

    [Test]
    public async Task ParseExpression_ShouldFallback_WhenCompileEvaluationThrows()
    {
        var parser = new ExpressionParser();

        var expr = Expression.Call(typeof(ThrowingHelpers), nameof(ThrowingHelpers.ThrowingMethod), Type.EmptyTypes);
        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<FunctionExpression>();
        await Assert.That(((FunctionExpression)parsed).FunctionName).IsEqualTo(nameof(ThrowingHelpers.ThrowingMethod));
    }

    [Test]
    public async Task ParseExpression_ShouldEvaluateMemberAccessViaCompile_WhenContainerCannotBeEvaluated()
    {
        var parser = new ExpressionParser();

        MethodInfo getContainer = typeof(ExpressionParserAdditionalCoverageTests)
            .GetMethod(nameof(GetContainer), BindingFlags.NonPublic | BindingFlags.Static)!;

        var valueProp = typeof(Container).GetProperty(nameof(Container.Value))!;
        var expr = Expression.Property(Expression.Call(getContainer), valueProp);
        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<TinyDb.Query.MemberExpression>();
        await Assert.That(((TinyDb.Query.MemberExpression)parsed).MemberName).IsEqualTo(nameof(Container.Value));
    }

    [Test]
    public async Task ParseExpression_ShouldManualEvaluateBinaryArithmetic()
    {
        var parser = new ExpressionParser();

        Expression<Func<int>> sub = () => 10 - 3;
        var parsedSub = parser.ParseExpression(sub.Body);
        await Assert.That(parsedSub).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsedSub).Value).IsEqualTo(7);

        Expression<Func<decimal>> mul = () => 2.5m * 4m;
        var parsedMul = parser.ParseExpression(mul.Body);
        await Assert.That(parsedMul).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsedMul).Value).IsEqualTo(10m);
    }

    [Test]
    public async Task ParseExpression_DivideByZero_ShouldFallbackToBinaryExpression()
    {
        var parser = new ExpressionParser();

        var expr = Expression.Divide(Expression.Constant(10), Expression.Constant(0));
        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<BinaryExpression>();
        await Assert.That(((BinaryExpression)parsed).NodeType).IsEqualTo(ExpressionType.Divide);
    }

    [Test]
    public async Task ParseExpression_Convert_WithInvalidValue_ShouldFallbackToUnaryExpression()
    {
        var parser = new ExpressionParser();

        // Use object->int unbox conversion so the Expression can be built, but evaluation fails at runtime.
        var expr = Expression.Convert(Expression.Constant("not-an-int", typeof(object)), typeof(int));
        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<UnaryExpression>();
        await Assert.That(((UnaryExpression)parsed).NodeType).IsEqualTo(ExpressionType.Convert);
    }

    [Test]
    public async Task ParseExpression_NewExpression_ValueTypeWithoutCtor_ShouldReturnDefault()
    {
        var parser = new ExpressionParser();

        var expr = Expression.New(typeof(int));
        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsed).Value).IsEqualTo(0);
    }

    [Test]
    public async Task ParseExpression_NewExpression_ThrowingCtor_ShouldFallbackToConstructorExpression()
    {
        var parser = new ExpressionParser();

        var ctor = typeof(CtorThrows).GetConstructor(Type.EmptyTypes)!;
        var expr = Expression.New(ctor);
        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<ConstructorExpression>();
        await Assert.That(((ConstructorExpression)parsed).Type).IsEqualTo(typeof(CtorThrows));
    }

    [Test]
    public async Task ParseExpression_Conditional_WithUnsupportedTest_ShouldEvaluateViaCompile()
    {
        var parser = new ExpressionParser();

        var isNullOrEmpty = typeof(string).GetMethod(nameof(string.IsNullOrEmpty), new[] { typeof(string) })!;
        var test = Expression.Call(isNullOrEmpty, Expression.Constant("x"));
        var expr = Expression.Condition(test, Expression.Constant(1), Expression.Constant(2));

        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsed).Value).IsEqualTo(2);
    }

    [Test]
    public async Task ParseExpression_StaticMember_ThatThrows_ShouldThrowNotSupported()
    {
        var parser = new ExpressionParser();

        var prop = typeof(ThrowingHelpers).GetProperty(nameof(ThrowingHelpers.ThrowingProperty), BindingFlags.Public | BindingFlags.Static)!;
        var expr = Expression.Property(null, prop);

        await Assert.That(() => parser.ParseExpression(expr)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task ParseExpression_MethodCall_SpecialCases_ShouldWork()
    {
        var parser = new ExpressionParser();

        var param = Expression.Parameter(typeof(SampleEntity), "x");

        // Convert.ToInt32(x.Number)
        var toInt32 = typeof(Convert).GetMethod(nameof(Convert.ToInt32), new[] { typeof(string) })!;
        var numberProp = typeof(SampleEntity).GetProperty(nameof(SampleEntity.Number))!;
        var convertCall = Expression.Call(toInt32, Expression.Property(param, numberProp));
        var parsedConvert = parser.ParseExpression(convertCall);
        await Assert.That(parsedConvert).IsTypeOf<UnaryExpression>();
        await Assert.That(((UnaryExpression)parsedConvert).NodeType).IsEqualTo(ExpressionType.Convert);

        // x.Text.Equals("a")
        Expression<Func<SampleEntity, bool>> equalsExpr = x => x.Text.Equals("a");
        var parsedEquals = parser.ParseExpression(equalsExpr.Body);
        await Assert.That(parsedEquals).IsTypeOf<BinaryExpression>();
        await Assert.That(((BinaryExpression)parsedEquals).NodeType).IsEqualTo(ExpressionType.Equal);
    }

    [Test]
    public async Task ParseExpression_WhenDynamicCodeNotSupported_AndManualEvalFails_ShouldThrowNotSupported()
    {
        var parser = new ExpressionParser();
        var expr = Expression.Block(Expression.Constant(1));
        await Assert.That(() => parser.ParseExpression(expr)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task ParseExpression_ShouldManualEvaluatePropertyMember()
    {
        var parser = new ExpressionParser();

        var dt = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var prop = typeof(DateTime).GetProperty(nameof(DateTime.Year))!;
        var expr = Expression.Property(Expression.Constant(dt), prop);

        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsed).Value).IsEqualTo(dt.Year);
    }

    [Test]
    public async Task ParseExpression_ShouldManualEvaluateDivide_WhenNonZero()
    {
        var parser = new ExpressionParser();

        Expression<Func<int>> div = () => 10 / 2;
        var parsed = parser.ParseExpression(div.Body);

        await Assert.That(parsed).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsed).Value).IsEqualTo(5);
    }

    [Test]
    public async Task ParseExpression_NewExpression_WithNullConstantArgument_ShouldManualEvaluate()
    {
        var parser = new ExpressionParser();

        var ctor = typeof(CtorWithNullableString).GetConstructor(new[] { typeof(string) })!;
        var expr = Expression.New(ctor, Expression.Constant(null, typeof(string)));

        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<ConstantExpression>();
        await Assert.That(((CtorWithNullableString)((ConstantExpression)parsed).Value!).Value).IsNull();
    }

    [Test]
    public async Task ParseExpression_NewExpression_WhenArgumentCannotBeEvaluated_AndDynamicCodeNotSupported_ShouldReturnConstructorExpression()
    {
        var parser = new ExpressionParser();
        var ctor = typeof(CtorOk).GetConstructor(new[] { typeof(int) })!;
        var arg = Expression.Call(typeof(ThrowingHelpers), nameof(ThrowingHelpers.ThrowingMethod), Type.EmptyTypes);
        var expr = Expression.New(ctor, arg);

        var parsed = parser.ParseExpression(expr);

        await Assert.That(parsed).IsTypeOf<ConstructorExpression>();
        await Assert.That(((ConstructorExpression)parsed).Type).IsEqualTo(typeof(CtorOk));
    }

    [Test]
    public async Task ParseExpression_UnaryOperations_ShouldParse()
    {
        var parser = new ExpressionParser();

        Expression<Func<bool, bool>> notExpr = x => !x;
        var parsedNot = parser.ParseExpression(notExpr.Body);
        await Assert.That(parsedNot).IsTypeOf<UnaryExpression>();
        await Assert.That(((UnaryExpression)parsedNot).NodeType).IsEqualTo(ExpressionType.Not);

        Expression<Func<int, int>> negExpr = x => -x;
        var parsedNeg = parser.ParseExpression(negExpr.Body);
        await Assert.That(parsedNeg).IsTypeOf<BinaryExpression>();
        await Assert.That(((BinaryExpression)parsedNeg).NodeType).IsEqualTo(ExpressionType.Subtract);

        Expression<Func<int, long>> convertExpr = x => (long)x;
        var parsedConvert = parser.ParseExpression(convertExpr.Body);
        await Assert.That(parsedConvert).IsTypeOf<UnaryExpression>();
        await Assert.That(((UnaryExpression)parsedConvert).NodeType).IsEqualTo(ExpressionType.Convert);

        Expression<Func<int[], int>> arrayLenExpr = x => x.Length;
        var parsedLen = parser.ParseExpression(arrayLenExpr.Body);
        await Assert.That(parsedLen).IsTypeOf<UnaryExpression>();
        await Assert.That(((UnaryExpression)parsedLen).NodeType).IsEqualTo(ExpressionType.ArrayLength);
    }
}
