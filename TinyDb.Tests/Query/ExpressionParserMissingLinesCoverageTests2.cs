using System;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using ConstantExpression = TinyDb.Query.ConstantExpression;

namespace TinyDb.Tests.Query;

public sealed class ExpressionParserMissingLinesCoverageTests2
{
    private sealed class Holder
    {
        public int Field = 123;
        public int Property { get; } = 456;
    }

    private static class NullProvider
    {
        public static readonly Holder? NullHolder = null;
    }

    private sealed class StringCtor
    {
        public StringCtor(string value) => Value = value;
        public string Value { get; }
    }

    private readonly struct NonConvertible
    {
        public NonConvertible(int value) => Value = value;
        public int Value { get; }
        public static implicit operator int(NonConvertible value) => value.Value;
    }

    private static readonly PropertyInfo HolderProperty = ExpressionMemberInfo.Property<Holder, int>(h => h.Property);
    private static readonly FieldInfo NullHolderField = ExpressionMemberInfo.Field(() => NullProvider.NullHolder);

    [Test]
    public async Task ParseMemberExpression_WhenContainerConstant_ShouldEvaluateFieldAndProperty()
    {
        var parser = new ExpressionParser();

        int captured = 123;
        Expression<Func<int>> fieldExpr = () => captured;
        var parsedField = parser.ParseExpression(fieldExpr.Body);
        await Assert.That(parsedField).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsedField).Value).IsEqualTo(123);

        var holder = new Holder();
        var propExpr = Expression.Property(Expression.Constant(holder), HolderProperty);
        var parsedProp = parser.ParseExpression(propExpr);
        await Assert.That(parsedProp).IsNotTypeOf<ConstantExpression>();
    }

    [Test]
    public async Task ParseMethodCallExpression_ToString_OnConstant_ShouldBeConstantFolded()
    {
        var parser = new ExpressionParser();

        var toString = typeof(int).GetMethod(nameof(int.ToString), Type.EmptyTypes)!;
        var callExpr = Expression.Call(Expression.Constant(123), toString);
        var parsed = parser.ParseExpression(callExpr);

        await Assert.That(parsed).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsed).Value).IsEqualTo("123");
    }

    [Test]
    public async Task ParseExpression_WhenParentEvaluatesToNull_ShouldReturnNonConstantExpression()
    {
        var parser = new ExpressionParser();

        var inner = Expression.Field(null, NullHolderField);
        var outer = Expression.Property(inner, HolderProperty);

        var parsed = parser.ParseExpression(outer);
        await Assert.That(parsed).IsNotNull();
        await Assert.That(parsed).IsNotTypeOf<ConstantExpression>();
    }

    [Test]
    public async Task ParseExpression_WhenMemberTargetIsConstantNull_ShouldNotThrow()
    {
        var parser = new ExpressionParser();

        var invalidMemberAccess = Expression.Property(Expression.Constant(null, typeof(Holder)), HolderProperty);
        var parsed = parser.ParseExpression(invalidMemberAccess);

        await Assert.That(parsed).IsNotNull();
    }

    [Test]
    public async Task ParseExpression_WhenBinaryConstant_ShouldCoverMultiplyAndDivide()
    {
        var parser = new ExpressionParser();

        var multiply = Expression.Multiply(Expression.Constant(2), Expression.Constant(3));
        var parsedMultiply = parser.ParseExpression(multiply);
        await Assert.That(parsedMultiply).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsedMultiply).Value).IsEqualTo(6);

        var divideByZero = Expression.Divide(Expression.Constant(10), Expression.Constant(0));
        var parsedDivideByZero = parser.ParseExpression(divideByZero);
        await Assert.That(parsedDivideByZero).IsNotNull();
    }

    [Test]
    public async Task TryEvaluateNew_WithNullArgumentAndConditional_ShouldCoverBranches()
    {
        var parser = new ExpressionParser();
        var ctor = typeof(StringCtor).GetConstructor(new[] { typeof(string) })!;
        var padLeft = typeof(string).GetMethod(nameof(string.PadLeft), new[] { typeof(int) })!;
        var newExpr = Expression.New(ctor, Expression.Call(Expression.Constant("x"), padLeft, Expression.Constant(2)));
        var parsedNew = parser.ParseExpression(newExpr);
        await Assert.That(parsedNew).IsNotNull();
        await Assert.That(parsedNew).IsNotTypeOf<ConstantExpression>();

        var conditionalTrue = Expression.Condition(Expression.Constant(true), Expression.Constant(1), Expression.Constant(2));
        var conditionalFalse = Expression.Condition(Expression.Constant(false), Expression.Constant(1), Expression.Constant(2));

        var parsedTrue = parser.ParseExpression(conditionalTrue);
        var parsedFalse = parser.ParseExpression(conditionalFalse);
        await Assert.That(((ConstantExpression)parsedTrue).Value).IsEqualTo(1);
        await Assert.That(((ConstantExpression)parsedFalse).Value).IsEqualTo(2);
    }

    [Test]
    public async Task ParseExpression_Unary_ArrayLength_ShouldReturnIntExpression()
    {
        var parser = new ExpressionParser();
        var array = Expression.Constant(new[] { 1, 2, 3 });
        var expr = Expression.ArrayLength(array);

        var parsed = parser.ParseExpression(expr);
        await Assert.That(parsed).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)parsed).Value).IsEqualTo(3);
    }

    [Test]
    public async Task TryEvaluateConvert_WhenOperandNotEvaluatable_ShouldReturnNull()
    {
        var parser = new ExpressionParser();
        Expression<Func<NonConvertible, int>> toInt = v => v;
        var conversionMethod = ((System.Linq.Expressions.UnaryExpression)toInt.Body).Method;
        await Assert.That(conversionMethod).IsNotNull();

        var convert = Expression.Convert(Expression.Constant(new NonConvertible(123)), typeof(int), conversionMethod);
        var result = parser.ParseExpression(convert);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsNotTypeOf<ConstantExpression>();
    }
}
