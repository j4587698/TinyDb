using System;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

public class ExpressionParserEvaluateBranchCoverageTests4
{
    private sealed class Dummy
    {
        public int Value { get; set; }
    }

    [Test]
    public async Task TryEvaluateBinary_WithUnsupportedNodeType_ShouldReturnNull()
    {
        var expr = Expression.Modulo(Expression.Constant(5), Expression.Constant(2));
        var result = ExpressionConstantEvaluator.TryEvaluateBinary(expr);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task EvaluateDivide_WhenDivisorIsZero_ShouldReturnNull()
    {
        var result = ExpressionConstantEvaluator.EvaluateDivide(1, 0);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ParseUnaryExpression_WithUnsupportedUnaryPlus_ShouldThrow()
    {
        var parser = new ExpressionParser();
        var param = Expression.Parameter(typeof(Dummy), "x");
        var member = Expression.Property(param, ExpressionMemberInfo.Property<Dummy, int>(x => x.Value));
        var unaryPlus = Expression.UnaryPlus(member);
        var body = Expression.GreaterThan(unaryPlus, Expression.Constant(0));
        var expr = Expression.Lambda<Func<Dummy, bool>>(body, param);

        await Assert.That(() => parser.Parse(expr))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ParseUnaryExpression_WithUnsupportedTypeAs_ShouldThrow()
    {
        var parser = new ExpressionParser();
        var param = Expression.Parameter(typeof(object), "o");
        var typeAs = Expression.TypeAs(param, typeof(string));

        await Assert.That(() => parser.ParseExpression(typeAs))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ParseUnaryExpression_WithUnsupportedConvertChecked_ShouldThrow()
    {
        var parser = new ExpressionParser();
        var param = Expression.Parameter(typeof(int), "x");
        var convertChecked = Expression.ConvertChecked(param, typeof(long));

        await Assert.That(() => parser.ParseExpression(convertChecked))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ParseUnaryExpression_WithUnsupportedNegateChecked_ShouldThrow()
    {
        var parser = new ExpressionParser();
        var param = Expression.Parameter(typeof(int), "x");
        var negateChecked = Expression.NegateChecked(param);

        await Assert.That(() => parser.ParseExpression(negateChecked))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ParseUnaryExpression_WithNegate_ShouldRewriteToSubtractFromZero()
    {
        var parser = new ExpressionParser();
        var param = Expression.Parameter(typeof(Dummy), "x");
        var member = Expression.Property(param, ExpressionMemberInfo.Property<Dummy, int>(x => x.Value));
        var negate = Expression.Negate(member);

        var parsed = parser.ParseExpression(negate);

        await Assert.That(parsed).IsTypeOf<TinyDb.Query.BinaryExpression>();

        var binary = (TinyDb.Query.BinaryExpression)parsed;
        await Assert.That(binary.NodeType).IsEqualTo(ExpressionType.Subtract);

        await Assert.That(binary.Left).IsTypeOf<TinyDb.Query.ConstantExpression>();
        var leftConstant = (TinyDb.Query.ConstantExpression)binary.Left;
        await Assert.That(leftConstant.Value).IsEqualTo(0);
    }
}
