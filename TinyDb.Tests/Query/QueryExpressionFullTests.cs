using TinyDb.Query;
using System.Linq.Expressions;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExpressionFullTests
{
    [Test]
    public async Task ConstantExpression_Properties_Should_Work()
    {
        var expr = new TinyDb.Query.ConstantExpression(42);
        await Assert.That(expr.Value).IsEqualTo(42);
        await Assert.That(expr.NodeType).IsEqualTo(ExpressionType.Constant);
    }

    [Test]
    public async Task ParameterExpression_Properties_Should_Work()
    {
        var expr = new TinyDb.Query.ParameterExpression("x");
        await Assert.That(expr.Name).IsEqualTo("x");
        await Assert.That(expr.NodeType).IsEqualTo(ExpressionType.Parameter);
    }

    [Test]
    public async Task MemberExpression_Properties_Should_Work()
    {
        var expr = new TinyDb.Query.MemberExpression("Name", new TinyDb.Query.ParameterExpression("x"));
        await Assert.That(expr.MemberName).IsEqualTo("Name");
        await Assert.That(expr.Expression).IsNotNull();
        await Assert.That(expr.NodeType).IsEqualTo(ExpressionType.MemberAccess);
    }

    [Test]
    public async Task BinaryExpression_Properties_Should_Work()
    {
        var left = new TinyDb.Query.ConstantExpression(1);
        var right = new TinyDb.Query.ConstantExpression(2);
        var expr = new TinyDb.Query.BinaryExpression(ExpressionType.Equal, left, right);
        
        await Assert.That(expr.NodeType).IsEqualTo(ExpressionType.Equal);
        await Assert.That(expr.Left).IsSameReferenceAs(left);
        await Assert.That(expr.Right).IsSameReferenceAs(right);
    }

    [Test]
    public async Task FunctionExpression_Properties_Should_Work()
    {
        var target = new TinyDb.Query.MemberExpression("Text");
        var arg = new TinyDb.Query.ConstantExpression("val");
        var expr = new TinyDb.Query.FunctionExpression("Contains", target, arg);
        
        await Assert.That(expr.FunctionName).IsEqualTo("Contains");
        await Assert.That(expr.Target).IsSameReferenceAs(target);
        await Assert.That(expr.Arguments.Count).IsEqualTo(1);
        await Assert.That(expr.Arguments[0]).IsSameReferenceAs(arg);
        await Assert.That(expr.NodeType).IsEqualTo(ExpressionType.Call);
    }
}
