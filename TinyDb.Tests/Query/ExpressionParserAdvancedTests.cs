using System;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using BinaryExpression = TinyDb.Query.BinaryExpression;
using ConstantExpression = TinyDb.Query.ConstantExpression;

namespace TinyDb.Tests.Query;

public class ExpressionParserAdvancedTests
{
    private readonly ExpressionParser _parser = new();

    [Test]
    public async Task Parse_Not_ShouldWork()
    {
        Expression<Func<User, bool>> expr = u => u.Age != 20;
        var queryExpr = _parser.Parse(expr);
        
        await Assert.That(queryExpr).IsTypeOf<BinaryExpression>();
        var binary = (BinaryExpression)queryExpr;
        await Assert.That(binary.NodeType).IsEqualTo(ExpressionType.NotEqual);
    }

    [Test]
    public async Task Parse_Negate_ShouldWork()
    {
        Expression<Func<User, bool>> expr = u => -u.Age == -20;
        var queryExpr = _parser.Parse(expr);
        
        await Assert.That(queryExpr).IsTypeOf<TinyDb.Query.BinaryExpression>();
    }

    [Test]
    public async Task Parse_StringEquals_ShouldWork()
    {
        Expression<Func<User, bool>> expr = u => u.Name.Equals("test");
        var queryExpr = _parser.Parse(expr);
        
        await Assert.That(queryExpr).IsTypeOf<TinyDb.Query.BinaryExpression>();
        var binary = (TinyDb.Query.BinaryExpression)queryExpr;
        await Assert.That(binary.NodeType).IsEqualTo(ExpressionType.Equal);
    }

    [Test]
    public async Task Parse_ToString_ShouldWork()
    {
        Expression<Func<User, bool>> expr = u => u.Age.ToString() == "20";
        var queryExpr = _parser.Parse(expr);
        
        await Assert.That(queryExpr).IsTypeOf<TinyDb.Query.BinaryExpression>();
    }

    [Test]
    public async Task Parse_StaticMember_ShouldWork()
    {
        Expression<Func<User, bool>> expr = u => u.Age == int.MaxValue;
        var queryExpr = _parser.Parse(expr);
        
        await Assert.That(queryExpr).IsTypeOf<TinyDb.Query.BinaryExpression>();
        var binary = (TinyDb.Query.BinaryExpression)queryExpr;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        var constant = (ConstantExpression)binary.Right;
        await Assert.That(constant.Value).IsEqualTo(int.MaxValue);
    }
}
