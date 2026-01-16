using System.Linq.Expressions;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class ExpressionParserTests
{
    private readonly ExpressionParser _parser = new();

    public class QueryDoc
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool Active { get; set; }
    }

    [Test]
    public async Task Parse_Simple_Equals_Should_Work()
    {
        Expression<Func<QueryDoc, bool>> expr = x => x.Id == 1;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result.NodeType).IsEqualTo(ExpressionType.Equal);
    }

    [Test]
    public async Task Parse_And_Expression_Should_Work()
    {
        Expression<Func<QueryDoc, bool>> expr = x => x.Id > 0 && x.Active;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result.NodeType).IsEqualTo(ExpressionType.AndAlso);
    }

    [Test]
    public async Task Parse_Or_Expression_Should_Work()
    {
        Expression<Func<QueryDoc, bool>> expr = x => x.Id == 1 || x.Name == "test";
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result.NodeType).IsEqualTo(ExpressionType.OrElse);
    }

    [Test]
    public async Task Parse_Not_Equals_Should_Work()
    {
        Expression<Func<QueryDoc, bool>> expr = x => x.Id != 1;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result.NodeType).IsEqualTo(ExpressionType.NotEqual);
    }

    [Test]
    public async Task Parse_Parameter_Expression_Should_Work()
    {
        Expression<Func<QueryDoc, bool>> expr = x => x != null;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Nested_Member_Should_Throw_Or_Work()
    {
        // Current implementation uses member.Member.Name which is the property name.
        // Let's see how it handles x.Active.
        Expression<Func<QueryDoc, bool>> expr = x => x.Active;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result.NodeType).IsEqualTo(ExpressionType.MemberAccess);
    }

    [Test]
    public async Task Parse_Boolean_Constant_Should_Work()
    {
        Expression<Func<QueryDoc, bool>> expr = x => true;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result.NodeType).IsEqualTo(ExpressionType.Constant);
    }
}