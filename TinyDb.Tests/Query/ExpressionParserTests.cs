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

    [Test]
    public async Task Parse_BinaryExpressions_ShouldSucceed()
    {
        // Add
        var query = _parser.Parse<QueryDoc>(x => x.Id + 1 > 10);
        await Assert.That(query).IsNotNull();
        var bin = (TinyDb.Query.BinaryExpression)query;
        await Assert.That(bin.NodeType).IsEqualTo(ExpressionType.GreaterThan);
        var left = (TinyDb.Query.BinaryExpression)bin.Left;
        await Assert.That(left.NodeType).IsEqualTo(ExpressionType.Add);

        // Subtract
        query = _parser.Parse<QueryDoc>(x => x.Id - 1 > 10);
        left = (TinyDb.Query.BinaryExpression)((TinyDb.Query.BinaryExpression)query).Left;
        await Assert.That(left.NodeType).IsEqualTo(ExpressionType.Subtract);

        // Multiply
        query = _parser.Parse<QueryDoc>(x => x.Id * 2 > 10);
        left = (TinyDb.Query.BinaryExpression)((TinyDb.Query.BinaryExpression)query).Left;
        await Assert.That(left.NodeType).IsEqualTo(ExpressionType.Multiply);

        // Divide
        query = _parser.Parse<QueryDoc>(x => x.Id / 2 > 10);
        left = (TinyDb.Query.BinaryExpression)((TinyDb.Query.BinaryExpression)query).Left;
        await Assert.That(left.NodeType).IsEqualTo(ExpressionType.Divide);
    }

    [Test]
    public async Task Parse_UnaryExpressions_ShouldSucceed()
    {
        // Not
        var query = _parser.Parse<QueryDoc>(x => !x.Active);
        await Assert.That(query).IsTypeOf<TinyDb.Query.UnaryExpression>();
        var unary = (TinyDb.Query.UnaryExpression)query;
        await Assert.That(unary.NodeType).IsEqualTo(ExpressionType.Not);

        // Negate
        query = _parser.Parse<QueryDoc>(x => -x.Id < 0);
        var bin = (TinyDb.Query.BinaryExpression)query;
        var left = (TinyDb.Query.BinaryExpression)bin.Left;
        // In the parser, Negate is converted to (0 - operand)
        await Assert.That(left.NodeType).IsEqualTo(ExpressionType.Subtract);
        await Assert.That(((TinyDb.Query.ConstantExpression)left.Left).Value).IsEqualTo(0);

        // Convert (implicit cast int to long for comparison if needed, or explicit)
        // x => (double)x.Id > 10.5
        query = _parser.Parse<QueryDoc>(x => (double)x.Id > 10.5);
        bin = (TinyDb.Query.BinaryExpression)query;
        await Assert.That(bin.Left).IsTypeOf<TinyDb.Query.UnaryExpression>();
        unary = (TinyDb.Query.UnaryExpression)bin.Left;
        await Assert.That(unary.NodeType).IsEqualTo(ExpressionType.Convert);
    }
    
    // MemberExpressions tests were correct (MemberName)

    [Test]
    public async Task Parse_MethodCalls_ShouldSucceed()
    {
        // Convert.ToInt32
        var query = _parser.Parse<QueryDoc>(x => Convert.ToInt32(x.Id) == 10);
        var bin = (TinyDb.Query.BinaryExpression)query;
        await Assert.That(bin.Left).IsTypeOf<TinyDb.Query.UnaryExpression>();
        await Assert.That(((TinyDb.Query.UnaryExpression)bin.Left).NodeType).IsEqualTo(ExpressionType.Convert);

        // Equals
        query = _parser.Parse<QueryDoc>(x => x.Name.Equals("John"));
        bin = (TinyDb.Query.BinaryExpression)query;
        await Assert.That(bin.NodeType).IsEqualTo(ExpressionType.Equal);

        // ToString (Constant)
        int val = 123;
        query = _parser.Parse<QueryDoc>(x => x.Name == val.ToString());
        bin = (TinyDb.Query.BinaryExpression)query;
        await Assert.That(bin.Right).IsTypeOf<TinyDb.Query.ConstantExpression>();
        await Assert.That(((TinyDb.Query.ConstantExpression)bin.Right).Value).IsEqualTo("123");

        // String Methods
        query = _parser.Parse<QueryDoc>(x => x.Name.Contains("Jo"));
        var func = (TinyDb.Query.FunctionExpression)query;
        await Assert.That(func.FunctionName).IsEqualTo("Contains");

        query = _parser.Parse<QueryDoc>(x => x.Name.StartsWith("J"));
        func = (TinyDb.Query.FunctionExpression)query;
        await Assert.That(func.FunctionName).IsEqualTo("StartsWith");

        query = _parser.Parse<QueryDoc>(x => x.Name.EndsWith("n"));
        func = (TinyDb.Query.FunctionExpression)query;
        await Assert.That(func.FunctionName).IsEqualTo("EndsWith");
    }

    [Test]
    public async Task Parse_ObjectInitialization_ShouldSucceed()
    {
        // MemberInitExpression (e.g. Select(x => new QueryDoc { Name = x.Name }))
        Expression<Func<QueryDoc, QueryDoc>> selector = x => new QueryDoc { Name = x.Name, Id = x.Id };
        var query = _parser.ParseExpression(selector.Body);
        
        await Assert.That(query).IsTypeOf<TinyDb.Query.MemberInitQueryExpression>();
        var memberInit = (TinyDb.Query.MemberInitQueryExpression)query;
        await Assert.That(memberInit.Bindings).HasCount(2);
        
        // Constructor Expression using Tuples (assuming one that takes params)
        Expression<Func<QueryDoc, Tuple<int>>> tupleSelector = x => new Tuple<int>(x.Id);
        query = _parser.ParseExpression(tupleSelector.Body);
        await Assert.That(query).IsTypeOf<ConstructorExpression>();
    }
}