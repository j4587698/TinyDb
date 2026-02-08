using System;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

/// <summary>
/// Tests for ExpressionParser covering ConstructorExpression and MemberInitExpression parsing
/// </summary>
public class ExpressionParserProjectionTests
{
    private readonly ExpressionParser _parser = new();

    #region ConstructorExpression Tests

    [Test]
    public async Task Parse_NewExpression_Simple_Should_Create_ConstructorExpression()
    {
        Expression<Func<TestEntity, SimpleDto>> expr = e => new SimpleDto(e.Name);

        var result = _parser.ParseExpression(expr.Body);

        await Assert.That(result).IsTypeOf<ConstructorExpression>();
        var ctorExpr = (ConstructorExpression)result;
        await Assert.That(ctorExpr.NodeType).IsEqualTo(ExpressionType.New);
        await Assert.That(ctorExpr.Type).IsEqualTo(typeof(SimpleDto));
        await Assert.That(ctorExpr.Arguments.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Parse_NewExpression_MultipleArgs_Should_Create_ConstructorExpression()
    {
        Expression<Func<TestEntity, MultiArgDto>> expr = e => new MultiArgDto(e.Name, e.Value);

        var result = _parser.ParseExpression(expr.Body);

        await Assert.That(result).IsTypeOf<ConstructorExpression>();
        var ctorExpr = (ConstructorExpression)result;
        await Assert.That(ctorExpr.Arguments.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Parse_NewExpression_NoArgs_Should_Create_ConstructorExpression()
    {
        Expression<Func<TestEntity, EmptyDto>> expr = e => new EmptyDto();

        var result = _parser.ParseExpression(expr.Body);

        // For empty constructor with no arguments, ExpressionParser might optimize to ConstantExpression
        // or return ConstructorExpression depending on implementation
        await Assert.That(result is TinyDb.Query.ConstructorExpression || result is TinyDb.Query.ConstantExpression).IsTrue();
    }

    #endregion

    #region MemberInitExpression Tests

    [Test]
    public async Task Parse_MemberInitExpression_Should_Create_MemberInitQueryExpression()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { DisplayName = e.Name, Amount = e.Value };

        var result = _parser.ParseExpression(expr.Body);

        await Assert.That(result).IsTypeOf<MemberInitQueryExpression>();
        var memberInit = (MemberInitQueryExpression)result;
        await Assert.That(memberInit.NodeType).IsEqualTo(ExpressionType.MemberInit);
        await Assert.That(memberInit.Type).IsEqualTo(typeof(ProjectedDto));
        await Assert.That(memberInit.Bindings.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Parse_MemberInitExpression_Bindings_Should_Have_Correct_Names()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { DisplayName = e.Name, Amount = e.Value };

        var result = _parser.ParseExpression(expr.Body);
        var memberInit = (MemberInitQueryExpression)result;

        var bindingNames = memberInit.Bindings.Select(b => b.MemberName).ToList();
        await Assert.That(bindingNames).Contains("DisplayName");
        await Assert.That(bindingNames).Contains("Amount");
    }

    [Test]
    public async Task Parse_MemberInitExpression_SingleBinding_Should_Work()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { DisplayName = e.Name };

        var result = _parser.ParseExpression(expr.Body);

        await Assert.That(result).IsTypeOf<MemberInitQueryExpression>();
        var memberInit = (MemberInitQueryExpression)result;
        await Assert.That(memberInit.Bindings.Count).IsEqualTo(1);
        await Assert.That(memberInit.Bindings[0].MemberName).IsEqualTo("DisplayName");
    }

    [Test]
    public async Task Parse_MemberInitExpression_WithArithmetic_Should_Work()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { Amount = e.Value * 2 };

        var result = _parser.ParseExpression(expr.Body);

        await Assert.That(result).IsTypeOf<MemberInitQueryExpression>();
        var memberInit = (MemberInitQueryExpression)result;
        await Assert.That(memberInit.Bindings[0].Value).IsTypeOf<TinyDb.Query.BinaryExpression>();
    }

    [Test]
    public async Task Parse_MemberInitExpression_WithStringConcat_Should_Work()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { DisplayName = e.Name + " - " + e.Category };

        var result = _parser.ParseExpression(expr.Body);

        await Assert.That(result).IsTypeOf<MemberInitQueryExpression>();
    }

    #endregion

    #region Edge Cases

    [Test]
    public async Task Parse_MemberInitExpression_EmptyBindings_Should_Work()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { };

        var result = _parser.ParseExpression(expr.Body);

        // Empty initializer might be parsed as New or MemberInit depending on compiler
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_NestedMemberInit_Should_Work()
    {
        Expression<Func<TestEntity, NestedDto>> expr = e => new NestedDto
        {
            Inner = new ProjectedDto { DisplayName = e.Name }
        };

        var result = _parser.ParseExpression(expr.Body);

        await Assert.That(result).IsTypeOf<MemberInitQueryExpression>();
        var memberInit = (MemberInitQueryExpression)result;
        await Assert.That(memberInit.Bindings[0].MemberName).IsEqualTo("Inner");
        await Assert.That(memberInit.Bindings[0].Value).IsTypeOf<MemberInitQueryExpression>();
    }

    #endregion

    #region Test Classes

    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public string Category { get; set; } = "";
        public decimal Value { get; set; }
    }

    public class SimpleDto
    {
        public string Name { get; }
        public SimpleDto(string name) => Name = name;
    }

    public class MultiArgDto
    {
        public string Name { get; }
        public decimal Value { get; }
        public MultiArgDto(string name, decimal value)
        {
            Name = name;
            Value = value;
        }
    }

    public class EmptyDto
    {
    }

    public class ProjectedDto
    {
        public string DisplayName { get; set; } = "";
        public decimal Amount { get; set; }
    }

    public class NestedDto
    {
        public ProjectedDto Inner { get; set; } = new();
    }

    #endregion
}
