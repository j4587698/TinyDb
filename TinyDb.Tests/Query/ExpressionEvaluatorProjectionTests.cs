using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using TinyDb.Bson;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

/// <summary>
/// Tests for ExpressionEvaluator covering ConstructorExpression and MemberInitExpression evaluation
/// </summary>
public class ExpressionEvaluatorProjectionTests
{
    private readonly ExpressionParser _parser = new();

    #region ConstructorExpression Evaluation Tests

    [Test]
    public async Task Evaluate_ConstructorExpression_Simple_Should_Create_Instance()
    {
        Expression<Func<TestEntity, SimpleDto>> expr = e => new SimpleDto(e.Name);
        var queryExpr = _parser.ParseExpression(expr.Body);

        var entity = new TestEntity { Name = "Test", Value = 100 };
        var result = ExpressionEvaluator.EvaluateValue(queryExpr, entity);

        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<SimpleDto>();
        await Assert.That(((SimpleDto)result!).Name).IsEqualTo("Test");
    }

    [Test]
    public async Task Evaluate_ConstructorExpression_MultipleArgs_Should_Work()
    {
        Expression<Func<TestEntity, MultiArgDto>> expr = e => new MultiArgDto(e.Name, e.Value);
        var queryExpr = _parser.ParseExpression(expr.Body);

        var entity = new TestEntity { Name = "Item", Value = 42.5m };
        var result = ExpressionEvaluator.EvaluateValue(queryExpr, entity);

        await Assert.That(result).IsTypeOf<MultiArgDto>();
        var dto = (MultiArgDto)result!;
        await Assert.That(dto.Name).IsEqualTo("Item");
        await Assert.That(dto.Value).IsEqualTo(42.5m);
    }

    [Test]
    public async Task Evaluate_ConstructorExpression_WithArithmetic_Should_Work()
    {
        Expression<Func<TestEntity, MultiArgDto>> expr = e => new MultiArgDto(e.Name, e.Value * 2);
        var queryExpr = _parser.ParseExpression(expr.Body);

        var entity = new TestEntity { Name = "Double", Value = 25m };
        var result = ExpressionEvaluator.EvaluateValue(queryExpr, entity);

        await Assert.That(result).IsTypeOf<MultiArgDto>();
        await Assert.That(((MultiArgDto)result!).Value).IsEqualTo(50m);
    }

    #endregion

    #region MemberInitExpression Evaluation Tests

    [Test]
    public async Task Evaluate_MemberInitExpression_Should_Create_And_Initialize()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { DisplayName = e.Name, Amount = e.Value };
        var queryExpr = _parser.ParseExpression(expr.Body);

        var entity = new TestEntity { Name = "Product", Value = 99.99m };
        var result = ExpressionEvaluator.EvaluateValue(queryExpr, entity);

        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<ProjectedDto>();
        var dto = (ProjectedDto)result!;
        await Assert.That(dto.DisplayName).IsEqualTo("Product");
        await Assert.That(dto.Amount).IsEqualTo(99.99m);
    }

    [Test]
    public async Task Evaluate_MemberInitExpression_WithConcat_Should_Work()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { DisplayName = e.Category + ": " + e.Name };
        var queryExpr = _parser.ParseExpression(expr.Body);

        var entity = new TestEntity { Name = "Laptop", Category = "Electronics" };
        var result = ExpressionEvaluator.EvaluateValue(queryExpr, entity);

        await Assert.That(((ProjectedDto)result!).DisplayName).IsEqualTo("Electronics: Laptop");
    }

    [Test]
    public async Task Evaluate_MemberInitExpression_WithTypeConversion_Should_Work()
    {
        Expression<Func<TestEntity, IntDto>> expr = e => new IntDto { IntValue = (int)e.Value };
        var queryExpr = _parser.ParseExpression(expr.Body);

        var entity = new TestEntity { Value = 42.9m };
        var result = ExpressionEvaluator.EvaluateValue(queryExpr, entity);

        await Assert.That(result).IsTypeOf<IntDto>();
        // Note: decimal to int conversion truncates
        var intVal = ((IntDto)result!).IntValue;
        await Assert.That(intVal == 42 || intVal == 43).IsTrue();
    }

    [Test]
    public async Task Evaluate_MemberInitExpression_PartialInit_Should_Work()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { DisplayName = e.Name };
        var queryExpr = _parser.ParseExpression(expr.Body);

        var entity = new TestEntity { Name = "Partial" };
        var result = ExpressionEvaluator.EvaluateValue(queryExpr, entity);

        await Assert.That(((ProjectedDto)result!).DisplayName).IsEqualTo("Partial");
        await Assert.That(((ProjectedDto)result!).Amount).IsEqualTo(0m); // Default value
    }

    #endregion

    #region BsonDocument Evaluation Tests

    [Test]
    public async Task Evaluate_MemberInit_WithBsonDocument_Should_Work()
    {
        var queryExpr = new MemberInitQueryExpression(
            typeof(ProjectedDto),
            new List<(string, QueryExpression)>
            {
                ("DisplayName", new TinyDb.Query.MemberExpression("name")),
                ("Amount", new TinyDb.Query.MemberExpression("value"))
            }
        );

        var doc = new BsonDocument()
            .Set("name", "FromBson")
            .Set("value", 123.45m);

        var result = ExpressionEvaluator.EvaluateValue(queryExpr, doc);

        await Assert.That(result).IsTypeOf<ProjectedDto>();
        var dto = (ProjectedDto)result!;
        await Assert.That(dto.DisplayName).IsEqualTo("FromBson");
        await Assert.That(dto.Amount).IsEqualTo(123.45m);
    }

    [Test]
    public async Task Evaluate_ConstructorExpression_Direct_Should_Work()
    {
        var ctorExpr = new TinyDb.Query.ConstructorExpression(
            typeof(SimpleDto),
            new List<QueryExpression> { new TinyDb.Query.ConstantExpression("DirectTest") }
        );

        var entity = new TestEntity();
        var result = ExpressionEvaluator.EvaluateValue(ctorExpr, entity);

        await Assert.That(result).IsTypeOf<SimpleDto>();
        await Assert.That(((SimpleDto)result!).Name).IsEqualTo("DirectTest");
    }

    #endregion

    #region Edge Cases

    [Test]
    public async Task Evaluate_MemberInit_NullPropertyValue_Should_Work()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { DisplayName = null! };
        var queryExpr = _parser.ParseExpression(expr.Body);

        var entity = new TestEntity();
        var result = ExpressionEvaluator.EvaluateValue(queryExpr, entity);

        await Assert.That(((ProjectedDto)result!).DisplayName).IsNull();
    }

    [Test]
    public async Task Evaluate_MemberInit_WithNestedAccess_Should_Work()
    {
        Expression<Func<TestEntity, ProjectedDto>> expr = e => new ProjectedDto { DisplayName = e.Name.ToUpper() };
        var queryExpr = _parser.ParseExpression(expr.Body);

        var entity = new TestEntity { Name = "test" };
        var result = ExpressionEvaluator.EvaluateValue(queryExpr, entity);

        await Assert.That(((ProjectedDto)result!).DisplayName).IsEqualTo("TEST");
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

    public class ProjectedDto
    {
        public string DisplayName { get; set; } = "";
        public decimal Amount { get; set; }
    }

    public class IntDto
    {
        public int IntValue { get; set; }
    }

    #endregion
}
