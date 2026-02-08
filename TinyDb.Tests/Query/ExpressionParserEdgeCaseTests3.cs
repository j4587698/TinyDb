using System;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using ConstantExpression = TinyDb.Query.ConstantExpression;
using BinaryExpression = TinyDb.Query.BinaryExpression;
using UnaryExpression = TinyDb.Query.UnaryExpression;
using MemberExpression = TinyDb.Query.MemberExpression;
using ParameterExpression = TinyDb.Query.ParameterExpression;

namespace TinyDb.Tests.Query;

/// <summary>
/// Edge case tests for ExpressionParser to improve coverage
/// Focuses on: static members, ToCamelCase coverage, Convert.To methods, unsupported expressions
/// </summary>
public class ExpressionParserEdgeCaseTests3
{
    private readonly ExpressionParser _parser = new();

    #region Static Member Access Tests

    [Test]
    public async Task ParseExpression_StaticPropertyAccess_ShouldReturnConstant()
    {
        // DateTime.Now is a static property
        Expression<Func<TestEntity, bool>> expr = x => x.CreatedAt < DateTime.Now;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
    }

    [Test]
    public async Task ParseExpression_StaticFieldAccess_ShouldReturnConstant()
    {
        // string.Empty is a static field
        Expression<Func<TestEntity, bool>> expr = x => x.Name != string.Empty;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
    }

    [Test]
    public async Task ParseExpression_DateTimeUtcNow_ShouldReturnConstant()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.CreatedAt < DateTime.UtcNow;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task ParseExpression_EnvironmentNewLine_ShouldReturnConstant()
    {
        // Environment.NewLine is a static property
        Expression<Func<TestEntity, bool>> expr = x => x.Name.Contains(Environment.NewLine);
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
    }

    #endregion

    #region Closure and Local Variable Tests

    [Test]
    public async Task ParseExpression_ClosureVariable_ShouldEvaluateToConstant()
    {
        var localValue = 42;
        Expression<Func<TestEntity, bool>> expr = x => x.Age == localValue;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
        
        var binary = (BinaryExpression)result;
        // The right side should be a constant with value 42
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        var constant = (ConstantExpression)binary.Right;
        await Assert.That(constant.Value).IsEqualTo(42);
    }

    [Test]
    public async Task ParseExpression_NestedClosureVariable_ShouldEvaluateToConstant()
    {
        var obj = new { Value = 100 };
        Expression<Func<TestEntity, bool>> expr = x => x.Age == obj.Value;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
    }

    [Test]
    public async Task ParseExpression_ClosureStringVariable_ShouldEvaluateToConstant()
    {
        var searchName = "TestName";
        Expression<Func<TestEntity, bool>> expr = x => x.Name == searchName;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        var constant = (ConstantExpression)binary.Right;
        await Assert.That(constant.Value).IsEqualTo("TestName");
    }

    #endregion

    #region Convert.To Method Tests

    [Test]
    public async Task ParseExpression_ConvertToInt32_ShouldReturnConvertExpression()
    {
        Expression<Func<TestEntity, bool>> expr = x => Convert.ToInt32(x.DoubleValue) > 10;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
        
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Left).IsTypeOf<UnaryExpression>();
    }

    [Test]
    public async Task ParseExpression_ConvertToDouble_ShouldReturnConvertExpression()
    {
        Expression<Func<TestEntity, bool>> expr = x => Convert.ToDouble(x.Age) > 10.5;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task ParseExpression_ConvertToString_ShouldReturnConvertExpression()
    {
        Expression<Func<TestEntity, bool>> expr = x => Convert.ToString(x.Age) == "25";
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
    }

    #endregion

    #region ToString Method Tests

    [Test]
    public async Task ParseExpression_ToStringOnConstant_ShouldEvaluateImmediately()
    {
        var value = 42;
        Expression<Func<TestEntity, bool>> expr = x => x.Name == value.ToString();
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        // The right side should be "42" as a string constant
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        var constant = (ConstantExpression)binary.Right;
        await Assert.That(constant.Value).IsEqualTo("42");
    }

    [Test]
    public async Task ParseExpression_ToStringOnMember_ShouldReturnFunctionExpression()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.Age.ToString() == "25";
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        // Left side should be a FunctionExpression for ToString
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Left).IsTypeOf<FunctionExpression>();
    }

    #endregion

    #region Equals Method Tests

    [Test]
    public async Task ParseExpression_EqualsMethod_ShouldConvertToBinaryEqual()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.Name.Equals("Test");
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
        
        var binary = (BinaryExpression)result;
        await Assert.That(binary.NodeType).IsEqualTo(ExpressionType.Equal);
    }

    [Test]
    public async Task ParseExpression_ObjectEqualsMethod_ShouldConvertToBinaryEqual()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.Age.Equals(25);
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
    }

    #endregion

    #region Arithmetic Expression Tests

    [Test]
    public async Task ParseExpression_AddOperation_ShouldReturnBinaryAdd()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.Age + 5 > 30;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
        
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Left).IsTypeOf<BinaryExpression>();
        var addExpr = (BinaryExpression)binary.Left;
        await Assert.That(addExpr.NodeType).IsEqualTo(ExpressionType.Add);
    }

    [Test]
    public async Task ParseExpression_SubtractOperation_ShouldReturnBinarySubtract()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.Age - 5 > 20;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        var subExpr = (BinaryExpression)binary.Left;
        await Assert.That(subExpr.NodeType).IsEqualTo(ExpressionType.Subtract);
    }

    [Test]
    public async Task ParseExpression_MultiplyOperation_ShouldReturnBinaryMultiply()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.Age * 2 > 50;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        var mulExpr = (BinaryExpression)binary.Left;
        await Assert.That(mulExpr.NodeType).IsEqualTo(ExpressionType.Multiply);
    }

    [Test]
    public async Task ParseExpression_DivideOperation_ShouldReturnBinaryDivide()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.Age / 2 > 10;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        var divExpr = (BinaryExpression)binary.Left;
        await Assert.That(divExpr.NodeType).IsEqualTo(ExpressionType.Divide);
    }

    #endregion

    #region Negate Expression Tests

    [Test]
    public async Task ParseExpression_NegateOperation_ShouldReturnSubtractFromZero()
    {
        Expression<Func<TestEntity, bool>> expr = x => -x.Age < 0;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
        
        var binary = (BinaryExpression)result;
        // Negate is converted to (0 - x)
        await Assert.That(binary.Left).IsTypeOf<BinaryExpression>();
        var negExpr = (BinaryExpression)binary.Left;
        await Assert.That(negExpr.NodeType).IsEqualTo(ExpressionType.Subtract);
    }

    #endregion

    #region Not Expression Tests

    [Test]
    public async Task ParseExpression_NotOperation_ShouldReturnUnaryNot()
    {
        Expression<Func<TestEntity, bool>> expr = x => !x.IsActive;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<UnaryExpression>();
        
        var unary = (UnaryExpression)result;
        await Assert.That(unary.NodeType).IsEqualTo(ExpressionType.Not);
    }

    [Test]
    public async Task ParseExpression_DoubleNot_ShouldReturnNestedUnary()
    {
        Expression<Func<TestEntity, bool>> expr = x => !!x.IsActive;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<UnaryExpression>();
        
        var outer = (UnaryExpression)result;
        await Assert.That(outer.Operand).IsTypeOf<UnaryExpression>();
    }

    #endregion

    #region Null Expression Tests

    [Test]
    public async Task Parse_WithNullExpression_ShouldReturnNull()
    {
        var result = _parser.Parse<TestEntity>(null!);
        
        await Assert.That(result).IsNull();
    }

    #endregion

    #region Parameter Expression Tests

    [Test]
    public async Task ParseExpression_ParameterOnly_ShouldReturnParameter()
    {
        // This is an edge case - just the parameter itself
        var param = System.Linq.Expressions.Expression.Parameter(typeof(TestEntity), "x");
        
        var result = _parser.ParseExpression(param);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<ParameterExpression>();
        var paramExpr = (ParameterExpression)result;
        await Assert.That(paramExpr.Name).IsEqualTo("x");
    }

    #endregion

    #region Complex Constant Evaluation Tests

    [Test]
    public async Task ParseExpression_MathOperation_ShouldEvaluateToConstant()
    {
        // Math.Max should be evaluated as constant since it doesn't depend on parameter
        var max = Math.Max(10, 20);
        Expression<Func<TestEntity, bool>> expr = x => x.Age > max;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        var constant = (ConstantExpression)binary.Right;
        await Assert.That(constant.Value).IsEqualTo(20);
    }

    [Test]
    public async Task ParseExpression_ArrayLength_ShouldEvaluateToConstant()
    {
        var arr = new int[] { 1, 2, 3, 4, 5 };
        Expression<Func<TestEntity, bool>> expr = x => x.Age > arr.Length;
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        var constant = (ConstantExpression)binary.Right;
        await Assert.That(constant.Value).IsEqualTo(5);
    }

    #endregion

    #region Unsupported Expression Tests

    [Test]
    public async Task ParseExpression_UnsupportedBinaryOp_ShouldThrow()
    {
        // Create an unsupported binary expression that contains a parameter
        // (expressions without parameters get evaluated as constants and won't throw)
        var param = System.Linq.Expressions.Expression.Parameter(typeof(int?), "x");
        var binary = System.Linq.Expressions.Expression.MakeBinary(
            ExpressionType.Coalesce,
            param,  // Use parameter so expression can't be pre-evaluated
            System.Linq.Expressions.Expression.Constant(5));
        
        await Assert.That(() => _parser.ParseExpression(binary))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ParseExpression_UnsupportedUnaryOp_ShouldThrow()
    {
        // Create an unsupported unary expression (e.g., TypeAs)
        var param = System.Linq.Expressions.Expression.Parameter(typeof(object), "x");
        var unary = System.Linq.Expressions.Expression.TypeAs(param, typeof(string));
        
        await Assert.That(() => _parser.ParseExpression(unary))
            .Throws<NotSupportedException>();
    }

    #endregion

    #region MemberInit Expression Tests

    [Test]
    public async Task ParseExpression_MemberInitWithMultipleBindings_ShouldParseMemberBindings()
    {
        Expression<Func<TestEntity, ResultEntity>> expr = x => new ResultEntity
        {
            Name = x.Name,
            Value = x.Age
        };
        
        var result = _parser.ParseExpression(expr.Body);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<MemberInitQueryExpression>();
        
        var memberInit = (MemberInitQueryExpression)result;
        await Assert.That(memberInit.Bindings.Count).IsEqualTo(2);
    }

    #endregion

    #region Generic Method Call Tests

    [Test]
    public async Task ParseExpression_ContainsMethod_ShouldReturnFunctionExpression()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.Name.Contains("test");
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<FunctionExpression>();
        
        var func = (FunctionExpression)result;
        await Assert.That(func.FunctionName).IsEqualTo("Contains");
    }

    [Test]
    public async Task ParseExpression_StartsWithMethod_ShouldReturnFunctionExpression()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.Name.StartsWith("test");
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<FunctionExpression>();
        
        var func = (FunctionExpression)result;
        await Assert.That(func.FunctionName).IsEqualTo("StartsWith");
    }

    [Test]
    public async Task ParseExpression_EndsWithMethod_ShouldReturnFunctionExpression()
    {
        Expression<Func<TestEntity, bool>> expr = x => x.Name.EndsWith("test");
        
        var result = _parser.Parse(expr);
        
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<FunctionExpression>();
        
        var func = (FunctionExpression)result;
        await Assert.That(func.FunctionName).IsEqualTo("EndsWith");
    }

    #endregion

    #region Test Entities

    private class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public double DoubleValue { get; set; }
        public DateTime CreatedAt { get; set; }
        public bool IsActive { get; set; }
    }

    private class ResultEntity
    {
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    #endregion
}
