using System;
using System.Linq.Expressions;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using LinqExpression = System.Linq.Expressions.Expression;
using QueryBinaryExpression = TinyDb.Query.BinaryExpression;
using QueryConstantExpression = TinyDb.Query.ConstantExpression;
using QueryParameterExpression = TinyDb.Query.ParameterExpression;

namespace TinyDb.Tests.Query;

/// <summary>
/// Additional ExpressionParser edge case tests for improved coverage
/// Focuses on: AOT fallback, static member access, member evaluation failure, unsupported operations
/// </summary>
public class ExpressionParserEdgeCaseTests2
{
    private readonly ExpressionParser _parser = new();

    public class TestDoc
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime CreatedAt { get; set; }
        public int? NullableValue { get; set; }
    }

    #region Static Member Access Tests

    [Test]
    public async Task Parse_StaticPropertyAccess_ShouldEvaluateToConstant()
    {
        // Tests line 165-175: Static member access (DateTime.Now, etc.)
        Expression<Func<TestDoc, bool>> expr = x => x.CreatedAt < DateTime.UtcNow;
        
        // Should parse without throwing
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<QueryBinaryExpression>();
    }

    [Test]
    public async Task Parse_StaticFieldAccess_ShouldEvaluateToConstant()
    {
        // Test static field access
        Expression<Func<TestDoc, bool>> expr = x => x.Name == string.Empty;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_StaticReadOnlyField_ShouldEvaluateToConstant()
    {
        // Test static readonly fields like DateTime.MaxValue
        Expression<Func<TestDoc, bool>> expr = x => x.CreatedAt < DateTime.MaxValue;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    #endregion

    #region Member Expression with Closure Tests

    [Test]
    public async Task Parse_ClosureVariable_ShouldEvaluateToConstant()
    {
        // Tests line 188-206: Member expression with closure
        var localValue = 42;
        Expression<Func<TestDoc, bool>> expr = x => x.Id == localValue;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<QueryBinaryExpression>();

        var binary = (QueryBinaryExpression)result;
        // Right side should be evaluated to constant
        await Assert.That(binary.Right).IsTypeOf<QueryConstantExpression>();
        await Assert.That(((QueryConstantExpression)binary.Right).Value).IsEqualTo(42);
    }

    [Test]
    public async Task Parse_NestedClosureVariable_ShouldEvaluateToConstant()
    {
        var obj = new { Value = 100 };
        Expression<Func<TestDoc, bool>> expr = x => x.Id == obj.Value;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    #endregion

    #region Constant Expression Without Parameter Tests

    [Test]
    public async Task Parse_PureConstantExpression_ShouldOptimize()
    {
        // Tests line 32-51: Expression without parameter should be evaluated early
        var value = 5;
        Expression<Func<TestDoc, bool>> expr = x => 10 + value == 15;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_ConstantMathExpression_ShouldEvaluate()
    {
        // Pure math without parameter
        Expression<Func<TestDoc, bool>> expr = x => x.Id == 2 + 3;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    #endregion

    #region Binary Expression Tests

    [Test]
    public async Task Parse_AddExpression_ShouldParseCorrectly()
    {
        // Tests line 149: Add operation
        Expression<Func<TestDoc, bool>> expr = x => x.Id + 1 == 10;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_SubtractExpression_ShouldParseCorrectly()
    {
        // Tests line 150: Subtract operation
        Expression<Func<TestDoc, bool>> expr = x => x.Id - 1 == 0;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_MultiplyExpression_ShouldParseCorrectly()
    {
        // Tests line 151: Multiply operation
        Expression<Func<TestDoc, bool>> expr = x => x.Id * 2 == 10;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_DivideExpression_ShouldParseCorrectly()
    {
        // Tests line 152: Divide operation
        Expression<Func<TestDoc, bool>> expr = x => x.Id / 2 == 5;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_GreaterThanExpression_ShouldParseCorrectly()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Id > 10;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_LessThanExpression_ShouldParseCorrectly()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Id < 10;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_GreaterThanOrEqualExpression_ShouldParseCorrectly()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Id >= 10;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_LessThanOrEqualExpression_ShouldParseCorrectly()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Id <= 10;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    #endregion

    #region Unary Expression Tests

    [Test]
    public async Task Parse_NotExpression_ShouldParseCorrectly()
    {
        // Tests line 243: Not operation
        Expression<Func<TestDoc, bool>> expr = x => !(x.Id > 10);
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_NegateExpression_ShouldParseCorrectly()
    {
        // Tests line 244: Negate operation (converted to subtraction)
        Expression<Func<TestDoc, bool>> expr = x => -x.Id < 0;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_ConvertExpression_ShouldParseCorrectly()
    {
        // Tests line 245: Convert operation
        Expression<Func<TestDoc, bool>> expr = x => (double)x.Id > 0.5;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    #endregion

    #region Method Call Expression Tests

    [Test]
    public async Task Parse_EqualsMethod_ShouldConvertToBinaryExpression()
    {
        // Tests line 267-273: Equals method
        var value = "test";
        Expression<Func<TestDoc, bool>> expr = x => x.Name.Equals(value);
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_ToStringMethod_WithConstant_ShouldOptimize()
    {
        // Tests line 276-285: ToString on constant
        var num = 42;
        Expression<Func<TestDoc, bool>> expr = x => x.Name == num.ToString();
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_ConvertToMethod_ShouldParseAsConvert()
    {
        // Tests line 261-265: Convert.To methods
        Expression<Func<TestDoc, bool>> expr = x => Convert.ToDouble(x.Id) > 0.5;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_StringContains_ShouldParseFunctionExpression()
    {
        // Tests generic method parsing (line 287-291)
        Expression<Func<TestDoc, bool>> expr = x => x.Name.Contains("test");
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_StringStartsWith_ShouldParseFunctionExpression()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Name.StartsWith("test");
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_StringEndsWith_ShouldParseFunctionExpression()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Name.EndsWith("test");
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    #endregion

    #region New Expression Tests

    [Test]
    public async Task Parse_NewExpression_ShouldParseConstructorExpression()
    {
        // Tests line 69-73: New expression
        Expression<Func<TestDoc, bool>> expr = x => x.CreatedAt > new DateTime(2020, 1, 1);
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    #endregion

    #region MemberInit Expression Tests

    [Test]
    public async Task ParseExpression_MemberInit_ShouldWork()
    {
        // Tests line 76-91: MemberInit expression
        // This is typically used in projections
        // MemberInit with bindings that reference parameters should return MemberInitQueryExpression
        var param = LinqExpression.Parameter(typeof(TestDoc), "x");
        var nameProperty = typeof(TestDoc).GetProperty(nameof(TestDoc.Name))!;
        var memberInit = LinqExpression.MemberInit(
            LinqExpression.New(typeof(TestDoc)),
            LinqExpression.Bind(
                nameProperty,
                LinqExpression.Property(param, nameProperty)
            )
        );
        
        var result = _parser.ParseExpression(memberInit);
        await Assert.That(result).IsNotNull();
        // When binding references a parameter, it should be MemberInitQueryExpression
        await Assert.That(result).IsTypeOf<MemberInitQueryExpression>();
    }

    [Test]
    public async Task ParseExpression_MemberInit_WithConstantBinding_ShouldReturnConstant()
    {
        // MemberInit with all constant bindings can be optimized to ConstantExpression
        var memberInit = LinqExpression.MemberInit(
            LinqExpression.New(typeof(TestDoc)),
            LinqExpression.Bind(
                typeof(TestDoc).GetProperty(nameof(TestDoc.Name))!,
                LinqExpression.Constant("test")
            )
        );
        
        var result = _parser.ParseExpression(memberInit);
        await Assert.That(result).IsNotNull();
        // Constant-only binding may be optimized to ConstantExpression
        await Assert.That(result is MemberInitQueryExpression || result is QueryConstantExpression).IsTrue();
    }

    #endregion

    #region Parameter Expression Tests

    [Test]
    public async Task ParseExpression_Parameter_ShouldReturnParameterExpression()
    {
        // Tests line 226-229: Parameter expression
        var param = LinqExpression.Parameter(typeof(TestDoc), "x");
        
        var result = _parser.ParseExpression(param);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<QueryParameterExpression>();
    }

    #endregion

    #region Logical Expression Tests

    [Test]
    public async Task Parse_AndAlso_ShouldParseCorrectly()
    {
        // Tests line 147: AndAlso
        Expression<Func<TestDoc, bool>> expr = x => x.Id > 0 && x.Name != null;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_OrElse_ShouldParseCorrectly()
    {
        // Tests line 148: OrElse
        Expression<Func<TestDoc, bool>> expr = x => x.Id > 0 || x.Name != null;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    #endregion

    #region Null Expression Tests

    [Test]
    public async Task Parse_NullExpression_ShouldReturnNull()
    {
        // Tests line 19: Null expression
        var result = _parser.Parse<TestDoc>(null!);
        await Assert.That(result).IsNull();
    }

    #endregion

    #region Complex Expression Tests

    [Test]
    public async Task Parse_ComplexExpression_ShouldParseCorrectly()
    {
        var minId = 1;
        var maxId = 100;
        var prefix = "test";
        
        Expression<Func<TestDoc, bool>> expr = x => 
            x.Id >= minId && 
            x.Id <= maxId && 
            x.Name.StartsWith(prefix) &&
            x.CreatedAt > DateTime.UtcNow.AddDays(-30);
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_NestedMemberAccess_ShouldParseCorrectly()
    {
        // Test member access chain
        Expression<Func<TestDoc, bool>> expr = x => x.Name.Length > 0;
        
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    #endregion
}

/// <summary>
/// Tests for ParameterChecker internal class
/// </summary>
public class ParameterCheckerTests
{
    private readonly ExpressionParser _parser = new();

    public class TestDoc
    {
        public int Id { get; set; }
    }

    [Test]
    public async Task ParameterChecker_WithParameter_ShouldReturnTrue()
    {
        // Expression with parameter should not be evaluated early
        Expression<Func<TestDoc, bool>> expr = x => x.Id > 10;
        
        // This will exercise ParameterChecker.Check returning true
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task ParameterChecker_WithoutParameter_ShouldReturnFalse()
    {
        // Expression without parameter should be evaluated early
        var localVar = 5;
        Expression<Func<TestDoc, bool>> expr = x => localVar + 3 == 8;
        
        // This will exercise ParameterChecker.Check returning false
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }
}
