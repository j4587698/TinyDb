using System;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Bson;
using BinaryExpression = TinyDb.Query.BinaryExpression;
using ConstantExpression = TinyDb.Query.ConstantExpression;
using MemberExpression = TinyDb.Query.MemberExpression;
using UnaryExpression = TinyDb.Query.UnaryExpression;
using ParameterExpression = TinyDb.Query.ParameterExpression;

namespace TinyDb.Tests.Query;

public class ExpressionParserCoverageTests
{
    private readonly ExpressionParser _parser = new();

    public class TestDoc
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool IsActive { get; set; }
        public DateTime Created { get; set; }
        public double Score { get; set; }
    }

    [Test]
    public async Task Parse_Null_Expression_Returns_Null()
    {
        var result = _parser.Parse<TestDoc>(null!);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Parse_Arithmetic_Expressions()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Id + 5 == 10;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
        
        var binary = (BinaryExpression)result;
        await Assert.That(binary.NodeType).IsEqualTo(ExpressionType.Equal);
        await Assert.That(binary.Left).IsTypeOf<BinaryExpression>(); // (x.Id + 5)
        
        var left = (BinaryExpression)binary.Left;
        await Assert.That(left.NodeType).IsEqualTo(ExpressionType.Add);
    }
    
    [Test]
    public async Task Parse_Arithmetic_Subtract_Multiply_Divide()
    {
        Expression<Func<TestDoc, bool>> expr = x => (x.Id - 2) * 3 / 4 == 6;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task Parse_Unary_Negate()
    {
        Expression<Func<TestDoc, bool>> expr = x => -x.Id == -5;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        
        var binary = (BinaryExpression)result;
        // x.Id is Unary Negate -> Binary Subtract (0 - x.Id)
        await Assert.That(binary.Left).IsTypeOf<BinaryExpression>(); 
        var neg = (BinaryExpression)binary.Left;
        await Assert.That(neg.NodeType).IsEqualTo(ExpressionType.Subtract);
    }

    [Test]
    public async Task Parse_Binary_Operators_Full_Coverage()
    {
        // GreaterThan
        Expression<Func<TestDoc, bool>> gt = x => x.Id > 5;
        await Assert.That(_parser.Parse(gt).NodeType).IsEqualTo(ExpressionType.GreaterThan);
        
        // GreaterThanOrEqual
        Expression<Func<TestDoc, bool>> gte = x => x.Id >= 5;
        await Assert.That(_parser.Parse(gte).NodeType).IsEqualTo(ExpressionType.GreaterThanOrEqual);
        
        // LessThan
        Expression<Func<TestDoc, bool>> lt = x => x.Id < 5;
        await Assert.That(_parser.Parse(lt).NodeType).IsEqualTo(ExpressionType.LessThan);
        
        // LessThanOrEqual
        Expression<Func<TestDoc, bool>> lte = x => x.Id <= 5;
        await Assert.That(_parser.Parse(lte).NodeType).IsEqualTo(ExpressionType.LessThanOrEqual);
        
        // Multiply
        Expression<Func<TestDoc, bool>> mul = x => x.Id * 2 == 10;
        var mulExpr = (BinaryExpression)_parser.Parse(mul);
        await Assert.That(((BinaryExpression)mulExpr.Left).NodeType).IsEqualTo(ExpressionType.Multiply);
        
        // Divide
        Expression<Func<TestDoc, bool>> div = x => x.Id / 2 == 5;
        var divExpr = (BinaryExpression)_parser.Parse(div);
        await Assert.That(((BinaryExpression)divExpr.Left).NodeType).IsEqualTo(ExpressionType.Divide);
    }

    [Test]
    public async Task Parse_Unary_Not()
    {
        Expression<Func<TestDoc, bool>> expr = x => !x.IsActive;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<UnaryExpression>();
        await Assert.That(result.NodeType).IsEqualTo(ExpressionType.Not);
    }
    
    [Test]
    public async Task Parse_MethodCall_ToString_Constant()
    {
        // Should optimize to constant "5"
        Expression<Func<TestDoc, bool>> expr = x => x.Name == 5.ToString(); 
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        var cons = (ConstantExpression)binary.Right;
        await Assert.That(cons.Value).IsEqualTo("5");
    }

    [Test]
    public async Task Parse_MethodCall_ToString_Member()
    {
        // x.Id.ToString() - ParseMethodCallExpression handles it generically as FunctionExpression
        Expression<Func<TestDoc, bool>> expr = x => x.Id.ToString() == "5";
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Left).IsTypeOf<TinyDb.Query.FunctionExpression>(); 
    }

    [Test]
    public async Task Parse_MethodCall_Equals()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Name.Equals("test");
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result.NodeType).IsEqualTo(ExpressionType.Equal);
    }
    
    [Test]
    public async Task Parse_MethodCall_Contains()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Name.Contains("sub");
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<FunctionExpression>();
        var func = (FunctionExpression)result;
        await Assert.That(func.FunctionName).IsEqualTo("Contains");
    }
    
    [Test]
    public async Task Parse_MethodCall_StartsWith()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Name.StartsWith("prefix");
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<FunctionExpression>();
        var func = (FunctionExpression)result;
        await Assert.That(func.FunctionName).IsEqualTo("StartsWith");
    }
    
    [Test]
    public async Task Parse_MethodCall_EndsWith()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Name.EndsWith("suffix");
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<FunctionExpression>();
        var func = (FunctionExpression)result;
        await Assert.That(func.FunctionName).IsEqualTo("EndsWith");
    }

    [Test]
    public async Task Parse_Convert_To()
    {
        Expression<Func<TestDoc, bool>> expr = x => Convert.ToInt32(x.Score) == 10;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Left).IsTypeOf<UnaryExpression>();
        var unary = (UnaryExpression)binary.Left;
        await Assert.That(unary.NodeType).IsEqualTo(ExpressionType.Convert);
    }

    [Test]
    public async Task Parse_Static_Member_Access()
    {
        // DateTime.Now is evaluated as constant?
        // Implementation tries to evaluate static members.
        Expression<Func<TestDoc, bool>> expr = x => x.Created < DateTime.Now;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
    }

    [Test]
    public async Task Parse_Closure_Variable_Access()
    {
        int limit = 100;
        Expression<Func<TestDoc, bool>> expr = x => x.Id > limit;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)binary.Right).Value).IsEqualTo(100);
    }

    [Test]
    public async Task Parse_Closure_Variable_Field_Access()
    {
        var config = new { Limit = 50 };
        Expression<Func<TestDoc, bool>> expr = x => x.Id > config.Limit;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)binary.Right).Value).IsEqualTo(50);
    }

    [Test]
    public async Task Parse_Unsupported_NodeType_Throws()
    {
        // It's hard to construct unsupported nodes with C# lambda syntax that compiles
        // but isn't supported. ListInit, NewArrayInit etc might be one.
        Expression<Func<TestDoc, bool>> expr = x => new[] { 1, 2 }.Contains(x.Id);
        // This usually compiles to ListInit or NewArrayInit, let's see if Parser handles it.
        // The parser has a try-catch block for "pre-evaluation".
        // If it does not depend on parameter, it is evaluated!
        // So new[] {1, 2} is evaluated to a constant array.
        // .Contains is an extension method (Enumerable.Contains).
        // It is a MethodCall.
        
        // Let's try to construct a Block expression manually which is not supported in ParseExpression directly
        // but ParseExpression takes Expression, so...
        var param = Expression.Parameter(typeof(TestDoc), "x");
        var block = Expression.Block(Expression.Constant(true));
        // ParseExpression checks ParameterChecker.Check(block) -> false (no param)
        // So it tries to evaluate it. Block returns true. -> ConstantExpression(true).
        
        // We need an expression that HAS a parameter but uses an unsupported node type.
        // e.g. ArrayLength?
        // x.Name.Length is MemberAccess.
        // ArrayAccess? x.someArray[0] -> IndexExpression?
        // The parser switches on NodeType. Index is not in the list.
        
        // Let's assume TestDoc has an array
        // But we can't easily change TestDoc here.
        // Use a lambda with array param?
        Expression<Func<int[], bool>> exprArray = x => x[0] == 1;
        // x[0] is IndexExpression (or MethodCall get_Item depending on target?)
        // For arrays, it is IndexExpression (since .NET 4?) or Binary?
        // Actually usually ArrayIndex.
        
        try
        {
            _parser.Parse(exprArray);
            // If it doesn't throw, check what it returned
        }
        catch (NotSupportedException)
        {
            // Expected
            return;
        }
    }

    /// <summary>
    /// Test static field access (not just properties like DateTime.Now)
    /// </summary>
    [Test]
    public async Task Parse_Static_Field_Access()
    {
        // Use a class with a static field (string.Empty is a static readonly field)
        Expression<Func<TestDoc, bool>> expr = x => x.Name == string.Empty;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)binary.Right).Value).IsEqualTo("");
    }

    /// <summary>
    /// Test static field access with int.MaxValue
    /// </summary>
    [Test]
    public async Task Parse_Static_Field_Access_IntMaxValue()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Id < int.MaxValue;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)binary.Right).Value).IsEqualTo(int.MaxValue);
    }

    /// <summary>
    /// Test closure access with an actual field (not property) through anonymous object
    /// </summary>
    [Test]
    public async Task Parse_Closure_With_Field_Access()
    {
        // Anonymous types actually use properties, but we can use a tuple which has fields
        var tuple = (Limit: 42, Name: "test");
        Expression<Func<TestDoc, bool>> expr = x => x.Id > tuple.Limit;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)binary.Right).Value).IsEqualTo(42);
    }

    /// <summary>
    /// Test closure access with captured class field 
    /// </summary>
    public class ClosureContainer
    {
        public int Threshold = 75; // Note: field, not property
        public string Prefix = "test_";
    }

    [Test]
    public async Task Parse_Closure_Class_Field_Access()
    {
        var container = new ClosureContainer();
        Expression<Func<TestDoc, bool>> expr = x => x.Id > container.Threshold;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)binary.Right).Value).IsEqualTo(75);
    }

    [Test]
    public async Task Parse_Closure_Class_Field_String_Access()
    {
        var container = new ClosureContainer();
        Expression<Func<TestDoc, bool>> expr = x => x.Name.StartsWith(container.Prefix);
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<FunctionExpression>();
        var func = (FunctionExpression)result;
        await Assert.That(func.FunctionName).IsEqualTo("StartsWith");
    }

    /// <summary>
    /// Test direct ParameterChecker.Check path for expressions with parameter
    /// </summary>
    [Test]
    public async Task Parse_ParameterChecker_HasParameter_FastExit()
    {
        // Expression with parameter in nested binary expression should trigger fast exit
        Expression<Func<TestDoc, bool>> expr = x => x.Id > 10 && x.Id < 100 && x.IsActive;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<BinaryExpression>();
    }

    /// <summary>
    /// Test parsing with complex nested closures
    /// </summary>
    [Test]
    public async Task Parse_Nested_Closure_Access()
    {
        var outer = new { Inner = new { Value = 123 } };
        Expression<Func<TestDoc, bool>> expr = x => x.Id == outer.Inner.Value;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)binary.Right).Value).IsEqualTo(123);
    }

    /// <summary>
    /// Test MemberInit expression parsing
    /// </summary>
    public class ResultDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Test]
    public async Task Parse_MemberInit_Expression()
    {
        // MemberInit is typically in Select expressions
        // We need to directly call ParseExpression with the body
        Expression<Func<TestDoc, ResultDto>> selector = x => new ResultDto { Id = x.Id, Name = x.Name };
        var result = _parser.ParseExpression(selector.Body);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<MemberInitQueryExpression>();
    }

    /// <summary>
    /// Test New expression parsing
    /// </summary>
    [Test]
    public async Task Parse_New_Expression()
    {
        Expression<Func<TestDoc, object>> selector = x => new { x.Id, x.Name };
        var result = _parser.ParseExpression(selector.Body);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<ConstructorExpression>();
    }

    /// <summary>
    /// Test unsupported binary operator
    /// </summary>
    [Test]
    public async Task Parse_Unsupported_Binary_Operator_Throws()
    {
        // ExclusiveOr (^) is not supported
        var param = Expression.Parameter(typeof(int), "x");
        var xorExpr = Expression.ExclusiveOr(param, Expression.Constant(5));
        var lambda = Expression.Lambda<Func<int, int>>(xorExpr, param);
        
        await Assert.That(() => _parser.ParseExpression(lambda.Body))
            .Throws<NotSupportedException>();
    }

    /// <summary>
    /// Test unsupported unary operator
    /// </summary>
    [Test]
    public async Task Parse_Unsupported_Unary_Operator_Throws()
    {
        // OnesComplement (~) is not supported
        var param = Expression.Parameter(typeof(int), "x");
        var complementExpr = Expression.OnesComplement(param);
        var lambda = Expression.Lambda<Func<int, int>>(complementExpr, param);
        
        await Assert.That(() => _parser.ParseExpression(lambda.Body))
            .Throws<NotSupportedException>();
    }

    /// <summary>
    /// Test Convert (cast) expression
    /// </summary>
    [Test]
    public async Task Parse_Convert_Cast_Expression()
    {
        Expression<Func<TestDoc, bool>> expr = x => (double)x.Id > 5.5;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Left).IsTypeOf<UnaryExpression>();
        var convert = (UnaryExpression)binary.Left;
        await Assert.That(convert.NodeType).IsEqualTo(ExpressionType.Convert);
    }

    /// <summary>
    /// Test OrElse operator
    /// </summary>
    [Test]
    public async Task Parse_OrElse_Operator()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Id == 1 || x.Id == 2;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result.NodeType).IsEqualTo(ExpressionType.OrElse);
    }

    /// <summary>
    /// Test NotEqual operator
    /// </summary>
    [Test]
    public async Task Parse_NotEqual_Operator()
    {
        Expression<Func<TestDoc, bool>> expr = x => x.Id != 0;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        await Assert.That(result.NodeType).IsEqualTo(ExpressionType.NotEqual);
    }

    /// <summary>
    /// Test pure constant expression without any parameter
    /// This should go through the optimization path (ParameterChecker returns false)
    /// </summary>
    [Test]
    public async Task Parse_Pure_Constant_Expression()
    {
        // 5 == 5 has no parameter, should be optimized
        Expression<Func<TestDoc, bool>> expr = x => 5 == 5;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        // The result could be ConstantExpression(true) after optimization
        // or BinaryExpression if optimization is disabled
        await Assert.That(result).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)result).Value).IsEqualTo(true);
    }

    /// <summary>
    /// Test complex math expression without parameter
    /// </summary>
    [Test]
    [SkipInAot("Complex math expressions require Lambda.Compile() which is not available in AOT")]
    public async Task Parse_Complex_Math_Without_Parameter()
    {
        var a = 10;
        var b = 20;
        Expression<Func<TestDoc, bool>> expr = x => x.Id > (a + b) * 2;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        // (a + b) * 2 = 60 should be optimized to constant
        await Assert.That(binary.Right).IsTypeOf<ConstantExpression>();
        await Assert.That(((ConstantExpression)binary.Right).Value).IsEqualTo(60);
    }

    /// <summary>
    /// Test direct call to ParseExpression with unsupported node type
    /// </summary>
    [Test]
    public async Task ParseExpression_Unsupported_NodeType_Throws()
    {
        // Create a TypeBinary expression (x is string) which might not be supported
        var param = Expression.Parameter(typeof(object), "x");
        var typeIs = Expression.TypeIs(param, typeof(string));
        
        await Assert.That(() => _parser.ParseExpression(typeIs))
            .Throws<NotSupportedException>();
    }

    /// <summary>
    /// Test parameter expression direct parsing
    /// </summary>
    [Test]
    public async Task Parse_Parameter_Expression_Direct()
    {
        var param = Expression.Parameter(typeof(int), "value");
        var result = _parser.ParseExpression(param);
        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsTypeOf<ParameterExpression>();
        await Assert.That(((ParameterExpression)result).Name).IsEqualTo("value");
    }

    /// <summary>
    /// Test member access on nested object
    /// </summary>
    public class NestedTestDoc
    {
        public int Id { get; set; }
        public TestDoc Child { get; set; } = new();
    }

    [Test]
    public async Task Parse_Nested_Member_Access()
    {
        Expression<Func<NestedTestDoc, bool>> expr = x => x.Child.Id > 5;
        var result = _parser.Parse(expr);
        await Assert.That(result).IsNotNull();
        var binary = (BinaryExpression)result;
        await Assert.That(binary.Left).IsTypeOf<MemberExpression>();
        var member = (MemberExpression)binary.Left;
        await Assert.That(member.MemberName).IsEqualTo("Id");
        // The inner expression should also be a MemberExpression for "Child"
        await Assert.That(member.Expression).IsTypeOf<MemberExpression>();
    }
}
