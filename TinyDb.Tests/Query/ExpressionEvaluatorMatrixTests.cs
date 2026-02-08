using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

using TBinaryExpression = TinyDb.Query.BinaryExpression;
using TConstantExpression = TinyDb.Query.ConstantExpression;
using TFunctionExpression = TinyDb.Query.FunctionExpression;

namespace TinyDb.Tests.Query;

public class ExpressionEvaluatorMatrixTests
{
    private static readonly object[] NumericValues = 
    {
        (byte)1, (sbyte)1, (short)1, (ushort)1, (int)1, (uint)1, (long)1, (ulong)1,
        (float)1.0f, (double)1.0, 1.0m, new Decimal128(1.0m)
    };

    private static readonly object[] TwoValues = 
    {
        (byte)2, (sbyte)2, (short)2, (ushort)2, (int)2, (uint)2, (long)2, (ulong)2,
        (float)2.0f, (double)2.0, 2.0m, new Decimal128(2.0m)
    };

    [Test]
    public async Task Compare_NumericMatrix_AllEqual()
    {
        foreach (var left in NumericValues)
        {
            foreach (var right in NumericValues)
            {
                var queryExpr = new TBinaryExpression(ExpressionType.Equal, 
                    new TConstantExpression(left), 
                    new TConstantExpression(right));
                
                var result = ExpressionEvaluator.Evaluate(queryExpr, new BsonDocument());
                if (!result)
                {
                    Assert.Fail($"Failed equality: {left.GetType().Name}({left}) == {right.GetType().Name}({right})");
                }
                await Assert.That(result).IsTrue();
            }
        }
    }

    [Test]
    public async Task Compare_NumericMatrix_Inequality()
    {
        foreach (var left in NumericValues) // 1
        {
            foreach (var right in TwoValues) // 2
            {
                var lt = new TBinaryExpression(ExpressionType.LessThan, new TConstantExpression(left), new TConstantExpression(right));
                await Assert.That(ExpressionEvaluator.Evaluate(lt, new BsonDocument())).IsTrue();
                
                var gt = new TBinaryExpression(ExpressionType.GreaterThan, new TConstantExpression(left), new TConstantExpression(right));
                await Assert.That(ExpressionEvaluator.Evaluate(gt, new BsonDocument())).IsFalse();
            }
        }
    }

    [Test]
    public async Task Compare_NonNumeric_Types()
    {
        // byte[]
        var b1 = new byte[] { 1, 2, 3 };
        var b2 = new byte[] { 1, 2, 3 };
        var b3 = new byte[] { 1, 2, 4 };
        var b4 = new byte[] { 1, 2 }; // Shorter
        
        await Assert.That(EvalEq(b1, b2)).IsTrue();
        await Assert.That(EvalEq(b1, b3)).IsFalse();
        await Assert.That(EvalEq(b1, b4)).IsFalse();
        
        // string
        await Assert.That(EvalEq("a", "a")).IsTrue();
        await Assert.That(EvalEq("a", "b")).IsFalse();
        
        // IComparable (DateTime)
        var dt = DateTime.UtcNow;
        await Assert.That(EvalEq(dt, dt)).IsTrue();
        await Assert.That(EvalEq(dt, dt.AddDays(1))).IsFalse();
        
        // Null
        await Assert.That(EvalEq(null, null)).IsTrue();
        await Assert.That(EvalEq(null, 1)).IsFalse();
        await Assert.That(EvalEq(1, null)).IsFalse();
        
        // Mixed Types (Fallback to ToString)
        // "1" == 1 -> "1" == "1"
        await Assert.That(EvalEq("1", 1)).IsTrue();
        await Assert.That(EvalEq(1, "1")).IsTrue();
        
        // "a" != 1
        await Assert.That(EvalEq("a", 1)).IsFalse();
    }

    [Test]
    public async Task MathOp_NumericMatrix_Add()
    {
        foreach (var left in NumericValues)
        {
            foreach (var right in TwoValues)
            {
                var add = new TBinaryExpression(ExpressionType.Add, new TConstantExpression(left), new TConstantExpression(right));
                var eq = new TBinaryExpression(ExpressionType.Equal, add, new TConstantExpression(3));
                var result = ExpressionEvaluator.Evaluate(eq, new BsonDocument());
                
                if (!result) Assert.Fail($"Add failed: {left} + {right} != 3");
                await Assert.That(result).IsTrue();
            }
        }
    }

    [Test]
    public async Task EvaluateFunction_Math_Abs()
    {
        await Assert.That(EvalMath("Abs", -1, 1)).IsTrue();
        await Assert.That(EvalMath("Abs", -1.5, 1.5)).IsTrue();
        await Assert.That(EvalMath("Abs", -1.5m, 1.5m)).IsTrue();
    }
    
    [Test]
    public async Task EvaluateFunction_Math_Round()
    {
        await Assert.That(EvalMath("Round", 1.2, 1.0)).IsTrue();
        await Assert.That(EvalMath("Round", 1.5, 2.0)).IsTrue(); 
        await Assert.That(EvalMath("Round", 1.234, 2, 1.23)).IsTrue(); 
    }

    private bool EvalMath(string func, object arg, object expected)
    {
        var call = new TFunctionExpression(func, null, new QueryExpression[] { new TConstantExpression(arg) });
        var eq = new TBinaryExpression(ExpressionType.Equal, call, new TConstantExpression(expected));
        return ExpressionEvaluator.Evaluate(eq, new BsonDocument());
    }
    
    private bool EvalMath(string func, object arg1, object arg2, object expected)
    {
        var call = new TFunctionExpression(func, null, new QueryExpression[] { new TConstantExpression(arg1), new TConstantExpression(arg2) });
        var eq = new TBinaryExpression(ExpressionType.Equal, call, new TConstantExpression(expected));
        return ExpressionEvaluator.Evaluate(eq, new BsonDocument());
    }
    
    private bool EvalEq(object? left, object? right)
    {
        var eq = new TBinaryExpression(ExpressionType.Equal, new TConstantExpression(left), new TConstantExpression(right));
        return ExpressionEvaluator.Evaluate(eq, new BsonDocument());
    }
}
