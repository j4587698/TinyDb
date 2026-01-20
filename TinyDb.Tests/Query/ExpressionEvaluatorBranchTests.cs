using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using BinaryExpression = TinyDb.Query.BinaryExpression;
using ConstantExpression = TinyDb.Query.ConstantExpression;
using FunctionExpression = TinyDb.Query.FunctionExpression;

namespace TinyDb.Tests.Query;

public class ExpressionEvaluatorBranchTests
{
    private class TestObj : IComparable
    {
        public int Value { get; set; }
        public int CompareTo(object? obj) => obj is TestObj o ? Value.CompareTo(o.Value) : 1;
    }

    private class NonComparable { }

    [Test]
    public async Task Compare_Nulls_ShouldWork()
    {
        // Compare is private, we test via BinaryExpression Equal/CompareTo
        // Or Reflection? Reflection is better to target Compare directly.
        var method = typeof(ExpressionEvaluator).GetMethod("Compare", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        
        await Assert.That((int)method!.Invoke(null, new object[] { null, null })!).IsEqualTo(0);
        await Assert.That((int)method.Invoke(null, new object[] { null, 1 })!).IsEqualTo(-1);
        await Assert.That((int)method.Invoke(null, new object[] { 1, null })!).IsEqualTo(1);
    }

    [Test]
    public async Task Compare_BsonWrappers_ShouldWork()
    {
        var method = typeof(ExpressionEvaluator).GetMethod("Compare", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        
        await Assert.That((int)method!.Invoke(null, new object[] { new BsonInt32(1), 1 })!).IsEqualTo(0);
        await Assert.That((int)method.Invoke(null, new object[] { 1, new BsonInt32(1) })!).IsEqualTo(0);
    }

    [Test]
    public async Task Compare_NumericTypes_ShouldWork()
    {
        var method = typeof(ExpressionEvaluator).GetMethod("Compare", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        
        // Decimal128 mixing
        var d128 = new Decimal128(10.5m);
        await Assert.That((int)method!.Invoke(null, new object[] { d128, 10.5m })!).IsEqualTo(0);
        await Assert.That((int)method.Invoke(null, new object[] { 10.5m, d128 })!).IsEqualTo(0);
        
        // Int vs Double
        await Assert.That((int)method.Invoke(null, new object[] { 10, 10.0 })!).IsEqualTo(0);
        
        // Decimal vs Int
        await Assert.That((int)method.Invoke(null, new object[] { 10m, 10 })!).IsEqualTo(0);
    }

    [Test]
    public async Task Compare_ByteArrays_ShouldWork()
    {
        var method = typeof(ExpressionEvaluator).GetMethod("Compare", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        
        byte[] b1 = { 1, 2, 3 };
        byte[] b2 = { 1, 2, 3 };
        byte[] b3 = { 1, 2, 4 };
        byte[] b4 = { 1, 2 };
        
        await Assert.That((int)method!.Invoke(null, new object[] { b1, b2 })!).IsEqualTo(0);
        await Assert.That((int)method.Invoke(null, new object[] { b1, b3 })!).IsLessThan(0);
        await Assert.That((int)method.Invoke(null, new object[] { b3, b1 })!).IsGreaterThan(0);
        await Assert.That((int)method.Invoke(null, new object[] { b1, b4 })!).IsGreaterThan(0); // Length check
    }

    [Test]
    public async Task Compare_IComparable_ShouldWork()
    {
        var method = typeof(ExpressionEvaluator).GetMethod("Compare", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        
        var o1 = new TestObj { Value = 1 };
        var o2 = new TestObj { Value = 2 };
        
        await Assert.That((int)method!.Invoke(null, new object[] { o1, o2 })!).IsLessThan(0);
    }

    [Test]
    public async Task Compare_ToStringFallback_ShouldWork()
    {
        var method = typeof(ExpressionEvaluator).GetMethod("Compare", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        
        var o1 = new NonComparable();
        var o2 = new NonComparable();
        
        // ToString returns type name usually, so they are equal
        await Assert.That((int)method!.Invoke(null, new object[] { o1, o2 })!).IsEqualTo(0);
    }

    [Test]
    public async Task EvaluateMathOp_Integers_ShouldReturnIntOrLong()
    {
        var doc = new BsonDocument();
        // 1 + 2 = 3 (int)
        var add = new BinaryExpression(ExpressionType.Add, new ConstantExpression(1), new ConstantExpression(2));
        var res = ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.Equal, add, new ConstantExpression(3)), doc);
        await Assert.That(res).IsTrue();
        
        // Check return type via reflection or careful assertion?
        // We can check if it equals double 3.0? Compare handles types.
        // Let's call EvaluateMathOp directly to check return type.
        var method = typeof(ExpressionEvaluator).GetMethod("EvaluateMathOp", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
        
        // Funcs
        Func<double, double, double> addD = (a, b) => a + b;
        Func<decimal, decimal, decimal> addM = (a, b) => a + b;
        
        var intRes = method.Invoke(null, new object[] { 1, 2, addD, addM });
        await Assert.That(intRes).IsTypeOf<int>();
        
        long l1 = 1;
        long l2 = 2;
        var longRes = method.Invoke(null, new object[] { l1, l2, addD, addM });
        // Even if inputs are long, if result fits in int, it returns int
        await Assert.That(longRes).IsTypeOf<int>();
        
        // Overflow int -> long?
        // MaxInt + 1 -> Double (because logic uses doubles)
        // 2147483647 + 1 = 2147483648. Fits in long.
        // My logic: if (dResult == Math.Floor(dResult))... if (dResult >= int.MinValue && ...) return int else return long.
        // So it should return long.
        var bigRes = method.Invoke(null, new object[] { int.MaxValue, 1, addD, addM });
        await Assert.That(bigRes).IsTypeOf<long>();
    }

    [Test]
    public async Task Evaluate_Logic_ShortCircuit_ShouldWork()
    {
        var doc = new BsonDocument();
        
        // AndAlso: False && Throw -> False (Should not throw)
        var andExpr = new BinaryExpression(ExpressionType.AndAlso, 
            new ConstantExpression(false),
            new FunctionExpression("ToString", null, new[] { new ConstantExpression(null) }) // ToString on null throws in EvaluateFunction
        );
        
        // EvaluateFunction throws if target is null and func is not static math/etc?
        // EvaluateFunction: if (targetValue == null) -> Math methods check. ToString is not there.
        // "Function 'ToString' is not supported for type null" -> Throws NotSupportedException.
        
        // So if right side is evaluated, it throws NotSupportedException.
        // If short-circuit works, it returns false.
        
        await Assert.That(ExpressionEvaluator.Evaluate(andExpr, doc)).IsFalse();
        
        // OrElse: True || Throw -> True
        var orExpr = new BinaryExpression(ExpressionType.OrElse,
            new ConstantExpression(true),
            new FunctionExpression("ToString", null, new[] { new ConstantExpression(null) })
        );
        
        await Assert.That(ExpressionEvaluator.Evaluate(orExpr, doc)).IsTrue();
    }
}
