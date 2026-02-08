using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using BinaryExpression = TinyDb.Query.BinaryExpression;
using ConstantExpression = TinyDb.Query.ConstantExpression;
using FunctionExpression = TinyDb.Query.FunctionExpression;
using MemberExpression = TinyDb.Query.MemberExpression;

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
        
        await Assert.That((int)method!.Invoke(null, new object?[] { null, null })!).IsEqualTo(0);
        await Assert.That((int)method.Invoke(null, new object?[] { BsonNull.Value, null })!).IsEqualTo(0);
        await Assert.That((int)method.Invoke(null, new object?[] { null, 1 })!).IsEqualTo(-1);
        await Assert.That((int)method.Invoke(null, new object?[] { 1, null })!).IsEqualTo(1);
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
        var method = typeof(ExpressionEvaluator).GetMethod(
            "EvaluateMathOp",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static
        ) ?? throw new InvalidOperationException("EvaluateMathOp not found.");
        
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

    [Test]
    public async Task Evaluate_Binary_SubtractAndDivide_ShouldWork()
    {
        var entity = new object();

        var subtract = new BinaryExpression(ExpressionType.Subtract, new ConstantExpression(10), new ConstantExpression(3));
        await Assert.That(ExpressionEvaluator.EvaluateValue(subtract, entity)).IsEqualTo(7);

        var subtractDecimal = new BinaryExpression(ExpressionType.Subtract, new ConstantExpression(10m), new ConstantExpression(3m));
        await Assert.That(ExpressionEvaluator.EvaluateValue(subtractDecimal, entity)).IsEqualTo(7m);

        var divide = new BinaryExpression(ExpressionType.Divide, new ConstantExpression(10), new ConstantExpression(4));
        await Assert.That(ExpressionEvaluator.EvaluateValue(divide, entity)).IsEqualTo(2.5);

        var divideDecimal = new BinaryExpression(ExpressionType.Divide, new ConstantExpression(10m), new ConstantExpression(4m));
        await Assert.That(ExpressionEvaluator.EvaluateValue(divideDecimal, entity)).IsEqualTo(2.5m);
    }

    [Test]
    public async Task Evaluate_Logic_NonBooleanOperands_ShouldReturnFalseWhenRightIsNotBool()
    {
        var doc = new BsonDocument();

        var andRightNotBool = new BinaryExpression(
            ExpressionType.AndAlso,
            new ConstantExpression(true),
            new ConstantExpression(123)
        );
        await Assert.That(ExpressionEvaluator.Evaluate(andRightNotBool, doc)).IsFalse();

        var orRightNotBool = new BinaryExpression(
            ExpressionType.OrElse,
            new ConstantExpression(false),
            new ConstantExpression(123)
        );
        await Assert.That(ExpressionEvaluator.Evaluate(orRightNotBool, doc)).IsFalse();
    }

    [Test]
    public async Task Evaluate_Function_Math_CeilingFloorRound_TwoArgs_ShouldWork()
    {
        var entity = new object();

        var ceil = new FunctionExpression("Ceiling", null, new[] { new ConstantExpression(1.2) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(ceil, entity)).IsEqualTo(2.0);

        var floor = new FunctionExpression("Floor", null, new[] { new ConstantExpression(1.8) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(floor, entity)).IsEqualTo(1.0);

        var ceilDecimal = new FunctionExpression("Ceiling", null, new[] { new ConstantExpression(1.2m) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(ceilDecimal, entity)).IsEqualTo(2m);

        var floorDecimal = new FunctionExpression("Floor", null, new[] { new ConstantExpression(1.8m) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(floorDecimal, entity)).IsEqualTo(1m);

        var round2 = new FunctionExpression("Round", null, new[] { new ConstantExpression(1.2345), new ConstantExpression(2) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(round2, entity)).IsEqualTo(1.23);

        var round2Decimal = new FunctionExpression("Round", null, new[] { new ConstantExpression(1.2345m), new ConstantExpression(2) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(round2Decimal, entity)).IsEqualTo(1.23m);
    }

    [Test]
    public async Task Evaluate_Function_String_InvalidArgs_ShouldReturnExpectedResults()
    {
        var entity = new object();

        var containsWrongType = new FunctionExpression("Contains", new ConstantExpression("abc"), new[] { new ConstantExpression(123) });
        await Assert.That(ExpressionEvaluator.Evaluate(containsWrongType, new BsonDocument())).IsFalse();

        var containsWrongArity = new FunctionExpression("Contains", new ConstantExpression("abc"), new[] { new ConstantExpression("a"), new ConstantExpression("b") });
        await Assert.That(ExpressionEvaluator.Evaluate(containsWrongArity, new BsonDocument())).IsFalse();

        var replaceWrongType = new FunctionExpression("Replace", new ConstantExpression("abc"), new[] { new ConstantExpression(1), new ConstantExpression(2) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(replaceWrongType, entity)).IsEqualTo("abc");

        var substringWrongArity = new FunctionExpression("Substring", new ConstantExpression("abcdef"), Array.Empty<QueryExpression>());
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(substringWrongArity, entity)).ThrowsExactly<ArgumentException>();

        var unsupportedStringFunc = new FunctionExpression("NoSuchStringFunction", new ConstantExpression("abc"), Array.Empty<QueryExpression>());
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(unsupportedStringFunc, entity)).ThrowsExactly<NotSupportedException>();
    }

    [Test]
    public async Task Evaluate_Function_Enumerable_ShiftTarget_ShouldWork()
    {
        var entity = new object();

        var countShift = new FunctionExpression("Count", null, new[] { new ConstantExpression(new[] { 1, 2, 3 }) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(countShift, entity)).IsEqualTo(3);

        var containsShift = new FunctionExpression("Contains", null, new[] { new ConstantExpression(new[] { 1, 2, 3 }), new ConstantExpression(2) });
        await Assert.That(ExpressionEvaluator.Evaluate(containsShift, new BsonDocument())).IsTrue();

        var bsonArray = new BsonArray(new BsonValue[] { new BsonInt32(1), new BsonInt32(2) });
        var containsBsonArray = new FunctionExpression("Contains", null, new[] { new ConstantExpression(bsonArray), new ConstantExpression(2) });
        await Assert.That(ExpressionEvaluator.Evaluate(containsBsonArray, new BsonDocument())).IsTrue();

        var nonCollectionEnumerable = Enumerable.Range(0, 5).Where(x => x < 3);
        var countNonCollection = new FunctionExpression("Count", new ConstantExpression(nonCollectionEnumerable), Array.Empty<QueryExpression>());
        await Assert.That(ExpressionEvaluator.EvaluateValue(countNonCollection, entity)).IsEqualTo(3);
    }

    [Test]
    public async Task Evaluate_MemberAccess_DateTimeProperties_ShouldWork()
    {
        var entity = new object();
        var dt = new DateTime(2024, 1, 2, 3, 4, 5);

        var expected = new (string Name, object Value)[]
        {
            ("Year", 2024),
            ("Month", 1),
            ("Day", 2),
            ("Hour", 3),
            ("Minute", 4),
            ("Second", 5),
            ("Date", dt.Date),
            ("DayOfWeek", (int)dt.DayOfWeek)
        };

        foreach (var (name, value) in expected)
        {
            var expr = new MemberExpression(name, new ConstantExpression(dt));
            await Assert.That(ExpressionEvaluator.EvaluateValue(expr, entity)).IsEqualTo(value);
        }

        var unknown = new MemberExpression("UnknownProperty", new ConstantExpression(dt));
        await Assert.That(ExpressionEvaluator.EvaluateValue(unknown, entity)).IsNull();
    }

    [Test]
    public async Task Evaluate_Function_DateTimeFunctions_ShouldWork()
    {
        var entity = new object();
        var dt = new DateTime(2024, 1, 2, 3, 4, 5);

        var addDays = new FunctionExpression("AddDays", new ConstantExpression(dt), new[] { new ConstantExpression(1) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(addDays, entity)).IsEqualTo(dt.AddDays(1));

        var addHours = new FunctionExpression("AddHours", new ConstantExpression(dt), new[] { new ConstantExpression(1) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(addHours, entity)).IsEqualTo(dt.AddHours(1));

        var addMinutes = new FunctionExpression("AddMinutes", new ConstantExpression(dt), new[] { new ConstantExpression(1) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(addMinutes, entity)).IsEqualTo(dt.AddMinutes(1));

        var addSeconds = new FunctionExpression("AddSeconds", new ConstantExpression(dt), new[] { new ConstantExpression(1) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(addSeconds, entity)).IsEqualTo(dt.AddSeconds(1));

        var addYears = new FunctionExpression("AddYears", new ConstantExpression(dt), new[] { new ConstantExpression(1) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(addYears, entity)).IsEqualTo(dt.AddYears(1));

        var addMonths = new FunctionExpression("AddMonths", new ConstantExpression(dt), new[] { new ConstantExpression(1) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(addMonths, entity)).IsEqualTo(dt.AddMonths(1));

        var dtToString = new FunctionExpression("ToString", new ConstantExpression(dt), Array.Empty<QueryExpression>());
        await Assert.That(ExpressionEvaluator.EvaluateValue(dtToString, entity)).IsEqualTo(dt.ToString());

        var unsupported = new FunctionExpression("NoSuchDateTimeFunction", new ConstantExpression(dt), Array.Empty<QueryExpression>());
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(unsupported, entity)).ThrowsExactly<NotSupportedException>();
    }
}
