using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using BinaryExpression = TinyDb.Query.BinaryExpression;
using ConstantExpression = TinyDb.Query.ConstantExpression;
using MemberExpression = TinyDb.Query.MemberExpression;
using UnaryExpression = TinyDb.Query.UnaryExpression;
using ParameterExpression = TinyDb.Query.ParameterExpression;

namespace TinyDb.Tests.Query;

public class ExpressionEvaluatorCoverageTests
{
    public class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public double Score { get; set; }
        public decimal Amount { get; set; }
        public DateTime Created { get; set; }
        public bool Active { get; set; }
        public List<string> Tags { get; set; } = new();
        public int? NullableId { get; set; }
    }

    [Test]
    public async Task Evaluate_Math_Functions()
    {
        var entity = new TestEntity { Score = -10.5 };
        
        // Abs
        await AssertEvaluate(entity, "Abs", entity.Score, 10.5);
        
        // Ceiling
        await AssertEvaluate(entity, "Ceiling", entity.Score, -10.0);
        
        // Floor
        await AssertEvaluate(entity, "Floor", entity.Score, -11.0);
        
        // Round (1 arg)
        entity.Score = 10.5; // Rounds to 10 (ToEven) or 11? Default is ToEven.
        // Actually Math.Round(10.5) is 10. Math.Round(11.5) is 12.
        await AssertEvaluate(entity, "Round", entity.Score, 10.0);
        
        // Sqrt
        entity.Score = 16.0;
        await AssertEvaluate(entity, "Sqrt", entity.Score, 4.0);

        // Pow (2 args)
        var expr = new FunctionExpression("Pow", null, new[] 
        { 
            new ConstantExpression(2.0), 
            new ConstantExpression(3.0) 
        });
        
        // Evaluate returns bool for Evaluate<T>(QueryExpression, T).
        // Wait, Evaluate<T> returns bool.
        // But EvaluateExpressionValue is private.
        // We can test via BinaryExpression: Pow(2,3) == 8
        var binExpr = new BinaryExpression(ExpressionType.Equal, expr, new ConstantExpression(8.0));
        await Assert.That(ExpressionEvaluator.Evaluate(binExpr, entity)).IsTrue();
    }

    [Test]
    public async Task Evaluate_Math_Min_Max()
    {
        var entity = new TestEntity();
        
        // Min
        var minExpr = new FunctionExpression("Min", null, new[] { new ConstantExpression(10), new ConstantExpression(5) });
        var checkMin = new BinaryExpression(ExpressionType.Equal, minExpr, new ConstantExpression(5.0)); // int converts to double
        await Assert.That(ExpressionEvaluator.Evaluate(checkMin, entity)).IsTrue();

        // Max
        var maxExpr = new FunctionExpression("Max", null, new[] { new ConstantExpression(10), new ConstantExpression(5) });
        var checkMax = new BinaryExpression(ExpressionType.Equal, maxExpr, new ConstantExpression(10.0));
        await Assert.That(ExpressionEvaluator.Evaluate(checkMax, entity)).IsTrue();
    }

    [Test]
    public async Task Evaluate_String_Functions()
    {
        var entity = new TestEntity { Name = "  Test String  " };
        
        // ToLower
        await AssertEvaluate(entity, "ToLower", "  Test String  ", "  test string  ", targetIsMember: true);
        
        // ToUpper
        await AssertEvaluate(entity, "ToUpper", "  Test String  ", "  TEST STRING  ", targetIsMember: true);
        
        // Trim
        await AssertEvaluate(entity, "Trim", "  Test String  ", "Test String", targetIsMember: true);
        
        // Substring(int)
        entity.Name = "012345";
        var sub1 = new FunctionExpression("Substring", new MemberExpression("Name", null), new[] { new ConstantExpression(2) });
        var checkSub1 = new BinaryExpression(ExpressionType.Equal, sub1, new ConstantExpression("2345"));
        await Assert.That(ExpressionEvaluator.Evaluate(checkSub1, entity)).IsTrue();
        
        // Substring(int, int)
        var sub2 = new FunctionExpression("Substring", new MemberExpression("Name", null), new[] { new ConstantExpression(2), new ConstantExpression(2) });
        var checkSub2 = new BinaryExpression(ExpressionType.Equal, sub2, new ConstantExpression("23"));
        await Assert.That(ExpressionEvaluator.Evaluate(checkSub2, entity)).IsTrue();
        
        // Replace
        entity.Name = "banana";
        var rep = new FunctionExpression("Replace", new MemberExpression("Name", null), new[] { new ConstantExpression("a"), new ConstantExpression("o") });
        var checkRep = new BinaryExpression(ExpressionType.Equal, rep, new ConstantExpression("bonono"));
        await Assert.That(ExpressionEvaluator.Evaluate(checkRep, entity)).IsTrue();
    }

    [Test]
    public async Task Evaluate_DateTime_Functions()
    {
        var now = DateTime.UtcNow;
        var entity = new TestEntity { Created = now };
        
        // AddDays
        var addDays = new FunctionExpression("AddDays", new MemberExpression("Created", null), new[] { new ConstantExpression(1.0) });
        var checkDays = new BinaryExpression(ExpressionType.Equal, addDays, new ConstantExpression(now.AddDays(1)));
        await Assert.That(ExpressionEvaluator.Evaluate(checkDays, entity)).IsTrue();
        
        // AddHours
        var addHours = new FunctionExpression("AddHours", new MemberExpression("Created", null), new[] { new ConstantExpression(1.0) });
        var checkHours = new BinaryExpression(ExpressionType.Equal, addHours, new ConstantExpression(now.AddHours(1)));
        await Assert.That(ExpressionEvaluator.Evaluate(checkHours, entity)).IsTrue();
    }

    [Test]
    public async Task Evaluate_Collections_Count()
    {
        var entity = new TestEntity { Tags = new List<string> { "a", "b", "c" } };
        
        // Count property
        var countExpr = new MemberExpression("Count", new MemberExpression("Tags", null));
        var checkCount = new BinaryExpression(ExpressionType.Equal, countExpr, new ConstantExpression(3));
        await Assert.That(ExpressionEvaluator.Evaluate(checkCount, entity)).IsTrue();
        
        // Count method
        var countFunc = new FunctionExpression("Count", new MemberExpression("Tags", null), Array.Empty<QueryExpression>());
        var checkFunc = new BinaryExpression(ExpressionType.Equal, countFunc, new ConstantExpression(3));
        await Assert.That(ExpressionEvaluator.Evaluate(checkFunc, entity)).IsTrue();
        
        // Contains method
        var containsFunc = new FunctionExpression("Contains", new MemberExpression("Tags", null), new[] { new ConstantExpression("b") });
        var checkContains = new BinaryExpression(ExpressionType.Equal, containsFunc, new ConstantExpression(true));
        await Assert.That(ExpressionEvaluator.Evaluate(checkContains, entity)).IsTrue();
    }

    [Test]
    public async Task Evaluate_BsonDocument_Access()
    {
        var doc = new BsonDocument()
            .Set("name", "test")
            .Set("age", 25)
            .Set("active", true)
            .Set("tags", new BsonArray(new BsonValue[] { "x", "y" }));
            
        // Member Access
        var nameExpr = new MemberExpression("name", null);
        var checkName = new BinaryExpression(ExpressionType.Equal, nameExpr, new ConstantExpression("test"));
        await Assert.That(ExpressionEvaluator.Evaluate(checkName, doc)).IsTrue();
        
        // Case insensitive check (convention)
        var nameExprCap = new MemberExpression("Name", null); // Should map to "name" via camelCase
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.Equal, nameExprCap, new ConstantExpression("test")), doc)).IsTrue();
        
        // BsonArray Count
        var countExpr = new MemberExpression("Count", new MemberExpression("tags", null));
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.Equal, countExpr, new ConstantExpression(2)), doc)).IsTrue();
    }

    [Test]
    public async Task Evaluate_BsonDocument_BinaryOperators()
    {
        var doc = new BsonDocument().Set("val", 10).Set("boolTrue", true).Set("boolFalse", false);
        
        // Equal
        await AssertEvaluateBson(doc, ExpressionType.Equal, "val", 10, true);
        await AssertEvaluateBson(doc, ExpressionType.Equal, "val", 11, false);
        
        // NotEqual
        await AssertEvaluateBson(doc, ExpressionType.NotEqual, "val", 11, true);
        
        // GreaterThan
        await AssertEvaluateBson(doc, ExpressionType.GreaterThan, "val", 5, true);
        await AssertEvaluateBson(doc, ExpressionType.GreaterThan, "val", 15, false);
        
        // GreaterThanOrEqual
        await AssertEvaluateBson(doc, ExpressionType.GreaterThanOrEqual, "val", 10, true);
        
        // LessThan
        await AssertEvaluateBson(doc, ExpressionType.LessThan, "val", 15, true);
        
        // LessThanOrEqual
        await AssertEvaluateBson(doc, ExpressionType.LessThanOrEqual, "val", 10, true);
        
        // AndAlso
        var andExpr = new BinaryExpression(ExpressionType.AndAlso, 
            new MemberExpression("boolTrue", null), 
            new MemberExpression("boolTrue", null)); // true && true
        await Assert.That(ExpressionEvaluator.Evaluate(andExpr, doc)).IsTrue();
        
        // OrElse
        var orExpr = new BinaryExpression(ExpressionType.OrElse, 
            new MemberExpression("boolFalse", null), 
            new MemberExpression("boolTrue", null)); // false || true
        await Assert.That(ExpressionEvaluator.Evaluate(orExpr, doc)).IsTrue();
    }
    
    [Test]
    public async Task Evaluate_BsonDocument_MathOps()
    {
        var doc = new BsonDocument().Set("a", 10).Set("b", 5);
        
        // Add
        var add = new BinaryExpression(ExpressionType.Add, new MemberExpression("a", null), new MemberExpression("b", null));
        var checkAdd = new BinaryExpression(ExpressionType.Equal, add, new ConstantExpression(15.0)); // Result is double/decimal?
        // EvaluateMathOp returns double for int/int unless specially handled.
        // My implementation returns double or decimal.
        // Let's check: 10 + 5 -> 15.0 (double).
        await Assert.That(ExpressionEvaluator.Evaluate(checkAdd, doc)).IsTrue();
        
        // Subtract
        var sub = new BinaryExpression(ExpressionType.Subtract, new MemberExpression("a", null), new MemberExpression("b", null));
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.Equal, sub, new ConstantExpression(5.0)), doc)).IsTrue();
        
        // Multiply
        var mul = new BinaryExpression(ExpressionType.Multiply, new MemberExpression("a", null), new MemberExpression("b", null));
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.Equal, mul, new ConstantExpression(50.0)), doc)).IsTrue();
        
        // Divide
        var div = new BinaryExpression(ExpressionType.Divide, new MemberExpression("a", null), new MemberExpression("b", null));
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.Equal, div, new ConstantExpression(2.0)), doc)).IsTrue();
    }

    private async Task AssertEvaluateBson(BsonDocument doc, ExpressionType op, string field, object val, bool expected)
    {
        var expr = new BinaryExpression(op, new MemberExpression(field, null), new ConstantExpression(val));
        await Assert.That(ExpressionEvaluator.Evaluate(expr, doc)).IsEqualTo(expected);
    }

    [Test]
    public async Task Evaluate_Comparison_MixedTypes()
    {
        var entity = new TestEntity { Score = 10.0 };
        
        // Double vs Int
        var expr = new BinaryExpression(ExpressionType.Equal, new MemberExpression("Score", null), new ConstantExpression(10));
        await Assert.That(ExpressionEvaluator.Evaluate(expr, entity)).IsTrue();
        
        // Double vs Decimal
        var entityDec = new TestEntity { Amount = 10.5m };
        var exprDec = new BinaryExpression(ExpressionType.Equal, new MemberExpression("Amount", null), new ConstantExpression(10.5));
        await Assert.That(ExpressionEvaluator.Evaluate(exprDec, entityDec)).IsTrue();
    }

    private async Task AssertEvaluate(TestEntity entity, string funcName, object arg, object expected, bool targetIsMember = false)
    {
        QueryExpression target = targetIsMember ? new MemberExpression("Name", null) : null!; // Hacky for test structure
        if (!targetIsMember)
        {
             // Static call, target is null.
             // But we need to pass argument.
             // Wait, the structure of EvaluateMathOneArg takes args from function expression.
             // Target is usually null for Math.Abs(x).
             // But x comes from where? From args.
             
             // So if testing Math.Abs(Score):
             // Target = null
             // Args = [MemberExpression(Score)]
             
             target = null!;
             var funcExpr = new FunctionExpression(funcName, target, new[] { new MemberExpression("Score", null) });
             var binary = new BinaryExpression(ExpressionType.Equal, funcExpr, new ConstantExpression(expected));
             await Assert.That(ExpressionEvaluator.Evaluate(binary, entity)).IsTrue();
             return;
        }
        
        // For string methods: Name.ToLower()
        // Target = MemberExpression(Name)
        // Args = []
        var funcExprStr = new FunctionExpression(funcName, new MemberExpression("Name", null), Array.Empty<QueryExpression>());
        var binaryStr = new BinaryExpression(ExpressionType.Equal, funcExprStr, new ConstantExpression(expected));
        await Assert.That(ExpressionEvaluator.Evaluate(binaryStr, entity)).IsTrue();
    }
}
