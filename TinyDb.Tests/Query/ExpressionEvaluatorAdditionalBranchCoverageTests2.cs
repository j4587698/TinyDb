using System;
using System.Collections;
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
using MemberInitQueryExpression = TinyDb.Query.MemberInitQueryExpression;
using UnaryExpression = TinyDb.Query.UnaryExpression;

namespace TinyDb.Tests.Query;

public class ExpressionEvaluatorAdditionalBranchCoverageTests2
{
    private sealed class DummyEntity
    {
        public bool FlagTrue { get; set; } = true;
        public bool FlagFalse { get; set; }
        public string Name { get; set; } = "x";
        public string? MaybeNull { get; set; }
        public int Value { get; set; } = 123;
    }

    private sealed class EnumerableOnly : IEnumerable
    {
        public IEnumerator GetEnumerator()
        {
            yield return 1;
            yield return 2;
            yield return 3;
        }
    }

    private sealed class UnknownQueryExpression : QueryExpression
    {
        public override ExpressionType NodeType => ExpressionType.Extension;
    }

    private sealed class Item
    {
        public int Value { get; set; }
    }

    [Test]
    public async Task Evaluate_ConstantExpression_BooleanAndNonBoolean_Branches()
    {
        var entity = new DummyEntity();

        await Assert.That(ExpressionEvaluator.Evaluate(new ConstantExpression(true), entity)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(new ConstantExpression(false), entity)).IsFalse();
        await Assert.That(() => ExpressionEvaluator.Evaluate(new ConstantExpression(1), entity)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task EvaluateValue_MemberInit_NullableValueType_ShouldReturnNull()
    {
        var entity = new DummyEntity();

        var expr = new MemberInitQueryExpression(typeof(int?), Array.Empty<(string MemberName, QueryExpression Value)>());
        await Assert.That(ExpressionEvaluator.EvaluateValue(expr, entity)).IsNull();
    }

    [Test]
    public async Task Evaluate_UnaryBooleanExpression_ShouldCoverBranches_ForEntityAndDocument()
    {
        var entity = new DummyEntity();
        var doc = new BsonDocument();

        var notTrue = new UnaryExpression(ExpressionType.Not, new ConstantExpression(true), typeof(bool));
        var notFalse = new UnaryExpression(ExpressionType.Not, new ConstantExpression(false), typeof(bool));
        var convertIntToInt = new UnaryExpression(ExpressionType.Convert, new ConstantExpression(1), typeof(int));
        var convertNull = new UnaryExpression(ExpressionType.Convert, new ConstantExpression(null), typeof(int));

        await Assert.That(ExpressionEvaluator.Evaluate(notTrue, entity)).IsFalse();
        await Assert.That(ExpressionEvaluator.Evaluate(notFalse, entity)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(convertIntToInt, entity)).IsFalse();
        await Assert.That(ExpressionEvaluator.Evaluate(convertNull, entity)).IsFalse();

        await Assert.That(ExpressionEvaluator.Evaluate(notTrue, doc)).IsFalse();
        await Assert.That(ExpressionEvaluator.Evaluate(notFalse, doc)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(convertIntToInt, doc)).IsFalse();
        await Assert.That(ExpressionEvaluator.Evaluate(convertNull, doc)).IsFalse();
    }

    [Test]
    public async Task EvaluateValue_UnaryConvert_NumericTypeCheckBranches_ShouldWork()
    {
        var entity = new DummyEntity();

        var intToString = new UnaryExpression(ExpressionType.Convert, new ConstantExpression(123), typeof(string));
        await Assert.That(ExpressionEvaluator.EvaluateValue(intToString, entity)).IsEqualTo("123");

        var stringToInt = new UnaryExpression(ExpressionType.Convert, new ConstantExpression("42"), typeof(int));
        await Assert.That(ExpressionEvaluator.EvaluateValue(stringToInt, entity)).IsEqualTo(42);
    }

    [Test]
    public async Task Evaluate_MemberExpression_BooleanAndNullBranches_ForEntity()
    {
        var entity = new DummyEntity { FlagTrue = true, FlagFalse = false, MaybeNull = null, Name = "ok" };

        await Assert.That(ExpressionEvaluator.Evaluate(new MemberExpression(nameof(DummyEntity.FlagTrue)), entity)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(new MemberExpression(nameof(DummyEntity.FlagFalse)), entity)).IsFalse();
        await Assert.That(ExpressionEvaluator.Evaluate(new MemberExpression(nameof(DummyEntity.Name)), entity)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(new MemberExpression(nameof(DummyEntity.MaybeNull)), entity)).IsFalse();
    }

    [Test]
    public async Task Evaluate_MemberExpression_BsonDocument_KeyLookup_ToCamelCase_And_IdMapping_ShouldWork()
    {
        var docCamel = new BsonDocument()
            .Set("name", "Alice")
            .Set("flag", true)
            .Set("flagFalse", false);

        await Assert.That(ExpressionEvaluator.Evaluate(new MemberExpression("Flag"), docCamel)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(new MemberExpression("FlagFalse"), docCamel)).IsFalse();
        await Assert.That(ExpressionEvaluator.Evaluate(new MemberExpression("Missing"), docCamel)).IsFalse();
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Name"), docCamel)).IsEqualTo("Alice");

        var docExact = new BsonDocument().Set("Name", "Bob");
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Name"), docExact)).IsEqualTo("Bob");

        var docId = new BsonDocument().Set("_id", 123);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Id"), docId)).IsEqualTo(123);

        var docEmpty = new BsonDocument().Set("", 7);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression(""), docEmpty)).IsEqualTo(7);

        var docLen1 = new BsonDocument().Set("a", 9);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("A"), docLen1)).IsEqualTo(9);
    }

    [Test]
    public async Task EvaluateValue_MemberExpression_TargetNull_ShouldReturnNull()
    {
        var expr = new MemberExpression("Anything", new ConstantExpression(null));
        await Assert.That(ExpressionEvaluator.EvaluateValue(expr, new object())).IsNull();
    }

    [Test]
    public async Task EvaluateValue_GetMemberValueFromTarget_CountBranches_ShouldWork()
    {
        var entity = new DummyEntity();

        var list = new List<int> { 1, 2, 3, 4 };
        var listCountExpr = new MemberExpression("Count", new ConstantExpression(list));
        await Assert.That(ExpressionEvaluator.EvaluateValue(listCountExpr, entity)).IsEqualTo(4);

        var enumerableOnly = new EnumerableOnly();
        var enumerableCountExpr = new MemberExpression("Count", new ConstantExpression(enumerableOnly));
        await Assert.That(ExpressionEvaluator.EvaluateValue(enumerableCountExpr, entity)).IsEqualTo(3);

        var bsonArray = new BsonArray(new BsonValue[] { 1, 2, 3 });
        var bsonArrayCountExpr = new MemberExpression("Count", new ConstantExpression(bsonArray));
        await Assert.That(ExpressionEvaluator.EvaluateValue(bsonArrayCountExpr, entity)).IsEqualTo(3);

        BsonValue wrappedArray = new BsonArrayValue(bsonArray);
        var wrappedArrayCountExpr = new MemberExpression("Count", new ConstantExpression(wrappedArray));
        await Assert.That(ExpressionEvaluator.EvaluateValue(wrappedArrayCountExpr, entity)).IsEqualTo(3);

        var grouping = new QueryPipeline.AotGrouping("key", new object[] { 1, 2 });
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Key", new ConstantExpression(grouping)), entity)).IsEqualTo("key");
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Count", new ConstantExpression(grouping)), entity)).IsEqualTo(2);
    }

    [Test]
    public async Task EvaluateValue_GetMemberValueFromTarget_ReflectionFallback_ShouldHandleFoundAndMissing()
    {
        var entity = new DummyEntity { Value = 10 };
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression(nameof(DummyEntity.Value)), (object)entity)).IsEqualTo(10);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("NoSuchProperty"), (object)entity)).IsNull();
    }

    [Test]
    public async Task Evaluate_BinaryAndAlsoOrElse_ShouldShortCircuit_AndCoverBranches()
    {
        var entity = new DummyEntity();

        var unknown = new UnknownQueryExpression();

        // Generic (entity) evaluation
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.AndAlso, new ConstantExpression(false), unknown), entity)).IsFalse();
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.AndAlso, new ConstantExpression(true), new ConstantExpression(false)), entity)).IsFalse();
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.AndAlso, new ConstantExpression(true), new ConstantExpression(true)), entity)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.AndAlso, new ConstantExpression(1), new ConstantExpression(true)), entity)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.AndAlso, new ConstantExpression(1), new ConstantExpression("x")), entity)).IsFalse();

        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.OrElse, new ConstantExpression(true), unknown), entity)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.OrElse, new ConstantExpression(false), new ConstantExpression(true)), entity)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.OrElse, new ConstantExpression(false), new ConstantExpression(false)), entity)).IsFalse();
        await Assert.That(ExpressionEvaluator.Evaluate(new BinaryExpression(ExpressionType.OrElse, new ConstantExpression(1), new ConstantExpression(true)), entity)).IsTrue();

        // Object (non-generic) evaluation
        await Assert.That((bool)ExpressionEvaluator.EvaluateValue(new BinaryExpression(ExpressionType.AndAlso, new ConstantExpression(false), unknown), new object())!).IsFalse();
        await Assert.That((bool)ExpressionEvaluator.EvaluateValue(new BinaryExpression(ExpressionType.OrElse, new ConstantExpression(true), unknown), new object())!).IsTrue();
    }

    [Test]
    public async Task EvaluateValue_BinaryAdd_StringConcatenation_WithNulls_ShouldWork()
    {
        var entity = new DummyEntity();

        var leftNull = new BinaryExpression(ExpressionType.Add, new ConstantExpression(null), new ConstantExpression("b"));
        await Assert.That(ExpressionEvaluator.EvaluateValue(leftNull, entity)).IsEqualTo("b");

        var rightNull = new BinaryExpression(ExpressionType.Add, new ConstantExpression("a"), new ConstantExpression(null));
        await Assert.That(ExpressionEvaluator.EvaluateValue(rightNull, entity)).IsEqualTo("a");
    }

    [Test]
    public async Task EvaluateValue_Function_EnumerableAndMathAndToStringBranches_ShouldWork()
    {
        var entity = new DummyEntity();

        var bsonArray = new BsonArray(new BsonValue[] { 1, 2, 3 });
        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("Count", new ConstantExpression(bsonArray), Array.Empty<QueryExpression>()), entity)).IsEqualTo(3);

        var grouping = new QueryPipeline.AotGrouping("key", new object[] { 1, 2, 3 });
        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("Sum", new ConstantExpression(grouping), Array.Empty<QueryExpression>()), entity)).IsEqualTo(6m);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("Max", new ConstantExpression(grouping), Array.Empty<QueryExpression>()), entity)).IsEqualTo(3);

        var items = new[] { new Item { Value = 1 }, new Item { Value = 2 } };
        var selector = (Expression<Func<Item, int>>)(x => x.Value);
        var sumWithSelector = new FunctionExpression("Sum", new ConstantExpression(items), new QueryExpression[] { new ConstantExpression(selector) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(sumWithSelector, entity)).IsEqualTo(3m);

        var roundOneArg = new FunctionExpression("Round", null, new QueryExpression[] { new ConstantExpression(1.4) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(roundOneArg, entity)).IsEqualTo(1d);

        var absDecimal128 = new FunctionExpression("Abs", null, new QueryExpression[] { new ConstantExpression(new Decimal128(-1.25m)) });
        var absResult = (Decimal128)ExpressionEvaluator.EvaluateValue(absDecimal128, entity)!;
        await Assert.That(absResult.ToDecimal()).IsEqualTo(1.25m);

        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("Pow", null, new QueryExpression[] { new ConstantExpression(2), new ConstantExpression(3) }), entity)).IsEqualTo(8d);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("Pow", null, new QueryExpression[] { new ConstantExpression(2) }), entity)).IsEqualTo(0d);

        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("Sqrt", null, new QueryExpression[] { new ConstantExpression(9) }), entity)).IsEqualTo(3d);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("Sqrt", null, Array.Empty<QueryExpression>()), entity)).IsEqualTo(0d);

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(new FunctionExpression("Min", null, new QueryExpression[] { new ConstantExpression(1) }), entity)).Throws<ArgumentException>();
        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("Min", null, new QueryExpression[] { new ConstantExpression(null), new ConstantExpression(2) }), entity)).IsEqualTo(0d);

        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("ToString", new ConstantExpression(123), Array.Empty<QueryExpression>()), entity)).IsEqualTo("123");
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(new FunctionExpression("Nope", new ConstantExpression(123), Array.Empty<QueryExpression>()), entity)).Throws<NotSupportedException>();
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(new FunctionExpression("Round", null, new QueryExpression[] { new ConstantExpression(1), new ConstantExpression(2), new ConstantExpression(3) }), entity)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task EvaluateValue_Function_DateTimeBranches_ShouldWork()
    {
        var entity = new DummyEntity();

        var dt = new DateTime(2026, 2, 3, 12, 34, 56, DateTimeKind.Utc);
        var addDays = new FunctionExpression("AddDays", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) });
        var dtPlus1 = (DateTime)ExpressionEvaluator.EvaluateValue(addDays, entity)!;
        await Assert.That(dtPlus1.Day).IsEqualTo(4);

        var getMonth = new MemberExpression("Month", new ConstantExpression(dt));
        await Assert.That(ExpressionEvaluator.EvaluateValue(getMonth, entity)).IsEqualTo(2);

        var getDayOfWeek = new MemberExpression("DayOfWeek", new ConstantExpression(dt));
        await Assert.That(ExpressionEvaluator.EvaluateValue(getDayOfWeek, entity)).IsEqualTo((int)dt.DayOfWeek);

        var getUnknown = new MemberExpression("NoSuchPart", new ConstantExpression(dt));
        await Assert.That(ExpressionEvaluator.EvaluateValue(getUnknown, entity)).IsNull();

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(new FunctionExpression("Nope", new ConstantExpression(dt), Array.Empty<QueryExpression>()), entity)).Throws<NotSupportedException>();
    }
}

