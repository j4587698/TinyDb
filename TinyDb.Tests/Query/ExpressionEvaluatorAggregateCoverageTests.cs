using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using BinaryExpression = TinyDb.Query.BinaryExpression;
using ConditionalQueryExpression = TinyDb.Query.ConditionalQueryExpression;
using ConstantExpression = TinyDb.Query.ConstantExpression;
using ConstructorExpression = TinyDb.Query.ConstructorExpression;
using FunctionExpression = TinyDb.Query.FunctionExpression;
using MemberExpression = TinyDb.Query.MemberExpression;
using MemberInitQueryExpression = TinyDb.Query.MemberInitQueryExpression;
using ParameterExpression = TinyDb.Query.ParameterExpression;

namespace TinyDb.Tests.Query;

public class ExpressionEvaluatorAggregateCoverageTests
{
    public sealed class TestEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public DateTime Created { get; set; }
    }

    [Entity]
    public sealed class OuterEntity
    {
        public InnerEntity Inner { get; set; } = new();
    }

    [Entity]
    public sealed class InnerEntity
    {
        public string Text { get; set; } = "";
    }

    public sealed class Projection
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";

        public Projection()
        {
        }

        public Projection(int id, string name)
        {
            Id = id;
            Name = name;
        }
    }

    private sealed class UnknownQueryExpression : QueryExpression
    {
        public override ExpressionType NodeType => ExpressionType.Extension;
    }

    [Test]
    public async Task EvaluateValue_ConditionalBranches_ShouldWork()
    {
        var entity = new TestEntity { Id = 1, Name = "n" };

        var chooseYes = new ConditionalQueryExpression(
            new ConstantExpression(true),
            new ConstantExpression("yes"),
            new ConstantExpression("no"));

        await Assert.That(ExpressionEvaluator.EvaluateValue(chooseYes, entity)).IsEqualTo("yes");
        await Assert.That(ExpressionEvaluator.EvaluateValue(chooseYes, (object)entity)).IsEqualTo("yes");

        var doc = new BsonDocument().Set("x", 1);
        await Assert.That(ExpressionEvaluator.EvaluateValue(chooseYes, doc)).IsEqualTo("yes");

        var testNull = new ConditionalQueryExpression(
            new ConstantExpression(null),
            new ConstantExpression(1),
            new ConstantExpression(2));
        await Assert.That(ExpressionEvaluator.EvaluateValue(testNull, entity)).IsNull();

        var testNotBool = new ConditionalQueryExpression(
            new ConstantExpression(1),
            new ConstantExpression(1),
            new ConstantExpression(2));
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(testNotBool, entity))
            .Throws<InvalidOperationException>();

        var resultNotBool = new ConditionalQueryExpression(
            new ConstantExpression(true),
            new ConstantExpression(1),
            new ConstantExpression(2));
        await Assert.That(() => ExpressionEvaluator.Evaluate(resultNotBool, entity))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task EvaluateValue_BsonDocument_SwitchArms_ShouldWork()
    {
        var doc = new BsonDocument()
            .Set("id", 42)
            .Set("name", "hello")
            .Set("tags", new BsonArray(new BsonValue[] { "a", "b", "c" }));

        var asParam = (BsonDocument)ExpressionEvaluator.EvaluateValue(new ParameterExpression("d"), doc)!;
        await Assert.That(asParam).IsSameReferenceAs(doc);

        var countTags = new FunctionExpression("Count", new MemberExpression("tags", null), Array.Empty<QueryExpression>());
        await Assert.That(ExpressionEvaluator.EvaluateValue(countTags, doc)).IsEqualTo(3);

        var ctor = new ConstructorExpression(typeof(Projection), new QueryExpression[]
        {
            new ConstantExpression(7),
            new ConstantExpression("ctor")
        });
        var created = (Projection)ExpressionEvaluator.EvaluateValue(ctor, doc)!;
        await Assert.That(created.Id).IsEqualTo(7);
        await Assert.That(created.Name).IsEqualTo("ctor");

        var init = new MemberInitQueryExpression(typeof(Projection), new (string MemberName, QueryExpression Value)[]
        {
            ("Id", new ConstantExpression("123")),
            ("Name", new ConstantExpression("init"))
        });
        var projected = (Projection)ExpressionEvaluator.EvaluateValue(init, doc)!;
        await Assert.That(projected.Id).IsEqualTo(123);
        await Assert.That(projected.Name).IsEqualTo("init");
    }

    [Test]
    public async Task EvaluateValue_ObjectOverload_OrElse_ShouldEvaluateRightWhenNeeded()
    {
        var shortCircuit = new BinaryExpression(
            ExpressionType.OrElse,
            new ConstantExpression(true),
            new ConstantExpression(false));
        await Assert.That(ExpressionEvaluator.EvaluateValue(shortCircuit, new object())).IsEqualTo(true);

        var evaluateRight = new BinaryExpression(
            ExpressionType.OrElse,
            new ConstantExpression(false),
            new ConstantExpression(true));
        await Assert.That(ExpressionEvaluator.EvaluateValue(evaluateRight, new object())).IsEqualTo(true);
    }

    [Test]
    public async Task Evaluate_ShouldThrowOnNonBooleanConstant()
    {
        var entity = new TestEntity();
        await Assert.That(() => ExpressionEvaluator.Evaluate(new ConstantExpression("not-bool"), entity))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task EvaluateValue_ShouldThrowOnUnknownQueryExpression()
    {
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(new UnknownQueryExpression(), new object()))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task Compare_ShouldHandleStringByteArrayAndIncompatibleComparable()
    {
        var entity = new TestEntity();

        var stringLess = new BinaryExpression(ExpressionType.LessThan, new ConstantExpression("a"), new ConstantExpression("b"));
        await Assert.That(ExpressionEvaluator.Evaluate(stringLess, entity)).IsTrue();

        var bytesEqual = new BinaryExpression(
            ExpressionType.Equal,
            new ConstantExpression(new byte[] { 1, 2 }),
            new ConstantExpression(new byte[] { 1, 2 }));
        await Assert.That(ExpressionEvaluator.Evaluate(bytesEqual, entity)).IsTrue();

        var bytesLessByLen = new BinaryExpression(
            ExpressionType.LessThan,
            new ConstantExpression(new byte[] { 1 }),
            new ConstantExpression(new byte[] { 1, 0 }));
        await Assert.That(ExpressionEvaluator.Evaluate(bytesLessByLen, entity)).IsTrue();

        var bytesLessByValue = new BinaryExpression(
            ExpressionType.LessThan,
            new ConstantExpression(new byte[] { 1, 2 }),
            new ConstantExpression(new byte[] { 1, 3 }));
        await Assert.That(ExpressionEvaluator.Evaluate(bytesLessByValue, entity)).IsTrue();

        var incompatibleComparable = new BinaryExpression(ExpressionType.Equal, new ConstantExpression(1), new ConstantExpression("1"));
        await Assert.That(ExpressionEvaluator.Evaluate(incompatibleComparable, entity)).IsTrue();
    }

    [Test]
    public async Task GetMemberValueFromTarget_CountAndMissingMember_ShouldWork()
    {
        var entity = new TestEntity();

        var range = Enumerable.Range(0, 3);
        var countEnumerable = new MemberExpression("Count", new ConstantExpression(range));
        await Assert.That(ExpressionEvaluator.EvaluateValue(countEnumerable, entity)).IsEqualTo(3);

        var wrappedArray = new BsonArrayValue(new BsonArray(new BsonValue[] { 1, 2, 3 }));
        var countWrappedArray = new MemberExpression("Count", new ConstantExpression(wrappedArray));
        await Assert.That(ExpressionEvaluator.EvaluateValue(countWrappedArray, entity)).IsEqualTo(3);

        var doc = new BsonDocument().Set("present", 1);
        var missing = new MemberExpression("missing", null);
        await Assert.That(ExpressionEvaluator.EvaluateValue(missing, doc)).IsNull();

        var unknownDatePart = new MemberExpression("DoesNotExist", new ConstantExpression(DateTime.UnixEpoch));
        await Assert.That(ExpressionEvaluator.EvaluateValue(unknownDatePart, entity)).IsNull();
    }

    [Test]
    public async Task EvaluateFunction_EnumerableAggregates_And_StaticEnumerableCalls_ShouldWork()
    {
        var entity = new TestEntity();

        var items = new[] { 1, 2, 3 };

        var sumNoSelector = new FunctionExpression("Sum", new ConstantExpression(items), Array.Empty<QueryExpression>());
        await Assert.That(ExpressionEvaluator.EvaluateValue(sumNoSelector, entity)).IsEqualTo(6m);

        var avgEmpty = new FunctionExpression("Average", new ConstantExpression(Array.Empty<int>()), Array.Empty<QueryExpression>());
        await Assert.That(ExpressionEvaluator.EvaluateValue(avgEmpty, entity)).IsEqualTo(0m);

        var min = new FunctionExpression("Min", new ConstantExpression(items), Array.Empty<QueryExpression>());
        await Assert.That(ExpressionEvaluator.EvaluateValue(min, entity)).IsEqualTo(1);

        var max = new FunctionExpression("Max", new ConstantExpression(items), Array.Empty<QueryExpression>());
        await Assert.That(ExpressionEvaluator.EvaluateValue(max, entity)).IsEqualTo(3);

        var staticCount = new FunctionExpression("Count", null, new QueryExpression[] { new ConstantExpression(items) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(staticCount, entity)).IsEqualTo(3);

        var absFromString = new FunctionExpression("Abs", null, new QueryExpression[] { new ConstantExpression("9") });
        await Assert.That(ExpressionEvaluator.EvaluateValue(absFromString, entity)).IsEqualTo(9d);

        var minWithNull = new FunctionExpression("Min", null, new QueryExpression[] { new ConstantExpression(null), new ConstantExpression(5) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(minWithNull, entity)).IsEqualTo(0.0);

        var absMissingArg = new FunctionExpression("Abs", null, Array.Empty<QueryExpression>());
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(absMissingArg, entity))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task EvaluateFunction_AotGrouping_MinMax_And_AverageNonEmpty_ShouldWork()
    {
        var entity = new TestEntity();

        var group = new QueryPipeline.AotGrouping("k", new object[] { 3, 1, 2 });
        var minOnGroup = new FunctionExpression("Min", new ConstantExpression(group), Array.Empty<QueryExpression>());
        var maxOnGroup = new FunctionExpression("Max", new ConstantExpression(group), Array.Empty<QueryExpression>());

        await Assert.That(ExpressionEvaluator.EvaluateValue(minOnGroup, entity)).IsEqualTo(1);
        await Assert.That(ExpressionEvaluator.EvaluateValue(maxOnGroup, entity)).IsEqualTo(3);

        var avgNonEmpty = new FunctionExpression("Average", new ConstantExpression(new[] { 1, 2, 3 }), Array.Empty<QueryExpression>());
        await Assert.That(ExpressionEvaluator.EvaluateValue(avgNonEmpty, entity)).IsEqualTo(2m);
    }

    [Test]
    public async Task GetMemberValueFromTarget_BsonArrayCount_AotGroupingCount_And_NestedMemberAccess_ObjectOverload_ShouldWork()
    {
        var entity = new TestEntity();

        var arr = new BsonArray(new BsonValue[] { 1, 2, 3 });
        var countArray = new MemberExpression("Count", new ConstantExpression(arr));
        await Assert.That(ExpressionEvaluator.EvaluateValue(countArray, entity)).IsEqualTo(3);

        var group = new QueryPipeline.AotGrouping("k", new object[] { "a", "b" });
        var countGroup = new MemberExpression("Count", new ConstantExpression(group));
        await Assert.That(ExpressionEvaluator.EvaluateValue(countGroup, entity)).IsEqualTo(2);

        var outer = new OuterEntity { Inner = new InnerEntity { Text = "abc" } };
        var nested = new MemberExpression("Text", new MemberExpression("Inner", new ParameterExpression("o")));
        await Assert.That(ExpressionEvaluator.EvaluateValue(nested, (object)outer)).IsEqualTo("abc");
    }
}
