using System;
using System.Collections;
using System.Collections.Generic;
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

namespace TinyDb.Tests.Query;

public class ExpressionEvaluatorAdditionalBranchTests
{
    private sealed class TestEntity
    {
        public string Name { get; set; } = "";
        public DateTime Created { get; set; }
    }

    private enum Status
    {
        A = 0,
        B = 1
    }

    private sealed class ProjectionTarget
    {
        public Status Status { get; set; }
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

    [Test]
    public async Task Evaluate_UnknownQueryExpression_ShouldThrow()
    {
        var expr = new UnknownQueryExpression();
        var entity = new TestEntity();
        var doc = new BsonDocument();

        await Assert.That(() => ExpressionEvaluator.Evaluate(expr, entity)).Throws<NotSupportedException>();
        await Assert.That(() => ExpressionEvaluator.Evaluate(expr, doc)).Throws<NotSupportedException>();

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(expr, entity)).Throws<NotSupportedException>();
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(expr, doc)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task Evaluate_Conditional_TestNull_ShouldReturnFalse()
    {
        var entity = new TestEntity();

        var expr = new ConditionalQueryExpression(
            new ConstantExpression(null),
            new ConstantExpression(true),
            new ConstantExpression(false));

        await Assert.That(ExpressionEvaluator.Evaluate(expr, entity)).IsFalse();
    }

    [Test]
    public async Task Evaluate_Conditional_TestNotBool_ShouldThrow()
    {
        var entity = new TestEntity();

        var expr = new ConditionalQueryExpression(
            new ConstantExpression(1),
            new ConstantExpression(true),
            new ConstantExpression(false));

        await Assert.That(() => ExpressionEvaluator.Evaluate(expr, entity)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task EvaluateValue_MemberInit_EnumConversionFallback_ShouldWork()
    {
        var entity = new TestEntity();

        var expr = new MemberInitQueryExpression(
            typeof(ProjectionTarget),
            new List<(string MemberName, QueryExpression Value)>
            {
                (nameof(ProjectionTarget.Status), new ConstantExpression(1))
            });

        var result = (ProjectionTarget)ExpressionEvaluator.EvaluateValue(expr, entity)!;
        await Assert.That(result.Status).IsEqualTo(Status.B);
    }

    [Test]
    public async Task EvaluateValue_String_Substring_InvalidArgCount_ShouldThrow()
    {
        var entity = new TestEntity();

        var expr = new FunctionExpression(
            "Substring",
            new ConstantExpression("abcdef"),
            new QueryExpression[]
            {
                new ConstantExpression(1),
                new ConstantExpression(2),
                new ConstantExpression(3)
            });

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(expr, entity)).Throws<ArgumentException>();
    }

    [Test]
    public async Task EvaluateValue_String_Replace_WrongArgs_ShouldReturnOriginal()
    {
        var entity = new TestEntity();

        var expr = new FunctionExpression(
            "Replace",
            new ConstantExpression("banana"),
            new QueryExpression[]
            {
                new ConstantExpression("a")
            });

        await Assert.That(ExpressionEvaluator.EvaluateValue(expr, entity)).IsEqualTo("banana");
    }

    [Test]
    public async Task EvaluateValue_String_UnknownFunction_ShouldThrow()
    {
        var entity = new TestEntity();

        var expr = new FunctionExpression(
            "Nope",
            new ConstantExpression("x"),
            Array.Empty<QueryExpression>());

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(expr, entity)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task EvaluateValue_Math_Round_TwoArgs_ShouldWork()
    {
        var entity = new TestEntity();

        var expr = new FunctionExpression(
            "Round",
            null,
            new QueryExpression[]
            {
                new ConstantExpression(10.55m),
                new ConstantExpression(1)
            });

        await Assert.That(ExpressionEvaluator.EvaluateValue(expr, entity)).IsEqualTo(10.6m);
    }

    [Test]
    public async Task EvaluateValue_Math_Abs_InvalidArgCount_ShouldThrow()
    {
        var entity = new TestEntity();

        var expr = new FunctionExpression(
            "Abs",
            null,
            Array.Empty<QueryExpression>());

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(expr, entity)).Throws<ArgumentException>();
    }

    [Test]
    public async Task EvaluateValue_Enumerable_Count_TargetInArgs_ShouldWork()
    {
        var entity = new TestEntity();
        var enumerable = new EnumerableOnly();

        var expr = new FunctionExpression(
            "Count",
            null,
            new QueryExpression[]
            {
                new ConstantExpression(enumerable)
            });

        await Assert.That(ExpressionEvaluator.EvaluateValue(expr, entity)).IsEqualTo(3);
    }

    [Test]
    public async Task EvaluateValue_DateTime_MemberAccess_ShouldWork()
    {
        var entity = new TestEntity { Created = new DateTime(2026, 2, 3, 12, 34, 56, DateTimeKind.Utc) };

        var yearExpr = new MemberExpression("Year", new MemberExpression(nameof(TestEntity.Created), null));
        var dayExpr = new MemberExpression("Day", new MemberExpression(nameof(TestEntity.Created), null));

        await Assert.That(ExpressionEvaluator.EvaluateValue(yearExpr, entity)).IsEqualTo(2026);
        await Assert.That(ExpressionEvaluator.EvaluateValue(dayExpr, entity)).IsEqualTo(3);
    }

    [Test]
    public async Task Evaluate_Compare_IncompatibleIComparable_ShouldNotThrow()
    {
        var entity = new TestEntity();

        var expr = new BinaryExpression(
            ExpressionType.GreaterThan,
            new ConstantExpression(1),
            new ConstantExpression("a"));

        await Assert.That(ExpressionEvaluator.Evaluate(expr, entity)).IsFalse();
    }

    [Test]
    public async Task Evaluate_Compare_ByteArray_LengthAndContent_ShouldWork()
    {
        var entity = new TestEntity();

        var lenDiff = new BinaryExpression(
            ExpressionType.Equal,
            new ConstantExpression(new byte[] { 1 }),
            new ConstantExpression(new byte[] { 1, 2 }));

        var contentDiff = new BinaryExpression(
            ExpressionType.Equal,
            new ConstantExpression(new byte[] { 1, 2, 3 }),
            new ConstantExpression(new byte[] { 1, 9, 3 }));

        await Assert.That(ExpressionEvaluator.Evaluate(lenDiff, entity)).IsFalse();
        await Assert.That(ExpressionEvaluator.Evaluate(contentDiff, entity)).IsFalse();
    }
}

