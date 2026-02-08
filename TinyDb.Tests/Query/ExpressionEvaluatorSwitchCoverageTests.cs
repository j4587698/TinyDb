using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using QueryConstantExpression = TinyDb.Query.ConstantExpression;
using QueryFunctionExpression = TinyDb.Query.FunctionExpression;
using QueryMemberExpression = TinyDb.Query.MemberExpression;

namespace TinyDb.Tests.Query;

public class ExpressionEvaluatorSwitchCoverageTests
{
    private sealed class Entity
    {
        public DateTime When { get; set; }
        public string Text { get; set; } = "";
    }

    [Test]
    public async Task EvaluateValue_DateTimeMemberAccess_ShouldSupportAllKnownMembers_AndReturnNullForUnknown()
    {
        var entity = new Entity
        {
            When = new DateTime(2024, 1, 2, 3, 4, 5)
        };

        var expected = new Dictionary<string, object?>
        {
            ["Year"] = 2024,
            ["Month"] = 1,
            ["Day"] = 2,
            ["Hour"] = 3,
            ["Minute"] = 4,
            ["Second"] = 5,
            ["Date"] = new DateTime(2024, 1, 2),
            ["DayOfWeek"] = (int)entity.When.DayOfWeek
        };

        foreach (var (member, value) in expected)
        {
            var expr = new QueryMemberExpression(member, new QueryMemberExpression(nameof(Entity.When)));
            await Assert.That(ExpressionEvaluator.EvaluateValue(expr, entity)).IsEqualTo(value);
        }

        var unknown = new QueryMemberExpression("UnknownMember", new QueryMemberExpression(nameof(Entity.When)));
        await Assert.That(ExpressionEvaluator.EvaluateValue(unknown, entity)).IsNull();
    }

    [Test]
    public async Task EvaluateValue_StringFunctions_ShouldCoverAllSupportedBranches_AndThrowForUnsupported()
    {
        var entity = new Entity
        {
            Text = "  AbcDef  "
        };

        var textMember = new QueryMemberExpression(nameof(Entity.Text));

        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Contains", textMember, new[] { new QueryConstantExpression("Abc") }), entity)).IsEqualTo(true);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Contains", textMember, new[] { new QueryConstantExpression(123) }), entity)).IsEqualTo(false);

        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("StartsWith", textMember, new[] { new QueryConstantExpression("  A") }), entity)).IsEqualTo(true);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("EndsWith", textMember, new[] { new QueryConstantExpression("  ") }), entity)).IsEqualTo(true);

        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("ToLower", textMember, Array.Empty<QueryExpression>()), entity)).IsEqualTo("  abcdef  ");
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("ToUpper", textMember, Array.Empty<QueryExpression>()), entity)).IsEqualTo("  ABCDEF  ");
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Trim", textMember, Array.Empty<QueryExpression>()), entity)).IsEqualTo("AbcDef");

        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Substring", textMember, new[] { new QueryConstantExpression(2) }), entity)).IsEqualTo("AbcDef  ");
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Substring", textMember, new[] { new QueryConstantExpression(2), new QueryConstantExpression(3) }), entity)).IsEqualTo("Abc");

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(
                new QueryFunctionExpression("Substring", textMember, new QueryExpression[] { new QueryConstantExpression(1), new QueryConstantExpression(1), new QueryConstantExpression(1) }),
                entity))
            .ThrowsExactly<ArgumentException>();

        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Replace", textMember, new[] { new QueryConstantExpression("Abc"), new QueryConstantExpression("X") }), entity)).IsEqualTo("  XDef  ");
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Replace", textMember, new[] { new QueryConstantExpression("Abc"), new QueryConstantExpression(123) }), entity)).IsEqualTo("  AbcDef  ");

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("NotSupportedStringFunction", textMember, Array.Empty<QueryExpression>()), entity))
            .ThrowsExactly<NotSupportedException>();
    }

    [Test]
    public async Task EvaluateValue_EnumerableAggregates_And_MathAndDateTimeFunctions_ShouldCoverMissingBranches()
    {
        var entity = new Entity
        {
            When = new DateTime(2024, 1, 2, 3, 4, 5)
        };

        // Enumerable aggregates (target is IEnumerable)
        var numbers = new[] { 1, 2, 3 };
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Sum", new QueryConstantExpression(numbers), Array.Empty<QueryExpression>()), entity)).IsEqualTo(6m);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Average", new QueryConstantExpression(Array.Empty<int>()), Array.Empty<QueryExpression>()), entity)).IsEqualTo(0m);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Min", new QueryConstantExpression(numbers), Array.Empty<QueryExpression>()), entity)).IsEqualTo(1);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Max", new QueryConstantExpression(numbers), Array.Empty<QueryExpression>()), entity)).IsEqualTo(3);

        // Math-like functions (targetValue == null path)
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Abs", null, new[] { new QueryConstantExpression(-3) }), entity)).IsEqualTo(3);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Ceiling", null, new[] { new QueryConstantExpression(1.1) }), entity)).IsEqualTo(2.0);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Floor", null, new[] { new QueryConstantExpression(1.9) }), entity)).IsEqualTo(1.0);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Round", null, new[] { new QueryConstantExpression(1.4) }), entity)).IsEqualTo(1.0);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Round", null, new[] { new QueryConstantExpression(1.2345), new QueryConstantExpression(2) }), entity)).IsEqualTo(1.23);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Min", null, new[] { new QueryConstantExpression(null), new QueryConstantExpression(1) }), entity)).IsEqualTo(0.0);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Pow", null, new[] { new QueryConstantExpression(2), new QueryConstantExpression(3) }), entity)).IsEqualTo(8.0);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Pow", null, new[] { new QueryConstantExpression(2) }), entity)).IsEqualTo(0.0);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Sqrt", null, new[] { new QueryConstantExpression(9) }), entity)).IsEqualTo(3.0);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("Sqrt", null, Array.Empty<QueryExpression>()), entity)).IsEqualTo(0.0);

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(
                new QueryFunctionExpression("Round", null, new QueryExpression[] { new QueryConstantExpression(1.0), new QueryConstantExpression(1), new QueryConstantExpression(1) }),
                entity))
            .ThrowsExactly<NotSupportedException>();

        // DateTime functions
        var whenMember = new QueryMemberExpression(nameof(Entity.When));
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("AddDays", whenMember, new[] { new QueryConstantExpression(1.0) }), entity)).IsEqualTo(entity.When.AddDays(1));
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("AddYears", whenMember, new[] { new QueryConstantExpression(1) }), entity)).IsEqualTo(entity.When.AddYears(1));
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("AddMonths", whenMember, new[] { new QueryConstantExpression(1) }), entity)).IsEqualTo(entity.When.AddMonths(1));
        await Assert.That(ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("ToString", whenMember, Array.Empty<QueryExpression>()), entity)).IsEqualTo(entity.When.ToString());

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(new QueryFunctionExpression("AddMilliseconds", whenMember, new[] { new QueryConstantExpression(1.0) }), entity))
            .ThrowsExactly<NotSupportedException>();
    }
}

