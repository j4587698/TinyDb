using System;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using ConstantExpression = TinyDb.Query.ConstantExpression;

namespace TinyDb.Tests.Query;

public sealed class ExpressionEvaluatorAdditionalFunctionCoverageTests
{
    [Test]
    public async Task EvaluateValue_StringFunctions_HappyPaths_ShouldWork()
    {
        var doc = new BsonDocument();

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Contains", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression("ell") }),
                doc))
            .IsEqualTo(true);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("StartsWith", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression("he") }),
                doc))
            .IsEqualTo(true);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("EndsWith", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression("lo") }),
                doc))
            .IsEqualTo(true);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("ToLower", new ConstantExpression("ABC"), Array.Empty<QueryExpression>()),
                doc))
            .IsEqualTo("abc");

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("ToUpper", new ConstantExpression("abc"), Array.Empty<QueryExpression>()),
                doc))
            .IsEqualTo("ABC");

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Trim", new ConstantExpression("  hi  "), Array.Empty<QueryExpression>()),
                doc))
            .IsEqualTo("hi");
    }

    [Test]
    public async Task EvaluateValue_StringSubstring_Replace_And_Unsupported_ShouldCoverBranches()
    {
        var doc = new BsonDocument();

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Substring", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression(2) }),
                doc))
            .IsEqualTo("llo");

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Substring", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression(1), new ConstantExpression(3) }),
                doc))
            .IsEqualTo("ell");

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Replace", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression("l"), new ConstantExpression("x") }),
                doc))
            .IsEqualTo("hexxo");

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Replace", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression(1) }),
                doc))
            .IsEqualTo("hello");

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("PadLeft", new ConstantExpression("x"), new QueryExpression[] { new ConstantExpression(10) }),
                doc))
            .ThrowsExactly<NotSupportedException>();
    }

    [Test]
    public async Task EvaluateValue_StringFunctions_InvalidArgs_ShouldCoverBranches()
    {
        var doc = new BsonDocument();

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Contains", new ConstantExpression("hello"), Array.Empty<QueryExpression>()),
                doc))
            .IsEqualTo(false);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("StartsWith", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression(123) }),
                doc))
            .IsEqualTo(false);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("EndsWith", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression(123) }),
                doc))
            .IsEqualTo(false);

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Substring", new ConstantExpression("hello"), Array.Empty<QueryExpression>()),
                doc))
            .ThrowsExactly<ArgumentException>();
    }

    [Test]
    public async Task EvaluateValue_MathFunctions_ShouldCoverBranches()
    {
        var doc = new BsonDocument();

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Abs", null, new QueryExpression[] { new ConstantExpression(-1) }),
                doc))
            .IsEqualTo(1);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Ceiling", null, new QueryExpression[] { new ConstantExpression(1.2) }),
                doc))
            .IsEqualTo(2d);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Floor", null, new QueryExpression[] { new ConstantExpression(1.8) }),
                doc))
            .IsEqualTo(1d);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Min", null, new QueryExpression[] { new ConstantExpression(2), new ConstantExpression(1) }),
                doc))
            .IsEqualTo(1d);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Max", null, new QueryExpression[] { new ConstantExpression(2), new ConstantExpression(1) }),
                doc))
            .IsEqualTo(2d);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Pow", null, new QueryExpression[] { new ConstantExpression(2), new ConstantExpression(3) }),
                doc))
            .IsEqualTo(8d);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Sqrt", null, new QueryExpression[] { new ConstantExpression(9) }),
                doc))
            .IsEqualTo(3d);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Round", null, new QueryExpression[] { new ConstantExpression(1.6) }),
                doc))
            .IsEqualTo(2d);
    }

    [Test]
    public async Task EvaluateValue_MathRound_TwoArgs_And_InvalidArgs_ShouldCoverBranches()
    {
        var doc = new BsonDocument();

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Round", null, new QueryExpression[] { new ConstantExpression(1.234), new ConstantExpression(2) }),
                doc))
            .IsEqualTo(1.23d);

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Round", null, new QueryExpression[] { new ConstantExpression(1.234), new ConstantExpression(2), new ConstantExpression(3) }),
                doc))
            .ThrowsExactly<NotSupportedException>();
    }

    [Test]
    public async Task EvaluateValue_EnumerableFunctions_ShouldCoverContainsCountAndAggregates()
    {
        var doc = new BsonDocument();

        var numbers = new[] { 1, 2, 3 };
        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Contains", null, new QueryExpression[] { new ConstantExpression(numbers), new ConstantExpression(2) }),
                doc))
            .IsEqualTo(true);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Count", null, new QueryExpression[] { new ConstantExpression(numbers) }),
                doc))
            .IsEqualTo(3);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Sum", null, new QueryExpression[] { new ConstantExpression(numbers) }),
                doc))
            .IsEqualTo(6m);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Average", null, new QueryExpression[] { new ConstantExpression(numbers) }),
                doc))
            .IsEqualTo(2m);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Min", null, new QueryExpression[] { new ConstantExpression(numbers) }),
                doc))
            .IsEqualTo(1);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Max", null, new QueryExpression[] { new ConstantExpression(numbers) }),
                doc))
            .IsEqualTo(3);

        var empty = Array.Empty<int>();
        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Average", null, new QueryExpression[] { new ConstantExpression(empty) }),
                doc))
            .IsEqualTo(0m);
        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Min", null, new QueryExpression[] { new ConstantExpression(empty) }),
                doc))
            .IsNull();
        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Max", null, new QueryExpression[] { new ConstantExpression(empty) }),
                doc))
            .IsNull();
    }

    [Test]
    public async Task EvaluateValue_EnumerableCount_NonCollection_ShouldEnumerate()
    {
        var doc = new BsonDocument();

        var seq = Enumerable.Range(1, 3).Select(x => x);
        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Count", null, new QueryExpression[] { new ConstantExpression(seq) }),
                doc))
            .IsEqualTo(3);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("Contains", null, new QueryExpression[] { new ConstantExpression(seq), new ConstantExpression(99) }),
                doc))
            .IsEqualTo(false);
    }

    [Test]
    public async Task EvaluateValue_DateTimeFunctions_Remaining_ShouldWork()
    {
        var doc = new BsonDocument();
        var dt = new DateTime(2020, 1, 1, 10, 20, 30, DateTimeKind.Utc);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("AddDays", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }),
                doc))
            .IsEqualTo(dt.AddDays(1));

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("AddHours", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }),
                doc))
            .IsEqualTo(dt.AddHours(1));

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("AddMinutes", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }),
                doc))
            .IsEqualTo(dt.AddMinutes(1));

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("AddSeconds", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }),
                doc))
            .IsEqualTo(dt.AddSeconds(1));

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("AddYears", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }),
                doc))
            .IsEqualTo(dt.AddYears(1));

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("AddMonths", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }),
                doc))
            .IsEqualTo(dt.AddMonths(1));

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("ToString", new ConstantExpression(123), Array.Empty<QueryExpression>()),
                doc))
            .IsEqualTo("123");

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(
                new FunctionExpression("AddMilliseconds", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }),
                doc))
            .ThrowsExactly<NotSupportedException>();
    }

    [Test]
    public async Task EvaluateValue_DateTimeMemberAccess_ShouldCoverBranches()
    {
        var doc = new BsonDocument();
        var dt = new DateTime(2021, 2, 3, 4, 5, 6, DateTimeKind.Utc);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new TinyDb.Query.MemberExpression("Year", new ConstantExpression(dt)),
                doc))
            .IsEqualTo(2021);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new TinyDb.Query.MemberExpression("Month", new ConstantExpression(dt)),
                doc))
            .IsEqualTo(2);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new TinyDb.Query.MemberExpression("Day", new ConstantExpression(dt)),
                doc))
            .IsEqualTo(3);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new TinyDb.Query.MemberExpression("Hour", new ConstantExpression(dt)),
                doc))
            .IsEqualTo(4);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new TinyDb.Query.MemberExpression("Minute", new ConstantExpression(dt)),
                doc))
            .IsEqualTo(5);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new TinyDb.Query.MemberExpression("Second", new ConstantExpression(dt)),
                doc))
            .IsEqualTo(6);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new TinyDb.Query.MemberExpression("Date", new ConstantExpression(dt)),
                doc))
            .IsEqualTo(dt.Date);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new TinyDb.Query.MemberExpression("DayOfWeek", new ConstantExpression(dt)),
                doc))
            .IsEqualTo((int)dt.DayOfWeek);

        await Assert.That(ExpressionEvaluator.EvaluateValue(
                new TinyDb.Query.MemberExpression("UnknownMember", new ConstantExpression(dt)),
                doc))
            .IsNull();
    }
}
