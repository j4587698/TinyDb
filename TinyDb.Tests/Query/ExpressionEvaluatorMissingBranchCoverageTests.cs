using System;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using BinaryExpression = TinyDb.Query.BinaryExpression;
using ConstantExpression = TinyDb.Query.ConstantExpression;
using MemberExpression = TinyDb.Query.MemberExpression;

namespace TinyDb.Tests.Query;

public class ExpressionEvaluatorMissingBranchCoverageTests
{
    [Test]
    public async Task Evaluate_BsonDocument_AndAlsoOrElse_ShouldCoverNonBooleanRightBranches()
    {
        var doc = new BsonDocument()
            .Set("boolTrue", true)
            .Set("boolFalse", false)
            .Set("val", 123);

        var andRightNonBool = new BinaryExpression(
            ExpressionType.AndAlso,
            new MemberExpression("boolTrue", null),
            new MemberExpression("val", null));
        await Assert.That(ExpressionEvaluator.EvaluateValue(andRightNonBool, doc)).IsEqualTo(false);

        var andRightFalse = new BinaryExpression(
            ExpressionType.AndAlso,
            new MemberExpression("boolTrue", null),
            new MemberExpression("boolFalse", null));
        await Assert.That(ExpressionEvaluator.EvaluateValue(andRightFalse, doc)).IsEqualTo(false);

        var orLeftTrueShortCircuit = new BinaryExpression(
            ExpressionType.OrElse,
            new MemberExpression("boolTrue", null),
            new MemberExpression("val", null));
        await Assert.That(ExpressionEvaluator.EvaluateValue(orLeftTrueShortCircuit, doc)).IsEqualTo(true);

        var orRightFalse = new BinaryExpression(
            ExpressionType.OrElse,
            new MemberExpression("boolFalse", null),
            new MemberExpression("boolFalse", null));
        await Assert.That(ExpressionEvaluator.EvaluateValue(orRightFalse, doc)).IsEqualTo(false);

        var orRightNonBool = new BinaryExpression(
            ExpressionType.OrElse,
            new MemberExpression("boolFalse", null),
            new MemberExpression("val", null));
        await Assert.That(ExpressionEvaluator.EvaluateValue(orRightNonBool, doc)).IsEqualTo(false);

        var orLeftNonBool = new BinaryExpression(
            ExpressionType.OrElse,
            new MemberExpression("val", null),
            new MemberExpression("boolTrue", null));
        await Assert.That(ExpressionEvaluator.EvaluateValue(orLeftNonBool, doc)).IsEqualTo(true);
    }

    [Test]
    public async Task EvaluateValue_DateTimeMemberAccess_ShouldCoverAllKnownMembers()
    {
        var doc = new BsonDocument();
        var dt = new DateTime(2020, 2, 3, 4, 5, 6, DateTimeKind.Utc);

        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Year", new ConstantExpression(dt)), doc)).IsEqualTo(2020);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Month", new ConstantExpression(dt)), doc)).IsEqualTo(2);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Day", new ConstantExpression(dt)), doc)).IsEqualTo(3);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Hour", new ConstantExpression(dt)), doc)).IsEqualTo(4);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Minute", new ConstantExpression(dt)), doc)).IsEqualTo(5);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Second", new ConstantExpression(dt)), doc)).IsEqualTo(6);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Date", new ConstantExpression(dt)), doc)).IsEqualTo(dt.Date);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("DayOfWeek", new ConstantExpression(dt)), doc)).IsEqualTo((int)dt.DayOfWeek);
        await Assert.That(ExpressionEvaluator.EvaluateValue(new MemberExpression("Unknown", new ConstantExpression(dt)), doc)).IsNull();
    }

    [Test]
    public async Task EvaluateValue_StringStartsWithEndsWith_InvalidArgs_ShouldReturnFalse()
    {
        var doc = new BsonDocument();

        var startsWrongType = new FunctionExpression("StartsWith", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression(123) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(startsWrongType, doc)).IsEqualTo(false);

        var startsWrongCount = new FunctionExpression("StartsWith", new ConstantExpression("hello"), Array.Empty<QueryExpression>());
        await Assert.That(ExpressionEvaluator.EvaluateValue(startsWrongCount, doc)).IsEqualTo(false);

        var endsWrongType = new FunctionExpression("EndsWith", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression(123) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(endsWrongType, doc)).IsEqualTo(false);

        var endsWrongCount = new FunctionExpression("EndsWith", new ConstantExpression("hello"), new QueryExpression[] { new ConstantExpression("x"), new ConstantExpression("y") });
        await Assert.That(ExpressionEvaluator.EvaluateValue(endsWrongCount, doc)).IsEqualTo(false);
    }

    [Test]
    public async Task EvaluateValue_MathRound_TwoArgsAndInvalidArity_ShouldCoverBranches()
    {
        var doc = new BsonDocument();

        var roundTwoArgs = new FunctionExpression("Round", null, new QueryExpression[] { new ConstantExpression(1.234), new ConstantExpression(2) });
        await Assert.That(ExpressionEvaluator.EvaluateValue(roundTwoArgs, doc)).IsEqualTo(1.23d);

        var roundInvalid = new FunctionExpression("Round", null, new QueryExpression[] { new ConstantExpression(1.0), new ConstantExpression(2), new ConstantExpression(3) });
        await Assert.That(() => ExpressionEvaluator.EvaluateValue(roundInvalid, doc)).ThrowsExactly<NotSupportedException>();
    }

    [Test]
    public async Task EvaluateValue_DateTimeFunctions_ShouldCoverMissingBranches()
    {
        var doc = new BsonDocument();
        var dt = new DateTime(2020, 1, 1, 10, 20, 30, DateTimeKind.Utc);

        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("AddMinutes", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }), doc))
            .IsEqualTo(dt.AddMinutes(1));
        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("AddSeconds", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }), doc))
            .IsEqualTo(dt.AddSeconds(1));
        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("AddMonths", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }), doc))
            .IsEqualTo(dt.AddMonths(1));
        await Assert.That(ExpressionEvaluator.EvaluateValue(new FunctionExpression("ToString", new ConstantExpression(dt), Array.Empty<QueryExpression>()), doc))
            .IsEqualTo(dt.ToString());

        await Assert.That(() => ExpressionEvaluator.EvaluateValue(new FunctionExpression("AddTicks", new ConstantExpression(dt), new QueryExpression[] { new ConstantExpression(1) }), doc))
            .ThrowsExactly<NotSupportedException>();
    }
}
