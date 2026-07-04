using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using LinqExpressionType = System.Linq.Expressions.ExpressionType;
using QueryBinaryExpression = TinyDb.Query.BinaryExpression;
using QueryConstantExpression = TinyDb.Query.ConstantExpression;
using QueryMemberExpression = TinyDb.Query.MemberExpression;

namespace TinyDb.Tests.Query;

public class QueryPredicateAnalyzerTests
{
    private sealed class NullToString
    {
        public override string? ToString() => null;
    }

    private sealed class ThrowingToString
    {
        public override string ToString() => throw new InvalidOperationException("boom");
    }

    [Test]
    public async Task ExtractConstantValue_ShouldIgnoreUnsupportedExpressions()
    {
        var memberResult = QueryPredicateAnalyzer.ExtractConstantValue(new QueryMemberExpression("Age"));
        await Assert.That(memberResult).IsNull();

        var binaryResult = QueryPredicateAnalyzer.ExtractConstantValue(
            new QueryBinaryExpression(
                LinqExpressionType.Equal,
                new QueryConstantExpression(1),
                new QueryConstantExpression(2)));
        await Assert.That(binaryResult).IsNull();

        var throwingResult = QueryPredicateAnalyzer.ExtractConstantValue(new QueryConstantExpression(new ThrowingToString()));
        await Assert.That(throwingResult).IsNull();
    }

    [Test]
    public async Task ExtractComparisonType_ShouldFallbackToEqualForUnsupportedOrMissingField()
    {
        var unsupported = QueryPredicateAnalyzer.ExtractComparisonType(
            new QueryBinaryExpression(
                LinqExpressionType.Add,
                new QueryMemberExpression("Age"),
                new QueryConstantExpression(1)),
            "Age");
        await Assert.That(unsupported).IsEqualTo(ComparisonType.Equal);

        var missingField = QueryPredicateAnalyzer.ExtractComparisonType(
            new QueryBinaryExpression(
                LinqExpressionType.Equal,
                new QueryConstantExpression(1),
                new QueryConstantExpression(2)),
            "Age");
        await Assert.That(missingField).IsEqualTo(ComparisonType.Equal);
    }

    [Test]
    public async Task ConvertToBsonValue_ShouldHandleFallbackAndPassthrough()
    {
        var fallback = QueryPredicateAnalyzer.ConvertToBsonValue(new NullToString());
        await Assert.That(fallback).IsTypeOf<BsonString>();
        await Assert.That(((BsonString)fallback!).Value).IsEqualTo(string.Empty);

        var input = new BsonInt32(123);
        var result = QueryPredicateAnalyzer.ConvertToBsonValue(input);

        await Assert.That(ReferenceEquals(result, input)).IsTrue();
    }

    [Test]
    public async Task ExtractComparisonMap_ShouldConvertBooleanMemberPredicates()
    {
        var positive = QueryPredicateAnalyzer.ExtractComparisonMap(new QueryMemberExpression("IsActive"));
        var positiveKey = positive["IsActive"].ToIndexScanKey("IsActive");
        await Assert.That(positiveKey.ComparisonType).IsEqualTo(ComparisonType.Equal);
        await Assert.That(positiveKey.Value).IsEqualTo(BsonBoolean.True);

        var negative = QueryPredicateAnalyzer.ExtractComparisonMap(
            new UnaryExpression(
                LinqExpressionType.Not,
                new QueryMemberExpression("IsActive"),
                typeof(bool)));
        var negativeKey = negative["IsActive"].ToIndexScanKey("IsActive");
        await Assert.That(negativeKey.ComparisonType).IsEqualTo(ComparisonType.Equal);
        await Assert.That(negativeKey.Value).IsEqualTo(BsonBoolean.False);
    }

    [Test]
    public async Task TryBuildDisjunctiveClauses_ShouldBuildDnfBranches()
    {
        var expression = new QueryBinaryExpression(
            LinqExpressionType.OrElse,
            new QueryBinaryExpression(
                LinqExpressionType.Equal,
                new QueryMemberExpression("Age"),
                new QueryConstantExpression(18)),
            new QueryBinaryExpression(
                LinqExpressionType.Equal,
                new QueryMemberExpression("Age"),
                new QueryConstantExpression(21)));

        var built = QueryPredicateAnalyzer.TryBuildDisjunctiveClauses(expression, 16, out var clauses);

        await Assert.That(built).IsTrue();
        await Assert.That(clauses.Count).IsEqualTo(2);
    }
}
