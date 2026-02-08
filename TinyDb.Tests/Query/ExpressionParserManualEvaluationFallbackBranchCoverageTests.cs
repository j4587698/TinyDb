using System;
using System.Linq.Expressions;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class ExpressionParserManualEvaluationFallbackBranchCoverageTests
{
    private readonly ExpressionParser _parser = new();

    private sealed class Container
    {
        public int Number;
    }

    [Test]
    public async Task ParseExpression_MemberOnConstantContainer_ShouldEvaluate()
    {
        var container = new Container { Number = 123 };
        Expression<Func<int>> expr = () => container.Number;

        var parsed = _parser.ParseExpression(expr.Body);
        await Assert.That(parsed).IsTypeOf<TinyDb.Query.ConstantExpression>();

        var constant = (TinyDb.Query.ConstantExpression)parsed;
        await Assert.That(constant.Value).IsEqualTo(123);
    }

    [Test]
    public async Task ParseExpression_ToStringOnConstant_ShouldOptimize()
    {
        Expression<Func<string>> expr = () => 123.ToString();

        var parsed = (TinyDb.Query.ConstantExpression)_parser.ParseExpression(expr.Body);
        await Assert.That(parsed.Value).IsEqualTo("123");
    }

    [Test]
    public async Task ParseExpression_ManualEvaluationUnsupportedOperators_ShouldFallbackToCompile()
    {
        var dt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var ts = TimeSpan.FromMinutes(1);

        Expression<Func<DateTime>> addExpr = () => dt + ts;
        var addParsed = (TinyDb.Query.ConstantExpression)_parser.ParseExpression(addExpr.Body);
        await Assert.That(addParsed.Value).IsEqualTo(dt + ts);

        Expression<Func<DateTime>> subExpr = () => dt - ts;
        var subParsed = (TinyDb.Query.ConstantExpression)_parser.ParseExpression(subExpr.Body);
        await Assert.That(subParsed.Value).IsEqualTo(dt - ts);

        Expression<Func<TimeSpan>> mulExpr = () => ts * 2.0;
        var mulParsed = (TinyDb.Query.ConstantExpression)_parser.ParseExpression(mulExpr.Body);
        await Assert.That(mulParsed.Value).IsEqualTo(ts * 2.0);
    }
}
