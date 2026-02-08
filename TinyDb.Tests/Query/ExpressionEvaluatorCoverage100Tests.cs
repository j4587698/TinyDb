using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class ExpressionEvaluatorCoverage100Tests
{
    [Test]
    public async Task Evaluate_OnBsonDocument_WithConstantBoolean_ShouldReturnConstantValue()
    {
        var doc = new BsonDocument();

        await Assert.That(ExpressionEvaluator.Evaluate(new ConstantExpression(true), doc)).IsTrue();
        await Assert.That(ExpressionEvaluator.Evaluate(new ConstantExpression(false), doc)).IsFalse();
    }
}

