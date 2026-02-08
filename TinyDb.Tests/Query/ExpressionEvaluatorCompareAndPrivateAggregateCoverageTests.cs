using System.Reflection;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class ExpressionEvaluatorCompareAndPrivateAggregateCoverageTests
{
    private sealed class Item
    {
        public string Category { get; set; } = "";
    }

    [Test]
    public async Task EvaluateValue_StringEquality_ShouldUseComparerPath()
    {
        var parser = new ExpressionParser();
        var expr = (System.Linq.Expressions.Expression<Func<Item, bool>>)(x => x.Category == "B");
        var parsed = parser.ParseExpression(expr.Body);

        await Assert.That(ExpressionEvaluator.EvaluateValue<Item>(parsed, new Item { Category = "B" })).IsEqualTo(true);
        await Assert.That(ExpressionEvaluator.EvaluateValue<Item>(parsed, new Item { Category = "A" })).IsEqualTo(false);
    }

    [Test]
    public async Task EvaluateEnumerableAggregate_WithUnsupportedFunctionName_ShouldReturnNull()
    {
        var method = typeof(ExpressionEvaluator).GetMethod(
            "EvaluateEnumerableAggregate",
            BindingFlags.NonPublic | BindingFlags.Static);

        await Assert.That(method == null).IsFalse();

        var selector = (Func<object, object>)(x => x);

        var aotGroup = new QueryPipeline.AotGrouping("k", new object[] { 1, 2 });
        var r1 = method!.Invoke(null, new object?[] { "Median", aotGroup, selector });
        await Assert.That(r1).IsNull();

        var r2 = method.Invoke(null, new object?[] { "Median", new[] { 1, 2 }, selector });
        await Assert.That(r2).IsNull();
    }
}

