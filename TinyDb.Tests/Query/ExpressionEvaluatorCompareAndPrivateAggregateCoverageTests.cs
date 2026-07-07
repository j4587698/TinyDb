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

}
