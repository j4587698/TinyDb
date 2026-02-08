using System;
using System.Collections.Generic;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExpressionAdditionalCoverageTests
{
    [Test]
    public async Task FunctionExpression_NullArguments_ShouldCreateEmptyList()
    {
        var expr = new FunctionExpression("f", (QueryExpression?)null, (IEnumerable<QueryExpression>)null!);

        await Assert.That(expr.Arguments.Count).IsEqualTo(0);
    }

    [Test]
    public async Task ConstructorExpression_NullArguments_ShouldCreateEmptyList()
    {
        var expr = new ConstructorExpression(typeof(int), null!);

        await Assert.That(expr.Arguments.Count).IsEqualTo(0);
    }

    [Test]
    public async Task MemberInitQueryExpression_NullBindings_ShouldCreateEmptyList()
    {
        var expr = new MemberInitQueryExpression(typeof(int), null!);

        await Assert.That(expr.Bindings.Count).IsEqualTo(0);
    }

    [Test]
    public async Task ConditionalQueryExpression_NodeType_ShouldBeConditional()
    {
        var expr = new ConditionalQueryExpression(
            new TinyDb.Query.ConstantExpression(true),
            new TinyDb.Query.ConstantExpression(1),
            new TinyDb.Query.ConstantExpression(2));

        await Assert.That(expr.NodeType).IsEqualTo(System.Linq.Expressions.ExpressionType.Conditional);
    }
}
