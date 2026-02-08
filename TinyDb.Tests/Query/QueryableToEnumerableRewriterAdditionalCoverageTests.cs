using System.Reflection;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryableToEnumerableRewriterAdditionalCoverageTests
{
    [Test]
    public async Task ExecuteInMemory_ShouldBeRemoved_InAotOnlyMode()
    {
        var asm = typeof(ExpressionParser).Assembly;
        var queryPipelineType = asm.GetType("TinyDb.Query.QueryPipeline", throwOnError: false);
        await Assert.That(queryPipelineType).IsNotNull();
        var executeInMemory = queryPipelineType.GetMethod("ExecuteInMemory", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(executeInMemory).IsNull();
    }

    [Test]
    public async Task QueryableToEnumerableRewriter_ShouldBeRemoved_InAotOnlyMode()
    {
        var asm = typeof(ExpressionParser).Assembly;
        var rewriterType = asm.GetType("TinyDb.Query.QueryableToEnumerableRewriter", throwOnError: false);
        await Assert.That(rewriterType).IsNull();
    }

    [Test]
    public async Task QueryableToEnumerableRewriter_Visit_ShouldNotExist_InAotOnlyMode()
    {
        var asm = typeof(ExpressionParser).Assembly;
        var rewriterType = asm.GetType("TinyDb.Query.QueryableToEnumerableRewriter", throwOnError: false);
        await Assert.That(rewriterType).IsNull();
    }
}
