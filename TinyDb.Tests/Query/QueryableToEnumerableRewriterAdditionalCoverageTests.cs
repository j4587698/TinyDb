using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryableToEnumerableRewriterAdditionalCoverageTests
{
    [UnconditionalSuppressMessage("Trimming", "IL2026", Justification = "This test intentionally verifies that an internal type name is absent from the assembly metadata.")]
    private static bool ContainsType(Assembly assembly, string fullTypeName)
    {
        return assembly.GetType(fullTypeName, throwOnError: false) != null;
    }

    private static bool ContainsDeclaredMethod(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.NonPublicMethods)] Type type,
        string methodName)
    {
        var methods = type.GetMethods(
            BindingFlags.Public |
            BindingFlags.NonPublic |
            BindingFlags.Static |
            BindingFlags.Instance |
            BindingFlags.DeclaredOnly);

        foreach (var method in methods)
        {
            if (string.Equals(method.Name, methodName, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    [Test]
    public async Task ExecuteInMemory_ShouldBeRemoved_InAotOnlyMode()
    {
        await Assert.That(ContainsDeclaredMethod(typeof(QueryPipeline), "ExecuteInMemory")).IsFalse();
    }

    [Test]
    public async Task QueryableToEnumerableRewriter_ShouldBeRemoved_InAotOnlyMode()
    {
        var asm = typeof(ExpressionParser).Assembly;
        await Assert.That(ContainsType(asm, "TinyDb.Query.QueryableToEnumerableRewriter")).IsFalse();
    }

    [Test]
    public async Task QueryableToEnumerableRewriter_Visit_ShouldNotExist_InAotOnlyMode()
    {
        var asm = typeof(ExpressionParser).Assembly;
        await Assert.That(ContainsType(asm, "TinyDb.Query.QueryableToEnumerableRewriter")).IsFalse();
    }
}
