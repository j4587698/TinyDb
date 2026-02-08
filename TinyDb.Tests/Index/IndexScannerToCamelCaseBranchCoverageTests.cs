using System.Reflection;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

public class IndexScannerToCamelCaseBranchCoverageTests
{
    [Test]
    public async Task ToCamelCase_ShouldCoverNullEmptySingleAndMultiChar()
    {
        var method = typeof(IndexScanner).GetMethod("ToCamelCase", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        await Assert.That((string?)method!.Invoke(null, new object?[] { null })).IsNull();
        await Assert.That((string)method.Invoke(null, new object?[] { "" })!).IsEqualTo("");
        await Assert.That((string)method.Invoke(null, new object?[] { "A" })!).IsEqualTo("a");
        await Assert.That((string)method.Invoke(null, new object?[] { "Abc" })!).IsEqualTo("abc");
    }
}

