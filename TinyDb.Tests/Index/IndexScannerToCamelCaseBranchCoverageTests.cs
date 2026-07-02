using System.Reflection;
using TinyDb.Attributes;
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
        await Assert.That((string)method.Invoke(null, new object?[] { "ID" })!).IsEqualTo("id");
        await Assert.That((string)method.Invoke(null, new object?[] { "URLValue" })!).IsEqualTo("urlValue");
        await Assert.That((string)method.Invoke(null, new object?[] { "_Id" })!).IsEqualTo("_id");
    }

    [Test]
    public async Task GetEntityIndexes_ShouldMapIdPropertyToStorageIdField()
    {
        var indexes = IndexScanner.GetEntityIndexes(typeof(IndexedIdEntity));

        await Assert.That(indexes.Count).IsEqualTo(1);
        await Assert.That(indexes[0].Fields[0]).IsEqualTo("_id");
    }

    [Entity]
    public sealed class IndexedIdEntity
    {
        [Index]
        public int ID { get; set; }
    }
}
