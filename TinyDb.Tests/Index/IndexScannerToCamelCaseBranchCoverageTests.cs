using TinyDb.Attributes;
using TinyDb.Index;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

public class IndexScannerToCamelCaseBranchCoverageTests
{
    [Test]
    public async Task ToCamelCase_ShouldCoverNullEmptySingleAndMultiChar()
    {
        await Assert.That((string?)BsonFieldName.ToCamelCase(null!)).IsNull();
        await Assert.That(BsonFieldName.ToCamelCase("")).IsEqualTo("");
        await Assert.That(BsonFieldName.ToCamelCase("A")).IsEqualTo("a");
        await Assert.That(BsonFieldName.ToCamelCase("Abc")).IsEqualTo("abc");
        await Assert.That(BsonFieldName.ToCamelCase("ID")).IsEqualTo("id");
        await Assert.That(BsonFieldName.ToCamelCase("URLValue")).IsEqualTo("urlValue");
        await Assert.That(BsonFieldName.ToCamelCase("_Id")).IsEqualTo("_id");
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
