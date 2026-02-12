using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public class MetadataDocumentCoverageTests
{
    [Test]
    public async Task RoundTrip_ShouldWork()
    {
        var original = new EntityMetadata
        {
            TypeName = "TestType",
            CollectionName = "TestCollection",
            DisplayName = "Test Display",
            Description = "Test Desc"
        };
        original.Properties.Add(new TinyDb.Metadata.PropertyMetadata
        {
            PropertyName = "P1",
            PropertyType = "System.String",
            DisplayName = "D1",
            Required = true,
            Order = 1
        });
        
        var doc = MetadataDocument.FromEntityMetadata(original);
        await Assert.That(doc.TypeName).IsEqualTo("TestType");
        await Assert.That(doc.TableName).IsEqualTo("TestCollection");
        await Assert.That(doc.Description).IsEqualTo("Test Desc");
        
        var restored = doc.ToEntityMetadata();
        await Assert.That(restored.TypeName).IsEqualTo("TestType");
        await Assert.That(restored.CollectionName).IsEqualTo("TestCollection");
        await Assert.That(restored.Description).IsEqualTo("Test Desc");
        await Assert.That(restored.Properties.Count).IsEqualTo(1);
        await Assert.That(restored.Properties[0].PropertyName).IsEqualTo("P1");
    }

    [Test]
    public async Task ToEntityMetadata_EmptyColumns_ShouldWork()
    {
        var doc = new MetadataDocument { TableName = "t", TypeName = "T", DisplayName = "D" };
        var meta = doc.ToEntityMetadata();
        await Assert.That(meta.Properties).IsEmpty();
    }

    [Test]
    public async Task ToEntityMetadata_WhitespaceDescription_ShouldBeNull()
    {
        var doc = new MetadataDocument { TableName = "t", TypeName = "T", DisplayName = "D", Description = " " };
        var meta = doc.ToEntityMetadata();
        await Assert.That(meta.Description).IsNull();
    }
}
