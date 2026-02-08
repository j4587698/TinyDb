using TinyDb.Bson;
using TinyDb.Metadata;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public class MetadataDocumentAotAdapterAdditionalCoverageTests
{
    [Test]
    public async Task GetPropertyValue_ShouldReturnAllKnownProperties()
    {
        var createdAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var updatedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc);

        var entity = new MetadataDocument
        {
            Id = ObjectId.NewObjectId(),
            TypeName = "T",
            CollectionName = "C",
            DisplayName = "D",
            Description = "Desc",
            PropertiesJson = "[]",
            CreatedAt = createdAt,
            UpdatedAt = updatedAt
        };

        await Assert.That(AotBsonMapper.GetPropertyValue(entity, nameof(MetadataDocument.Id))).IsNotNull();
        await Assert.That(AotBsonMapper.GetPropertyValue(entity, nameof(MetadataDocument.TypeName))).IsEqualTo("T");
        await Assert.That(AotBsonMapper.GetPropertyValue(entity, nameof(MetadataDocument.CollectionName))).IsEqualTo("C");
        await Assert.That(AotBsonMapper.GetPropertyValue(entity, nameof(MetadataDocument.DisplayName))).IsEqualTo("D");
        await Assert.That(AotBsonMapper.GetPropertyValue(entity, nameof(MetadataDocument.Description))).IsEqualTo("Desc");
        await Assert.That(AotBsonMapper.GetPropertyValue(entity, nameof(MetadataDocument.PropertiesJson))).IsEqualTo("[]");
        await Assert.That(AotBsonMapper.GetPropertyValue(entity, nameof(MetadataDocument.CreatedAt))).IsEqualTo(createdAt);
        await Assert.That(AotBsonMapper.GetPropertyValue(entity, nameof(MetadataDocument.UpdatedAt))).IsEqualTo(updatedAt);
    }

    [Test]
    public async Task HasValidId_ShouldReflectNullDefaultAndSetId()
    {
        await Assert.That(AotIdAccessor<MetadataDocument>.HasValidId(null!)).IsFalse();

        var entity = new MetadataDocument();
        await Assert.That(AotIdAccessor<MetadataDocument>.HasValidId(entity)).IsTrue();

        var id = new BsonObjectId(ObjectId.NewObjectId());
        AotBsonMapper.SetId(entity, id);

        await Assert.That(AotIdAccessor<MetadataDocument>.HasValidId(entity)).IsTrue();
    }
}
