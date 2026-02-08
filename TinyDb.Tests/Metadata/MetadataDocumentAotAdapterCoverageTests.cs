using TinyDb.Bson;
using TinyDb.Metadata;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Metadata;

public class MetadataDocumentAotAdapterCoverageTests
{
    [Test]
    public async Task AotAdapter_MetadataDocument_RoundTrip_And_PropertyAccess_ShouldWork()
    {
        var id = ObjectId.NewObjectId();
        var entity = new MetadataDocument
        {
            Id = id,
            TypeName = "T",
            CollectionName = "C",
            DisplayName = "D",
            Description = "Desc",
            PropertiesJson = "[]",
            CreatedAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            UpdatedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc)
        };

        var bson = AotBsonMapper.ToDocument(entity);
        await Assert.That(bson.ContainsKey("_id")).IsTrue();

        var roundtrip = AotBsonMapper.FromDocument<MetadataDocument>(bson);
        await Assert.That(roundtrip.TypeName).IsEqualTo("T");
        await Assert.That(roundtrip.CollectionName).IsEqualTo("C");

        var typeName = AotBsonMapper.GetPropertyValue(entity, nameof(MetadataDocument.TypeName));
        await Assert.That(typeName).IsEqualTo("T");

        var missing = AotBsonMapper.GetPropertyValue(entity, "__missing__");
        await Assert.That(missing).IsNull();

        var newId = new BsonObjectId(ObjectId.NewObjectId());
        AotBsonMapper.SetId(entity, newId);
        await Assert.That(entity.Id).IsEqualTo((ObjectId)newId);

        var idValue = AotBsonMapper.GetId(entity);
        await Assert.That(idValue.IsNull).IsFalse();
    }

    [Test]
    public async Task FromDocument_WithMissingFields_ShouldStillReturnInstance()
    {
        var bson = new BsonDocument()
            .Set("_id", new BsonObjectId(ObjectId.NewObjectId()))
            .Set("UnknownField", 1);

        var entity = AotBsonMapper.FromDocument<MetadataDocument>(bson);
        await Assert.That(entity).IsNotNull();
        await Assert.That(entity.TypeName).IsNotNull();
    }
}

