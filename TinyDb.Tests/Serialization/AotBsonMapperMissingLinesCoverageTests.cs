using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Metadata;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotBsonMapperMissingLinesCoverageTests
{
    private struct StructWithPropertyAndField
    {
        public int X { get; set; }
        public int Y { get; set; }
    }

    [Test]
    public async Task ConvertValue_Object_ShouldUnwrapKnownBsonTypes()
    {
        var dt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var oid = ObjectId.NewObjectId();

        await Assert.That(AotBsonMapper.ConvertValue(BsonNull.Value, typeof(object))).IsNull();
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDecimal128(12.34m), typeof(object))).IsNotNull();
        await Assert.That(AotBsonMapper.ConvertValue(new BsonDateTime(dt), typeof(object))).IsEqualTo(dt);
        await Assert.That(AotBsonMapper.ConvertValue(new BsonObjectId(oid), typeof(object))).IsEqualTo(oid);

        var arr = new BsonArray(new BsonValue[] { new BsonInt32(1) });
        var fallback = AotBsonMapper.ConvertValue(arr, typeof(object));
        await Assert.That(ReferenceEquals(fallback, arr)).IsTrue();
    }

    [Test]
    public async Task ConvertValue_TargetBsonValue_ShouldReturnOriginalInstance()
    {
        var value = new BsonInt32(123);
        var converted = AotBsonMapper.ConvertValue(value, typeof(BsonValue));

        await Assert.That(ReferenceEquals(converted, value)).IsTrue();
    }

    [Test]
    public async Task ConvertValue_ComplexObject_ShouldUseRegisteredAdapter()
    {
        var entity = new MetadataDocument
        {
            Id = ObjectId.NewObjectId(),
            TypeName = "T",
            CollectionName = "C",
            DisplayName = "D",
            Description = "Desc",
            PropertiesJson = "[]",
            CreatedAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            UpdatedAt = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Utc)
        };

        var doc = AotBsonMapper.ToDocument(entity);
        var converted = (MetadataDocument)AotBsonMapper.ConvertValue(doc, typeof(MetadataDocument))!;

        await Assert.That(converted.TypeName).IsEqualTo("T");
        await Assert.That(converted.CollectionName).IsEqualTo("C");
    }

    [Test]
    public async Task ConvertValue_DictionaryAndCollection_WithRegisteredAdapters_ShouldHitAdapterBranches()
    {
        var bsonDict = new BsonDocument().Set("a", 1).Set("b", 2);
        var dictObj = (Dictionary<string, int>)AotBsonMapper.ConvertValue(bsonDict, typeof(Dictionary<string, int>))!;
        await Assert.That(dictObj["a"]).IsEqualTo(1);

        var bsonArray = new BsonArray(new BsonValue[] { new BsonInt32(1) });
        var listObj = (List<int>)AotBsonMapper.ConvertValue(bsonArray, typeof(List<int>))!;
        await Assert.That(listObj.Count).IsEqualTo(1);
        await Assert.That(listObj[0]).IsEqualTo(1);
    }

    [Test]
    public async Task ConvertValue_InterfaceTargets_ShouldThrow()
    {
        var bsonDict = new BsonDocument().Set("a", 1);
        await Assert.That(() => AotBsonMapper.ConvertValue(bsonDict, typeof(IDictionary<string, int>)))
            .Throws<NotSupportedException>();

        var bsonArray = new BsonArray(new BsonValue[] { new BsonInt32(1), new BsonInt32(2) });
        await Assert.That(() => AotBsonMapper.ConvertValue(bsonArray, typeof(ICollection<int>)))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task FromDocument_UnregisteredType_ShouldThrow()
    {
        var doc = new BsonDocument()
            .Set("x", 10)
            .Set("y", 20);

        await Assert.That(() => AotBsonMapper.FromDocument<StructWithPropertyAndField>(doc))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ReflectionMetadata_PropertyMapGetter_ShouldBeCovered()
    {
        await Assert.That(EntityMetadata<MetadataDocument>.TryGetProperty(nameof(MetadataDocument.TypeName), out _))
            .IsTrue();
    }
}
