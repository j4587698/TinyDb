using System;
using System.IO;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class BsonSerializerExhaustiveTests
{
    [Test]
    public async Task BsonSerializer_AllBsonTypes_ShouldWork()
    {
        var doc = new BsonDocument();
        doc = doc.Set("double", 3.14);
        doc = doc.Set("string", "test");
        doc = doc.Set("doc", new BsonDocument().Set("x", 1));
        doc = doc.Set("array", new BsonArray(new BsonValue[] { 1, 2 }));
        doc = doc.Set("binary", new BsonBinary(new byte[] { 1, 2 }));
        doc = doc.Set("oid", ObjectId.NewObjectId());
        doc = doc.Set("bool", true);
        doc = doc.Set("date", DateTime.UtcNow);
        doc = doc.Set("null", BsonNull.Value);
        doc = doc.Set("regex", new BsonRegularExpression("abc", "i"));
        doc = doc.Set("int32", 123);
        doc = doc.Set("ts", new BsonTimestamp(12345L));
        doc = doc.Set("int64", 1234567890L);
        doc = doc.Set("dec128", 123.45m);
        doc = doc.Set("min", BsonMinKey.Value);
        doc = doc.Set("max", BsonMaxKey.Value);

        var bytes = BsonSerializer.Serialize(doc);
        await Assert.That(bytes).IsNotNull();
        
        var backValue = BsonSerializer.Deserialize(bytes);
        var back = backValue as BsonDocument;
        await Assert.That(back).IsNotNull();
        
        await Assert.That(back!.Count).IsEqualTo(doc.Count);
        foreach (var key in doc.Keys)
        {
            await Assert.That(back.ContainsKey(key)).IsTrue();
            await Assert.That(back[key].BsonType).IsEqualTo(doc[key].BsonType);
        }
    }

    [Test]
    public async Task BsonSerializer_EmptyDocument_ShouldWork()
    {
        var doc = new BsonDocument();
        var bytes = BsonSerializer.Serialize(doc);
        var backValue = BsonSerializer.Deserialize(bytes);
        var back = backValue as BsonDocument;
        await Assert.That(back).IsNotNull();
        await Assert.That(back!.Count).IsEqualTo(0);
    }
}
