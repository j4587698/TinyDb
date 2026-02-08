using System.Collections;
using System.Collections.Generic;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonDocumentAdditionalCoverageTests
{
    [Test]
    public async Task JsonRendering_ShouldCover_AllValueKinds()
    {
        var oid = ObjectId.NewObjectId();
        var dt = DateTime.UtcNow;

        var doc = new BsonDocument()
            .Set("str", new BsonString("a\"b"))
            .Set("bool", new BsonBoolean(true))
            .Set("null", BsonNull.Value)
            .Set("doc", new BsonDocument().Set("x", 1))
            .Set("arr", BsonArray.FromList(new List<object?> { 1, "x" }))
            .Set("oid", new BsonObjectId(oid))
            .Set("dt", new BsonDateTime(dt));

        var json = doc.ToString();
        await Assert.That(json).Contains("\"str\": \"a\\\"b\"");
        await Assert.That(json).Contains("\"bool\": true");
        await Assert.That(json).Contains("\"null\": null");
        await Assert.That(json).Contains("\"doc\":");
        await Assert.That(json).Contains("\"arr\":");
        await Assert.That(json).Contains("\"$oid\"");
        await Assert.That(json).Contains("\"$date\"");
    }

    [Test]
    public async Task CollectionInterfaces_ShouldExercise_ExplicitMembers()
    {
        var doc = new BsonDocument().Set("a", 1);

        await Assert.That(doc.Values.Count()).IsEqualTo(1);

        await Assert.That(() => doc.Add(new KeyValuePair<string, BsonValue>("b", 2))).Throws<NotSupportedException>();

        var enumerator = ((IEnumerable)doc).GetEnumerator();
        await Assert.That(enumerator.MoveNext()).IsTrue();

        var ro = (IReadOnlyDictionary<string, BsonValue>)doc;
        await Assert.That(ro.Keys.Count()).IsEqualTo(1);
        await Assert.That(ro.Values.Count()).IsEqualTo(1);
    }

    [Test]
    public async Task FromDictionary_UnsupportedType_ShouldThrow()
    {
        var dict = new Dictionary<string, object?>
        {
            ["x"] = Guid.NewGuid()
        };

        await Assert.That(() => BsonDocument.FromDictionary(dict)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task ToDictionary_UnknownBsonValue_ShouldUseRawValue()
    {
        var doc = new BsonDocument().Set("re", new BsonRegularExpression("ab"));
        var back = doc.ToDictionary();

        await Assert.That(back["re"]).IsEqualTo("ab");
    }

    [Test]
    public async Task ToType_ShouldSupport_Primitives_AndThrow_ForObjects()
    {
        var doc = new BsonDocument().Set("a", 1).Set("b", 2);

        await Assert.That((int)doc.ToType(typeof(int), null)).IsEqualTo(2);

        var asDoc = doc.ToType(typeof(BsonDocument), null);
        await Assert.That(ReferenceEquals(asDoc, doc)).IsTrue();

        var asDict = (Dictionary<string, object?>)doc.ToType(typeof(Dictionary<string, object?>), null);
        await Assert.That(asDict.Count).IsEqualTo(2);

        await Assert.That(() => doc.ToType(typeof(object), null)).Throws<InvalidCastException>();
    }
}

