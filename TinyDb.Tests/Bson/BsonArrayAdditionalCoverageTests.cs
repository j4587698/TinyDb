using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonArrayAdditionalCoverageTests
{
    [Test]
    public async Task ToString_Should_Format_Known_Bson_Types()
    {
        var oid = ObjectId.NewObjectId();
        var dt = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);

        var nested = new BsonArray(new BsonValue[] { 1 });
        var doc = new BsonDocument().Set("x", 1);

        var array = new BsonArray(new BsonValue[]
        {
            new BsonString("a\"b"),
            new BsonBoolean(true),
            BsonNull.Value,
            doc,
            nested,
            new BsonObjectId(oid),
            new BsonDateTime(dt),
            new BsonInt32(7)
        });

        var s = array.ToString();

        await Assert.That(s).Contains("\"a\\\"b\"");
        await Assert.That(s).Contains("true");
        await Assert.That(s).Contains("null");
        await Assert.That(s).Contains(doc.ToString());
        await Assert.That(s).Contains(nested.ToString());
        await Assert.That(s).Contains("$oid");
        await Assert.That(s).Contains(oid.ToString());
        await Assert.That(s).Contains("$date");
        await Assert.That(s).Contains("2020-01-02T03:04:05.000Z");
    }

    [Test]
    public async Task ToList_Should_Convert_Supported_Elements_And_Expose_RawValue_For_Others()
    {
        var oid = ObjectId.NewObjectId();
        var dt = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
        var bytes = new byte[] { 1, 2, 3 };

        var nested = new BsonArray(new BsonValue[] { "x" });
        var doc = new BsonDocument().Set("a", 1);

        var array = new BsonArray(new BsonValue[]
        {
            new BsonString("s"),
            new BsonInt32(1),
            new BsonInt64(2),
            new BsonDouble(3.5),
            new BsonBoolean(false),
            new BsonDateTime(dt),
            new BsonObjectId(oid),
            doc,
            nested,
            new BsonBinary(bytes)
        });

        var list = array.ToList();

        await Assert.That((string)list[0]!).IsEqualTo("s");
        await Assert.That((int)list[1]!).IsEqualTo(1);
        await Assert.That((long)list[2]!).IsEqualTo(2L);
        await Assert.That((double)list[3]!).IsEqualTo(3.5);
        await Assert.That((bool)list[4]!).IsFalse();
        await Assert.That((DateTime)list[5]!).IsEqualTo(dt);
        await Assert.That((ObjectId)list[6]!).IsEqualTo(oid);

        await Assert.That(list[7]).IsTypeOf<Dictionary<string, object?>>();
        var dict = (Dictionary<string, object?>)list[7]!;
        await Assert.That(dict["a"]).IsEqualTo(1);

        await Assert.That(list[8]).IsTypeOf<List<object?>>();
        var nestedList = (List<object?>)list[8]!;
        await Assert.That(nestedList.Count).IsEqualTo(1);
        await Assert.That(nestedList[0]).IsEqualTo("x");

        await Assert.That(list[9]).IsTypeOf<byte[]>();
        await Assert.That(((byte[])list[9]!).SequenceEqual(bytes)).IsTrue();
    }

    [Test]
    public async Task FromList_ImplicitConversions_And_ToType_Should_Work_And_Throw_When_Unsupported()
    {
        var oid = ObjectId.NewObjectId();
        var dt = DateTime.UnixEpoch;

        var list = new List<object?>
        {
            null,
            "s",
            1,
            2L,
            3.5,
            (float)4.5,
            true,
            dt,
            oid,
            new Dictionary<string, object?> { ["k"] = 1 },
            new List<object?> { "nested" }
        };

        var arr = BsonArray.FromList(list);
        await Assert.That(arr.Count).IsEqualTo(list.Count);
        await Assert.That(arr[0].IsNull).IsTrue();
        await Assert.That(arr[1].IsString).IsTrue();
        await Assert.That(arr[2].BsonType).IsEqualTo(BsonType.Int32);
        await Assert.That(arr[3].BsonType).IsEqualTo(BsonType.Int64);
        await Assert.That(arr[4].BsonType).IsEqualTo(BsonType.Double);
        await Assert.That(arr[5].BsonType).IsEqualTo(BsonType.Double); // float -> double
        await Assert.That(arr[6].IsBoolean).IsTrue();
        await Assert.That(arr[7].IsDateTime).IsTrue();
        await Assert.That(arr[8].IsObjectId).IsTrue();
        await Assert.That(arr[9].IsDocument).IsTrue();
        await Assert.That(arr[10].IsArray).IsTrue();

        BsonArray fromArray = new object?[] { 1, 2L, "x" };
        await Assert.That(fromArray.Count).IsEqualTo(3);

        var asList = (List<object?>)arr.ToType(typeof(List<object?>), null);
        await Assert.That(asList.Count).IsEqualTo(arr.Count);

        var asInt = (int)arr.ToType(typeof(int), null);
        await Assert.That(asInt).IsEqualTo(arr.Count);

        await Assert.That(() => arr.ToType(typeof(Guid), null)).Throws<InvalidCastException>();

        await Assert.That(() => BsonArray.FromList(new List<object?> { new Uri("https://example.com") }))
            .Throws<NotSupportedException>();

        // Ensure explicit non-generic enumerator path is exercised
        var e = ((IEnumerable)arr).GetEnumerator();
        await Assert.That(e).IsNotNull();
    }
}
