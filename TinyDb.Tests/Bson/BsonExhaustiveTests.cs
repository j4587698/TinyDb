using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonExhaustiveTests
{
    [Test]
    public async Task BsonDocument_Full_IDictionary_Implementation_ShouldWork()
    {
        var doc = new BsonDocument();
        doc = doc.Set("a", 1).Set("b", 2);

        IDictionary<string, BsonValue> idoc = doc;
        await Assert.That(idoc.Count).IsEqualTo(2);
        await Assert.That(idoc.Keys.Count).IsEqualTo(2);
        await Assert.That(idoc.Values.Count).IsEqualTo(2);

        await Assert.That(idoc.ContainsKey("a")).IsTrue();

        BsonValue val;
        await Assert.That(idoc.TryGetValue("b", out val!)).IsTrue();
        await Assert.That(val.ToInt32(null)).IsEqualTo(2);

        await Assert.That(doc.Get("a").ToInt32(null)).IsEqualTo(1);
        await Assert.That(doc.Get("nonexistent", 42).ToInt32(null)).IsEqualTo(42);

        await Assert.That(() => idoc["c"] = 3).Throws<NotSupportedException>();
        var kvp = new KeyValuePair<string, BsonValue>("c", 3);
        await Assert.That(() => idoc.Add(kvp)).Throws<NotSupportedException>();
        await Assert.That(() => idoc.Remove("a")).Throws<NotSupportedException>();
        await Assert.That(() => idoc.Clear()).Throws<NotSupportedException>();

        // Test non-dictionary methods
        var doc3 = doc.RemoveKey("a");
        await Assert.That(doc3.ContainsKey("a")).IsFalse();

        // Test enumerator
        int count = 0;
        foreach (var item in idoc)
        {
            count++;
        }
        await Assert.That(count).IsEqualTo(2);
    }

    [Test]
    public async Task BsonArray_Full_IList_Implementation_ShouldWork()
    {
        IList<BsonValue> arr = new BsonArray(new BsonValue[] { 10, 20 });

        await Assert.That(arr.Count).IsEqualTo(2);
        await Assert.That(arr[0].ToInt32(null)).IsEqualTo(10);
        await Assert.That(arr.Contains(20)).IsTrue();
        await Assert.That(arr.IndexOf(20)).IsEqualTo(1);

        var bsonArr = (BsonArray)arr;
        await Assert.That(bsonArr.Get(0).ToInt32(null)).IsEqualTo(10);

        await Assert.That(() => arr.Add(30)).Throws<NotSupportedException>();
        await Assert.That(() => arr.Insert(0, 5)).Throws<NotSupportedException>();
        await Assert.That(() => arr.Remove(10)).Throws<NotSupportedException>();
        await Assert.That(() => arr.RemoveAt(0)).Throws<NotSupportedException>();
        await Assert.That(() => arr.Clear()).Throws<NotSupportedException>();

        BsonValue[] copy = new BsonValue[2];
        arr.CopyTo(copy, 0);
        await Assert.That(copy[0].ToInt32(null)).IsEqualTo(10);

        var arr2 = bsonArr.RemoveAtValue(0);
        await Assert.That(arr2.Count).IsEqualTo(1);
        await Assert.That(arr2[0].ToInt32(null)).IsEqualTo(20);
    }

    [Test]
    public async Task BsonValue_IConvertible_EdgeCases_ShouldWork()
    {
        // Test all common types for remaining IConvertible methods
        BsonValue str = "true";
        await Assert.That(str.ToBoolean(null)).IsTrue();

        BsonValue num = 123;
        await Assert.That(num.ToByte(null)).IsEqualTo((byte)123);
        await Assert.That(num.ToInt16(null)).IsEqualTo((short)123);
        await Assert.That(num.ToUInt16(null)).IsEqualTo((ushort)123);
        await Assert.That(num.ToUInt32(null)).IsEqualTo((uint)123);
        await Assert.That(num.ToUInt64(null)).IsEqualTo((ulong)123);
        await Assert.That(num.ToSByte(null)).IsEqualTo((sbyte)123);
        await Assert.That(num.ToChar(null)).IsEqualTo((char)123);

        BsonValue dbl = 45.67;
        await Assert.That(dbl.ToSingle(null)).IsEqualTo(45.67f);

        // Test ToType
        await Assert.That(num.ToType(typeof(int), null)).IsEqualTo(123);
        await Assert.That(str.ToType(typeof(string), null)).IsEqualTo("true");
    }

    [Test]
    public async Task BsonDocument_Create_ShouldWork()
    {
        var doc = BsonDocument.Create("key", "value");
        await Assert.That(doc.Count).IsEqualTo(1);
        await Assert.That(doc["key"].ToString()).IsEqualTo("value");
    }

    [Test]
    public async Task BsonArray_Create_ShouldWork()
    {
        var arr = BsonArray.Create(42);
        await Assert.That(arr.Count).IsEqualTo(1);
        await Assert.That(arr[0].ToInt32(null)).IsEqualTo(42);
    }

    [Test]
    public async Task BsonBinary_SubTypes_ShouldWork()
    {
        var data = new byte[] { 1, 2, 3 };
        foreach (BsonBinary.BinarySubType subType in Enum.GetValues<BsonBinary.BinarySubType>())
        {
            var bin = new BsonBinary(data, subType);
            await Assert.That(bin.SubType).IsEqualTo(subType);
            await Assert.That(bin.Bytes.ToArray().SequenceEqual(data)).IsTrue();
            await Assert.That(bin.ToString()).Contains(subType.ToString());
        }

        var bin2 = new BsonBinary(Guid.NewGuid());
        await Assert.That(bin2.SubType).IsEqualTo(BsonBinary.BinarySubType.Uuid);

        BsonBinary implicitBin = new byte[] { 4, 5 };
        byte[] implicitBytes = implicitBin;
        await Assert.That(implicitBytes.Length).IsEqualTo(2);
    }

    [Test]
    public async Task BsonBinary_IConvertible_ShouldThrow()
    {
        IConvertible bin = new BsonBinary(new byte[0]);
        await Assert.That(() => bin.ToBoolean(null)).Throws<InvalidCastException>();
        await Assert.That(() => bin.ToInt32(null)).Throws<InvalidCastException>();
        await Assert.That(bin.ToString(null)).Contains("Binary");
    }

    [Test]
    public async Task BsonDocument_IConvertible_ShouldWork()
    {
        IConvertible doc = new BsonDocument().Set("a", 1);
        await Assert.That(doc.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(doc.ToString(null)).Contains("{");
        await Assert.That(doc.ToBoolean(null)).IsTrue();
        await Assert.That(doc.ToInt32(null)).IsEqualTo(1);
    }

    [Test]
    public async Task BsonArray_IConvertible_ShouldWork()
    {
        IConvertible arr = new BsonArray();
        await Assert.That(arr.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(arr.ToString(null)).IsEqualTo("[]");
        await Assert.That(arr.ToBoolean(null)).IsFalse();
        await Assert.That(arr.ToInt32(null)).IsEqualTo(0);
    }

    [Test]
    public async Task BsonValue_IsHelpers_ShouldWork()
    {
        BsonValue v = 1;
        await Assert.That(v.IsNumeric).IsTrue();
        await Assert.That(v.IsDocument).IsFalse();
        await Assert.That(v.IsArray).IsFalse();
        await Assert.That(v.IsString).IsFalse();
        await Assert.That(v.IsBoolean).IsFalse();
        await Assert.That(v.IsObjectId).IsFalse();
        await Assert.That(v.IsDateTime).IsFalse();
        await Assert.That(v.IsNull).IsFalse();

        v = BsonNull.Value;
        await Assert.That(v.IsNull).IsTrue();
    }
}
