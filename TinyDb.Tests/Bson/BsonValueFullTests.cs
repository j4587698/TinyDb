using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonValueFullTests
{
    [Test]
    public async Task BsonInt32_IConvertible_ShouldWork()
    {
        IConvertible val = new BsonInt32(42);
        await Assert.That(val.ToInt32(null)).IsEqualTo(42);
        await Assert.That(val.ToInt64(null)).IsEqualTo(42L);
        await Assert.That(val.ToDouble(null)).IsEqualTo(42.0);
        await Assert.That(val.ToBoolean(null)).IsTrue();
        await Assert.That(val.ToDecimal(null)).IsEqualTo(42m);
        await Assert.That(val.ToByte(null)).IsEqualTo((byte)42);
        await Assert.That(val.ToSingle(null)).IsEqualTo(42.0f);
        await Assert.That(val.ToString(null)).IsEqualTo("42");
    }

    [Test]
    public async Task BsonString_IConvertible_ShouldWork()
    {
        IConvertible val = new BsonString("123");
        await Assert.That(val.ToInt32(null)).IsEqualTo(123);
        await Assert.That(val.ToString(null)).IsEqualTo("123");
    }

    [Test]
    public async Task BsonBoolean_IConvertible_ShouldWork()
    {
        IConvertible val = new BsonBoolean(true);
        await Assert.That(val.ToBoolean(null)).IsTrue();
        await Assert.That(val.ToInt32(null)).IsEqualTo(1);
        
        IConvertible valFalse = new BsonBoolean(false);
        await Assert.That(valFalse.ToInt32(null)).IsEqualTo(0);
    }

    [Test]
    public async Task BsonDouble_IConvertible_ShouldWork()
    {
        IConvertible val = new BsonDouble(3.14);
        await Assert.That(val.ToDouble(null)).IsEqualTo(3.14);
        await Assert.That(val.ToInt32(null)).IsEqualTo(3); // Truncation
    }

    [Test]
    public async Task BsonInt64_IConvertible_ShouldWork()
    {
        IConvertible val = new BsonInt64(1234567890123L);
        await Assert.That(val.ToInt64(null)).IsEqualTo(1234567890123L);
        await Assert.That(val.ToDouble(null)).IsEqualTo(1234567890123.0);
    }

    [Test]
    public async Task BsonDateTime_IConvertible_ShouldWork()
    {
        var now = DateTime.Now;
        IConvertible val = new BsonDateTime(now);
        // Compare with low precision because BSON might truncate milliseconds
        await Assert.That(Math.Abs((val.ToDateTime(null) - now).TotalSeconds)).IsLessThan(1.0);
    }

    [Test]
    public async Task BsonNull_IConvertible_ShouldReturnDefaults()
    {
        IConvertible val = BsonNull.Value;
        await Assert.That(val.ToInt32(null)).IsEqualTo(0);
        await Assert.That(val.ToBoolean(null)).IsFalse();
        await Assert.That(val.ToString(null)).IsEqualTo("null");
    }

    [Test]
    public async Task BsonBinary_ShouldWork()
    {
        var data = new byte[] { 1, 2, 3 };
        var bin = new BsonBinary(data);
        await Assert.That(bin.Bytes.ToArray().SequenceEqual(data)).IsTrue();
        await Assert.That(bin.SubType).IsEqualTo(BsonBinary.BinarySubType.Generic);
        
        var guid = Guid.NewGuid();
        var binGuid = new BsonBinary(guid);
        await Assert.That(binGuid.SubType).IsEqualTo(BsonBinary.BinarySubType.Uuid);
        await Assert.That(new Guid(binGuid.Bytes.ToArray())).IsEqualTo(guid);
    }

    [Test]
    public async Task BsonObjectId_ShouldWork()
    {
        var oid = ObjectId.NewObjectId();
        var bsonOid = new BsonObjectId(oid);
        await Assert.That(bsonOid.Value).IsEqualTo(oid);
        await Assert.That(bsonOid.ToString()).IsEqualTo(oid.ToString());
    }
}
