using System;
using System.Globalization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonValueExtensiveTests
{
    [Test]
    public async Task BsonDecimal128_Extensive_ShouldWork()
    {
        BsonDecimal128 val = 123.45m;
        IConvertible conv = val;
        
        await Assert.That(conv.ToDecimal(null)).IsEqualTo(123.45m);
        await Assert.That(conv.ToDouble(null)).IsEqualTo(123.45);
        await Assert.That(conv.ToBoolean(null)).IsTrue();
        await Assert.That(conv.ToByte(null)).IsEqualTo((byte)123);
        await Assert.That(conv.ToInt32(null)).IsEqualTo(123);
        await Assert.That(conv.ToInt64(null)).IsEqualTo(123L);
        await Assert.That(conv.ToUInt64(null)).IsEqualTo(123UL);
        await Assert.That(conv.GetTypeCode()).IsEqualTo(TypeCode.Decimal);
        await Assert.That(conv.ToType(typeof(decimal), null)).IsEqualTo(123.45m);
    }

    [Test]
    public async Task BsonDouble_Extensive_ShouldWork()
    {
        BsonDouble val = 3.14;
        IConvertible conv = val;
        
        await Assert.That(conv.ToDouble(null)).IsEqualTo(3.14);
        await Assert.That(conv.ToDecimal(null)).IsEqualTo(3.14m);
        await Assert.That(conv.ToInt32(null)).IsEqualTo(3);
        await Assert.That(conv.GetTypeCode()).IsEqualTo(TypeCode.Double);
    }

    [Test]
    public async Task BsonInt32_Extensive_ShouldWork()
    {
        BsonInt32 val = 100;
        IConvertible conv = val;
        await Assert.That(conv.ToChar(null)).IsEqualTo((char)100);
        await Assert.That(conv.ToInt16(null)).IsEqualTo((short)100);
        await Assert.That(conv.ToSByte(null)).IsEqualTo((sbyte)100);
        await Assert.That(conv.ToUInt32(null)).IsEqualTo(100u);
    }

    [Test]
    public async Task BsonTimestamp_Extensive_ShouldWork()
    {
        var ts = new BsonTimestamp(1000, 50);
        await Assert.That(ts.Timestamp).IsEqualTo(1000);
        await Assert.That(ts.Increment).IsEqualTo(50);
        await Assert.That(ts.ToDateTime().Year).IsGreaterThanOrEqualTo(1970);
        
        var current = BsonTimestamp.CreateCurrent(1);
        await Assert.That(current.Increment).IsEqualTo(1);
        await Assert.That(current.Timestamp).IsGreaterThan(0);
        
        BsonTimestamp implicitTs = 123456789L;
        long implicitLong = implicitTs;
        await Assert.That(implicitLong).IsEqualTo(123456789L);
        
        await Assert.That(ts.Equals(new BsonTimestamp(1000, 50))).IsTrue();
        await Assert.That(ts.CompareTo(current)).IsLessThan(0);
    }

    [Test]
    public async Task BsonRegularExpression_Extensive_ShouldWork()
    {
        var re = new BsonRegularExpression("abc", "im");
        await Assert.That(re.Pattern).IsEqualTo("abc");
        await Assert.That(re.Options).IsEqualTo("im");
        
        var re2 = new BsonRegularExpression("abc", "im");
        await Assert.That(re.Equals(re2)).IsTrue();
        await Assert.That(re.GetHashCode()).IsEqualTo(re2.GetHashCode());
        
        await Assert.That(re.CompareTo(new BsonRegularExpression("abd"))).IsLessThan(0);
    }

    [Test]
    public async Task BsonBoolean_Extensive_ShouldWork()
    {
        BsonBoolean val = true;
        IConvertible conv = val;
        await Assert.That(conv.ToType(typeof(bool), null)).IsEqualTo(true);
        await Assert.That(conv.ToType(typeof(string), null)).IsEqualTo("true");
    }

    [Test]
    public async Task BsonDateTime_Extensive_ShouldWork()
    {
        var now = DateTime.UtcNow;
        BsonDateTime val = now;
        IConvertible conv = val;
        await Assert.That(conv.ToType(typeof(DateTime), null)).IsEqualTo(now);
    }

    [Test]
    public async Task BsonObjectId_Extensive_ShouldWork()
    {
        var oid = ObjectId.NewObjectId();
        BsonObjectId val = oid;
        await Assert.That(val.Value).IsEqualTo(oid);
        await Assert.That(val.ToString()).IsEqualTo(oid.ToString());
        
        ObjectId implicitOid = val;
        await Assert.That(implicitOid).IsEqualTo(oid);
        
        await Assert.That(val.Equals(new BsonObjectId(oid))).IsTrue();
        await Assert.That(val.CompareTo(new BsonObjectId(ObjectId.Empty))).IsGreaterThan(0);
    }
}
