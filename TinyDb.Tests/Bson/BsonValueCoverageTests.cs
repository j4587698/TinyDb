using System;
using System.Threading.Tasks;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonValueCoverageTests
{
    [Test]
    public async Task BsonDouble_Coverage()
    {
        var d1 = new BsonDouble(1.5);
        var d2 = new BsonDouble(1.5);
        var d3 = new BsonDouble(2.5);
        
        await Assert.That(d1.Equals(d2)).IsTrue();
        await Assert.That(d1.Equals(d3)).IsFalse();
        await Assert.That(d1.CompareTo(d2)).IsEqualTo(0);
        await Assert.That(d1.CompareTo(d3)).IsLessThan(0);
        await Assert.That(d1.GetHashCode()).IsEqualTo(d2.GetHashCode());
        await Assert.That(d1.ToString()).IsEqualTo("1.5");
        await Assert.That(d1.Value).IsEqualTo(1.5d);
        
        await Assert.That(d1.ToBoolean(null)).IsTrue();
        await Assert.That(d1.ToDecimal(null)).IsEqualTo(1.5m);
        await Assert.That(d1.ToDouble(null)).IsEqualTo(1.5);
        await Assert.That(d1.ToSingle(null)).IsEqualTo(1.5f);
        await Assert.That(d1.ToInt32(null)).IsEqualTo(2); // Banker's rounding
        await Assert.That(d1.ToInt64(null)).IsEqualTo(2L);
        await Assert.That(d1.ToInt16(null)).IsEqualTo((short)2);
        await Assert.That(d1.ToByte(null)).IsEqualTo((byte)2);
        
        await Assert.That(() => d1.ToDateTime(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonInt64_Coverage()
    {
        var v1 = new BsonInt64(100L);
        await Assert.That(v1.Value).IsEqualTo(100L);
        await Assert.That(v1.CompareTo(new BsonInt64(200L))).IsLessThan(0);
        await Assert.That(v1.ToBoolean(null)).IsTrue();
        await Assert.That(v1.ToByte(null)).IsEqualTo((byte)100);
        await Assert.That(v1.ToDecimal(null)).IsEqualTo(100m);
        await Assert.That(v1.ToDouble(null)).IsEqualTo(100.0);
        await Assert.That(v1.ToInt16(null)).IsEqualTo((short)100);
        await Assert.That(v1.ToInt32(null)).IsEqualTo(100);
        await Assert.That(v1.ToInt64(null)).IsEqualTo(100L);
        await Assert.That(v1.ToSingle(null)).IsEqualTo(100.0f);
    }

    [Test]
    public async Task BsonString_Coverage()
    {
        var s1 = new BsonString("abc");
        var s2 = new BsonString("abc");
        var s3 = new BsonString("def");
        var sBool = new BsonString("true");
        var sInt = new BsonString("123");
        
        await Assert.That(s1.Equals(s2)).IsTrue();
        await Assert.That(s1.Equals(s3)).IsFalse();
        await Assert.That(s1.CompareTo(s2)).IsEqualTo(0);
        await Assert.That(s1.CompareTo(s3)).IsLessThan(0);
        await Assert.That(s1.GetHashCode()).IsEqualTo(s2.GetHashCode());
        await Assert.That(s1.ToString()).IsNotNull(); 
        
        await Assert.That(sBool.ToBoolean(null)).IsTrue();
        await Assert.That(sInt.ToInt32(null)).IsEqualTo(123);
        await Assert.That(sInt.ToDouble(null)).IsEqualTo(123.0);
        
        await Assert.That(() => s1.ToBoolean(null)).Throws<FormatException>(); 
    }

    [Test]
    public async Task BsonNull_Coverage()
    {
        var val = BsonNull.Value;
        await Assert.That(val.Equals(BsonNull.Value)).IsTrue();
        await Assert.That(val.ToString()).IsEqualTo("null");
        
        await Assert.That(val.ToBoolean(null)).IsFalse();
        await Assert.That(val.ToByte(null)).IsEqualTo((byte)0);
        await Assert.That(val.ToChar(null)).IsEqualTo('\0');
        await Assert.That(val.ToDateTime(null)).IsEqualTo(default(DateTime));
        await Assert.That(val.ToDecimal(null)).IsEqualTo(0m);
        await Assert.That(val.ToDouble(null)).IsEqualTo(0.0);
        await Assert.That(val.ToInt16(null)).IsEqualTo((short)0);
        await Assert.That(val.ToInt32(null)).IsEqualTo(0);
        await Assert.That(val.ToInt64(null)).IsEqualTo(0L);
        await Assert.That(val.ToSByte(null)).IsEqualTo((sbyte)0);
        await Assert.That(val.ToSingle(null)).IsEqualTo(0.0f);
        await Assert.That(val.ToUInt16(null)).IsEqualTo((ushort)0);
        await Assert.That(val.ToUInt32(null)).IsEqualTo(0u);
        await Assert.That(val.ToUInt64(null)).IsEqualTo(0UL);
        await Assert.That(val.ToType(typeof(object), null)).IsNull();
    }

    [Test]
    public async Task BsonMaxKey_Coverage()
    {
        var m1 = BsonMaxKey.Value;
        await Assert.That(m1.CompareTo(BsonNull.Value)).IsGreaterThan(0);
        await Assert.That(m1.ToString(null)).IsEqualTo("$maxKey");
        await Assert.That(m1.ToBoolean(null)).IsTrue();
        await Assert.That(m1.ToInt32(null)).IsEqualTo(int.MaxValue);
    }

    [Test]
    public async Task BsonMinKey_Coverage()
    {
        var m1 = BsonMinKey.Value;
        await Assert.That(m1.CompareTo(BsonNull.Value)).IsLessThan(0);
        await Assert.That(m1.ToString(null)).IsEqualTo("$minKey");
        await Assert.That(m1.ToBoolean(null)).IsFalse();
    }

    [Test]
    public async Task BsonTimestamp_Coverage()
    {
        var t1 = new BsonTimestamp(100);
        await Assert.That(t1.Value).IsEqualTo(100u);
        await Assert.That(t1.CompareTo(new BsonTimestamp(200))).IsLessThan(0);
        await Assert.That(t1.ToString()).IsNotNull();
    }

    [Test]
    public async Task BsonDecimal128_Coverage()
    {
        var val = new BsonDecimal128(123.45m);
        await Assert.That(val.Value).IsEqualTo(123.45m);
        await Assert.That(val.ToString()).IsEqualTo("123.45");
        
        await Assert.That(val.ToBoolean(null)).IsTrue();
        await Assert.That(val.ToDecimal(null)).IsEqualTo(123.45m);
        await Assert.That(val.ToDouble(null)).IsEqualTo(123.45);
        await Assert.That(val.ToSingle(null)).IsEqualTo(123.45f);
        await Assert.That(val.ToInt32(null)).IsEqualTo(123);
        await Assert.That(val.ToInt64(null)).IsEqualTo(123L);
        await Assert.That(val.ToInt16(null)).IsEqualTo((short)123);
        await Assert.That(val.ToByte(null)).IsEqualTo((byte)123);
        await Assert.That(val.ToSByte(null)).IsEqualTo((sbyte)123);
        await Assert.That(val.ToUInt16(null)).IsEqualTo((ushort)123);
        await Assert.That(val.ToUInt32(null)).IsEqualTo(123u);
        await Assert.That(val.ToUInt64(null)).IsEqualTo(123UL);
        
        await Assert.That(() => val.ToDateTime(null)).Throws<InvalidCastException>();
        await Assert.That(() => val.ToChar(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonDateTime_Coverage()
    {
        var now = DateTime.UtcNow;
        var val = new BsonDateTime(now);
        await Assert.That(val.Value).IsEqualTo(now);
        await Assert.That(val.ToDateTime(null)).IsEqualTo(now);
        await Assert.That(val.ToString()).IsNotNull();
        
        await Assert.That(() => val.ToBoolean(null)).Throws<InvalidCastException>();
        
        await Assert.That(() => val.ToDecimal(null)).Throws<InvalidCastException>();
        await Assert.That(() => val.ToDouble(null)).Throws<InvalidCastException>();
        await Assert.That(() => val.ToInt32(null)).Throws<InvalidCastException>();
    }
}
