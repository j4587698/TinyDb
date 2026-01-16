using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonIConvertibleTests
{
    [Test]
    public async Task BsonObjectId_IConvertible_Coverage()
    {
        var oid = ObjectId.NewObjectId();
        var val = new BsonObjectId(oid);
        
        // These rely on Convert.To...(string)
        // Usually throw FormatException for most types unless string is valid for that type
        await Assert.That(() => val.ToBoolean(null)).Throws<FormatException>();
        await Assert.That(() => val.ToInt32(null)).Throws<FormatException>();
        await Assert.That(val.ToString(null)).IsEqualTo(oid.ToString());
        
        await Assert.That(val.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(val.ToType(typeof(string), null)).IsEqualTo(oid.ToString());
        await Assert.That(val.ToType(typeof(ObjectId), null)).IsEqualTo(oid);
    }

    [Test]
    public async Task BsonDateTime_IConvertible_Coverage()
    {
        var now = DateTime.UtcNow;
        var val = new BsonDateTime(now);
        
        await Assert.That(val.ToDateTime(null)).IsEqualTo(now);
        await Assert.That(val.ToString(null)).IsEqualTo(now.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));
        
        // Others throw InvalidCastException or use Convert.To...(DateTime) which works for some
        // Convert.ToBoolean(DateTime) -> InvalidCastException
        await Assert.That(() => val.ToBoolean(null)).Throws<InvalidCastException>();
        await Assert.That(() => val.ToInt32(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonDouble_IConvertible_Coverage()
    {
        var val = new BsonDouble(123.45);
        
        await Assert.That(val.ToDouble(null)).IsEqualTo(123.45);
        await Assert.That(val.ToBoolean(null)).IsTrue();
        await Assert.That(val.ToInt32(null)).IsEqualTo(123);
        await Assert.That(val.ToDecimal(null)).IsEqualTo(123.45m);
        await Assert.That(val.ToString(null)).IsEqualTo("123.45");
    }

    [Test]
    public async Task BsonInt64_IConvertible_Coverage()
    {
        var val = new BsonInt64(123);
        
        await Assert.That(val.ToInt64(null)).IsEqualTo(123L);
        await Assert.That(val.ToInt32(null)).IsEqualTo(123);
        await Assert.That(val.ToBoolean(null)).IsTrue();
        await Assert.That(val.ToString(null)).IsEqualTo("123");
    }

    [Test]
    public async Task BsonString_IConvertible_Coverage()
    {
        var val = new BsonString("123");
        
        await Assert.That(val.ToString(null)).IsEqualTo("123");
        await Assert.That(val.ToInt32(null)).IsEqualTo(123);
        
        // Convert.ToBoolean("123") throws FormatException
        await Assert.That(() => val.ToBoolean(null)).Throws<FormatException>();
        
        var trueVal = new BsonString("True");
        await Assert.That(trueVal.ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task BsonNull_IConvertible_Coverage()
    {
        var val = BsonNull.Value;
        
        await Assert.That(val.ToBoolean(null)).IsFalse();
        await Assert.That(val.ToInt32(null)).IsEqualTo(0);
        await Assert.That(val.ToString(null)).IsEqualTo("null");
        await Assert.That(val.ToType(typeof(string), null)).IsNull();
        await Assert.That(val.ToType(typeof(int?), null)).IsNull();
    }
    
    [Test]
    public async Task BsonDecimal128_IConvertible_Coverage()
    {
        var val = new BsonDecimal128(123.45m);
        
        await Assert.That(val.ToDecimal(null)).IsEqualTo(123.45m);
        await Assert.That(val.ToDouble(null)).IsEqualTo(123.45);
        await Assert.That(val.ToInt32(null)).IsEqualTo(123);
        await Assert.That(val.ToBoolean(null)).IsTrue();
        await Assert.That(val.ToString(null)).IsEqualTo("123.45");
        
        // Overflow
        var max = new BsonDecimal128(decimal.MaxValue);
        await Assert.That(() => max.ToInt32(null)).Throws<OverflowException>();
    }
    
    [Test]
    public async Task BsonTimestamp_IConvertible_Coverage()
    {
        // Use constructor (int timestamp, int increment) to set timestamp correctly
        var val = new BsonTimestamp(1234567890, 0);
        
        // 1234567890 seconds from Unix epoch
        var expected = DateTimeOffset.FromUnixTimeSeconds(1234567890).UtcDateTime;
        await Assert.That(val.ToDateTime(null)).IsEqualTo(expected);
    }
}
