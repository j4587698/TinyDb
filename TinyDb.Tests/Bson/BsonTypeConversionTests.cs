using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonTypeConversionTests
{
    [Test]
    public async Task BsonInt64_Conversions()
    {
        BsonValue val = new BsonInt64(1234567890123L);
        await Assert.That(val.ToInt64(null)).IsEqualTo(1234567890123L);
        await Assert.That(val.ToDouble(null)).IsEqualTo(1234567890123.0);
        await Assert.That(val.ToString()).IsEqualTo("1234567890123");
        
        // Overflow check
        await Assert.That(() => val.ToInt32(null)).Throws<OverflowException>();
    }

    [Test]
    public async Task BsonDouble_Conversions()
    {
        BsonValue val = new BsonDouble(123.456);
        await Assert.That(val.ToDouble(null)).IsEqualTo(123.456);
        await Assert.That(val.ToInt32(null)).IsEqualTo(123);
        await Assert.That(val.ToBoolean(null)).IsTrue();
        
        var zero = new BsonDouble(0.0);
        await Assert.That(zero.ToBoolean(null)).IsFalse();
    }

    [Test]
    public async Task BsonBoolean_Conversions()
    {
        BsonValue t = new BsonBoolean(true);
        await Assert.That(t.ToBoolean(null)).IsTrue();
        await Assert.That(t.ToInt32(null)).IsEqualTo(1);
        
        BsonValue f = new BsonBoolean(false);
        await Assert.That(f.ToBoolean(null)).IsFalse();
        await Assert.That(f.ToInt32(null)).IsEqualTo(0);
    }

    [Test]
    public async Task BsonDateTime_Conversions()
    {
        var now = DateTime.UtcNow;
        BsonValue val = new BsonDateTime(now);
        await Assert.That(val.ToDateTime(null)).IsEqualTo(now);
        // CompareTo
        await Assert.That(val.CompareTo(new BsonDateTime(now))).IsEqualTo(0);
    }

    [Test]
    public async Task BsonDecimal128_Conversions()
    {
        BsonValue val = new BsonDecimal128(123.456m);
        await Assert.That(val.ToDecimal(null)).IsEqualTo(123.456m);
        await Assert.That(val.ToDouble(null)).IsEqualTo(123.456);
        await Assert.That(val.ToInt32(null)).IsEqualTo(123);
    }

    [Test]
    public async Task BsonBinary_Conversions()
    {
        var bytes = new byte[] { 1, 2, 3 };
        BsonValue val = new BsonBinary(bytes);
        
        // Default conversions usually fail or return simplified
        await Assert.That(val.ToString()).IsEqualTo("Binary(Generic, 3 bytes)");
        
        // Equals
        await Assert.That(val.Equals(new BsonBinary(new byte[] { 1, 2, 3 }))).IsTrue();
        await Assert.That(val.Equals(new BsonBinary(new byte[] { 1, 2 }))).IsFalse();
    }

    [Test]
    public async Task BsonTimestamp_Conversions()
    {
        BsonValue val = new BsonTimestamp(12345L);
        await Assert.That(val.ToInt64(null)).IsEqualTo(12345L);
        await Assert.That(val.CompareTo(new BsonTimestamp(12345L))).IsEqualTo(0);
    }
}
