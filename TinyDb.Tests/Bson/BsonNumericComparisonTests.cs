using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonNumericComparisonTests
{
    [Test]
    public async Task Double_CompareWith_OtherNumerics()
    {
        var d = new BsonDouble(10.0);
        
        await Assert.That(d.CompareTo(new BsonInt32(10))).IsEqualTo(0);
        await Assert.That(d.CompareTo(new BsonInt32(5))).IsPositive();
        await Assert.That(d.CompareTo(new BsonInt32(15))).IsNegative();
        
        await Assert.That(d.CompareTo(new BsonInt64(10L))).IsEqualTo(0);
        await Assert.That(d.CompareTo(new BsonInt64(5L))).IsPositive();
        
        await Assert.That(d.CompareTo(new BsonDecimal128(10m))).IsEqualTo(0);
        await Assert.That(d.CompareTo(new BsonDecimal128(5m))).IsPositive();
    }

    [Test]
    public async Task Decimal_CompareWith_OtherNumerics()
    {
        var dec = new BsonDecimal128(10m);
        
        await Assert.That(dec.CompareTo(new BsonInt32(10))).IsEqualTo(0);
        await Assert.That(dec.CompareTo(new BsonDouble(10.0))).IsEqualTo(0);
        await Assert.That(dec.CompareTo(new BsonInt64(10L))).IsEqualTo(0);
        
        await Assert.That(dec.CompareTo(new BsonInt32(5))).IsPositive();
        await Assert.That(dec.CompareTo(new BsonDouble(15.0))).IsNegative();
    }

    [Test]
    public async Task Int64_CompareWith_OtherNumerics()
    {
        var i64 = new BsonInt64(10L);
        
        await Assert.That(i64.CompareTo(new BsonInt32(10))).IsEqualTo(0);
        await Assert.That(i64.CompareTo(new BsonDouble(10.0))).IsEqualTo(0);
        await Assert.That(i64.CompareTo(new BsonDecimal128(10m))).IsEqualTo(0);
        
        await Assert.That(i64.CompareTo(new BsonInt32(15))).IsNegative();
        await Assert.That(i64.CompareTo(new BsonDouble(5.0))).IsPositive();
    }

    [Test]
    public async Task Int32_CompareWith_OtherNumerics()
    {
        var i32 = new BsonInt32(10);
        
        await Assert.That(i32.CompareTo(new BsonInt64(10L))).IsEqualTo(0);
        await Assert.That(i32.CompareTo(new BsonDouble(10.0))).IsEqualTo(0);
        await Assert.That(i32.CompareTo(new BsonDecimal128(10m))).IsEqualTo(0);
        
        await Assert.That(i32.CompareTo(new BsonInt64(5L))).IsPositive();
        await Assert.That(i32.CompareTo(new BsonDouble(15.0))).IsNegative();
    }
}
