using System;
using System.Globalization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonValueTypeTests
{
    [Test]
    public async Task BsonInt64_Operators_And_Comparisons_ShouldWork()
    {
        BsonInt64 val = 100L; // Implicit cast
        long l = val; // Implicit cast
        await Assert.That(l).IsEqualTo(100L);
        
        await Assert.That(val.CompareTo(new BsonInt64(200))).IsLessThan(0);
        await Assert.That(val.CompareTo(new BsonInt32(50))).IsGreaterThan(0);
        await Assert.That(val.Equals(new BsonInt64(100))).IsTrue();
        await Assert.That(val.GetHashCode()).IsEqualTo(100L.GetHashCode());
    }

    [Test]
    public async Task BsonDouble_Operators_And_Comparisons_ShouldWork()
    {
        BsonDouble val = 3.14;
        double d = val;
        await Assert.That(d).IsEqualTo(3.14);
        
        await Assert.That(val.CompareTo(new BsonDouble(2.0))).IsGreaterThan(0);
        await Assert.That(val.Equals(new BsonDouble(3.14))).IsTrue();
    }

    [Test]
    public async Task BsonDecimal128_ShouldWork()
    {
        decimal dec = 123.45m;
        BsonDecimal128 val = dec;
        decimal result = (decimal)val.Value;
        await Assert.That(result).IsEqualTo(dec);
        
        await Assert.That(val.ToString()).IsEqualTo("123.45");
        await Assert.That(val.ToDouble(null)).IsEqualTo(123.45);
    }

    [Test]
    public async Task BsonDateTime_ShouldWork()
    {
        var now = DateTime.UtcNow;
        BsonDateTime val = now;
        await Assert.That(val.Value).IsEqualTo(now);
        await Assert.That(val.IsDateTime).IsTrue();
    }

    [Test]
    public async Task BsonBoolean_ShouldWork()
    {
        BsonBoolean bTrue = true;
        BsonBoolean bFalse = false;
        await Assert.That(bTrue.Value).IsTrue();
        await Assert.That(bFalse.Value).IsFalse();
        await Assert.That(bTrue.IsBoolean).IsTrue();
    }
}
