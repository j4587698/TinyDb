using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class Decimal128CoverageTests
{
    [Test]
    public async Task Decimal128_Math_ShouldWork()
    {
        var d1 = new Decimal128(10.5m);
        var d2 = new Decimal128(2.0m);
        
        // Assuming implicit/explicit operators or methods
        // But Decimal128 is likely a simple wrapper or struct with minimal methods.
        // It has ToDecimal().
        
        await Assert.That(d1.ToDecimal()).IsEqualTo(10.5m);
        
        // Equality
        await Assert.That(d1.Equals(d1)).IsTrue();
        await Assert.That(d1.Equals(d2)).IsFalse();
        await Assert.That(d1.Equals((object)d1)).IsTrue();
        
        // ToString
        await Assert.That(d1.ToString()).IsEqualTo("10.5");
        
        // GetHashCode
        await Assert.That(d1.GetHashCode()).IsEqualTo(new Decimal128(10.5m).GetHashCode());
        
        // CompareTo (if implemented)
        if (d1 is IComparable<Decimal128> c)
        {
            await Assert.That(c.CompareTo(d2)).IsGreaterThan(0);
        }
    }
}
