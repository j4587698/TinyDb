using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class Decimal128ExtendedTests
{
    [Test]
    public async Task Decimal128_Conversion_ShouldWork()
    {
        var d = new Decimal128(123.45m);
        
        await Assert.That(d.ToString()).IsEqualTo("123.45");
        
        // IConvertible
        var convertible = (IConvertible)d;
        await Assert.That(convertible.ToDecimal(null)).IsEqualTo(123.45m);
        await Assert.That(convertible.ToDouble(null)).IsEqualTo(123.45);
        await Assert.That(convertible.ToInt32(null)).IsEqualTo(123);
        await Assert.That(convertible.ToString(null)).IsEqualTo("123.45");
        
        // Finite overflow
        var max = Decimal128.MaxValue;
        await Assert.That(() => max.ToDecimal()).Throws<OverflowException>();
    }

    [Test]
    public async Task Decimal128_Bytes_ShouldWork()
    {
        var d = new Decimal128(123.45m);
        var bytes = d.ToBytes();
        
        await Assert.That(bytes.Length).IsEqualTo(16);
        
        var d2 = Decimal128.FromBytes(bytes);
        await Assert.That(d2).IsEqualTo(d);
        
        await Assert.That(() => Decimal128.FromBytes(new byte[15])).Throws<ArgumentException>();
        await Assert.That(() => Decimal128.FromBytes(null!)).Throws<ArgumentException>();
    }

    [Test]
    public async Task Decimal128_Constants_ShouldBeValid()
    {
        await Assert.That(Decimal128.Zero.ToDecimal()).IsEqualTo(0m);
        await Assert.That(Decimal128.PositiveInfinity.ToString()).IsEqualTo("Infinity");
        await Assert.That(Decimal128.NegativeInfinity.ToString()).IsEqualTo("-Infinity");
        
        // Compare
        await Assert.That(Decimal128.Zero.CompareTo(new Decimal128(1))).IsLessThan(0);
        await Assert.That(Decimal128.Zero.CompareTo(new Decimal128(-1))).IsGreaterThan(0);
        await Assert.That(Decimal128.NegativeInfinity.CompareTo(Decimal128.MinValue)).IsLessThan(0);
        await Assert.That(Decimal128.PositiveInfinity.CompareTo(Decimal128.MaxValue)).IsGreaterThan(0);
    }
}
