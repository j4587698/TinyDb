using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonDecimal128OverflowCoverageTests
{
    [Test]
    public async Task Decimal128_ToDecimal_WhenSignificandTooLarge_ShouldThrowOverflow()
    {
        var tooLarge = new Decimal128(0, (6176UL << 49) | (1UL << 40) | 1);

        await Assert.That(() => tooLarge.ToDecimal()).Throws<OverflowException>();
    }

    [Test]
    public async Task BsonDecimal128_CompareTo_WhenToDecimalThrows_ShouldFallbackToBsonTypeComparison()
    {
        var tooLarge = new Decimal128(0, (6176UL << 49) | (1UL << 40) | 1);
        var val = new BsonDecimal128(tooLarge);
        var other = new BsonInt32(1);

        var comparison = val.CompareTo(other);

        await Assert.That(comparison).IsEqualTo(val.BsonType.CompareTo(other.BsonType));
    }
}
