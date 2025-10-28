using SimpleDb.Bson;
using SimpleDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Serialization;

public class BsonConversionTests
{
    [Test]
    public async Task ToBsonValue_Should_Use_BsonDecimal128_For_Decimal()
    {
        const decimal value = 123456789.123456789m;

        var bsonValue = BsonConversion.ToBsonValue(value);

        await Assert.That(bsonValue.GetType()).IsEqualTo(typeof(BsonDecimal128));
        await Assert.That(((BsonDecimal128)bsonValue).Value).IsEqualTo(value);
    }

    [Test]
    public async Task FromBsonValue_Should_Return_Decimal()
    {
        const decimal value = 987654321.987654321m;
        var bson = new BsonDecimal128(value);

        var result = BsonConversion.FromBsonValue(bson, typeof(decimal));

        await Assert.That(result).IsNotNull();
        await Assert.That(result).IsEqualTo(value);
    }
}
