using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonValueEqualityTests
{
    [Test]
    public async Task BsonValue_Equality_Operator_Should_Work()
    {
        BsonValue a = new BsonInt32(1);
        BsonValue b = new BsonInt32(1);
        BsonValue c = new BsonInt32(2);
        
        await Assert.That(a == b).IsTrue();
        await Assert.That(a != c).IsTrue();
        await Assert.That(a == (BsonValue?)null).IsFalse();
        await Assert.That((BsonValue?)null == a).IsFalse();
        
        BsonValue d = new BsonString("1");
        await Assert.That(a == d).IsFalse();
    }

    [Test]
    public async Task BsonValue_CompareTo_Should_Work()
    {
        BsonValue small = new BsonInt32(10);
        BsonValue large = new BsonInt32(20);
        BsonValue otherType = new BsonString("10");
        
        await Assert.That(small.CompareTo(large)).IsNegative();
        await Assert.That(large.CompareTo(small)).IsPositive();
        await Assert.That(small.CompareTo(otherType)).IsNotEqualTo(0);
    }
}
