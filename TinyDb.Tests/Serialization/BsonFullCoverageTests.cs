using TinyDb.Serialization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonFullCoverageTests
{
    [Test]
    public async Task Bson_Null_Comparisons()
    {
        var n = BsonNull.Value;
        await Assert.That(n.CompareTo(null)).IsEqualTo(0);
        await Assert.That(n.CompareTo(BsonNull.Value)).IsEqualTo(0);
        await Assert.That(n.CompareTo(new BsonInt32(1))).IsEqualTo(-1);
    }

    [Test]
    public async Task Bson_ObjectId_Comparisons()
    {
        var oid1 = ObjectId.NewObjectId();
        var oid2 = ObjectId.NewObjectId();
        var b1 = new BsonObjectId(oid1);
        var b2 = new BsonObjectId(oid2);
        
        // Ensure oid1 < oid2 for consistent test
        if (b1.CompareTo(b2) > 0) (b1, b2) = (b2, b1);
        
        await Assert.That(b1.CompareTo(b2)).IsLessThan(0);
        await Assert.That(b1.CompareTo(b1)).IsEqualTo(0);
        await Assert.That(b1.Equals(b1)).IsTrue();
        await Assert.That(b1.Equals(b2)).IsFalse();
    }

    [Test]
    public async Task Bson_DateTime_Comparisons()
    {
        var d1 = new BsonDateTime(DateTime.UtcNow);
        var d2 = new BsonDateTime(DateTime.UtcNow.AddDays(1));
        
        await Assert.That(d1.CompareTo(d2)).IsLessThan(0);
        await Assert.That(d1.Equals(d1)).IsTrue();
        await Assert.That(d1.Equals(d2)).IsFalse();
    }
}
