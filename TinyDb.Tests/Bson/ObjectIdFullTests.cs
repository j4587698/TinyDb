using System;
using System.Linq;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class ObjectIdFullTests
{
    [Test]
    public async Task ObjectId_Operators_ShouldWork()
    {
        var oid1 = ObjectId.NewObjectId();
        var oid2 = new ObjectId(oid1.ToByteArray().ToArray());
        var oid3 = ObjectId.Empty;
        
        await Assert.That(oid1 == oid2).IsTrue();
        await Assert.That(oid1 != oid3).IsTrue();
        await Assert.That(oid1.Equals(oid2)).IsTrue();
        await Assert.That(oid1.Equals((object)oid2)).IsTrue();
        await Assert.That(oid1.GetHashCode()).IsEqualTo(oid2.GetHashCode());
    }

    [Test]
    public async Task ObjectId_Comparison_ShouldWork()
    {
        byte[] bytes1 = new byte[12];
        bytes1[11] = 1;
        byte[] bytes2 = new byte[12];
        bytes2[11] = 2;
        
        var oid1 = new ObjectId(bytes1);
        var oid2 = new ObjectId(bytes2);
        
        await Assert.That(oid1.CompareTo(oid2)).IsLessThan(0);
        await Assert.That(oid2.CompareTo(oid1)).IsGreaterThan(0);
        await Assert.That(oid1 < oid2).IsTrue();
        await Assert.That(oid2 > oid1).IsTrue();
        await Assert.That(oid1 <= oid2).IsTrue();
        await Assert.That(oid2 >= oid1).IsTrue();
    }

    [Test]
    public async Task ObjectId_TryParse_EdgeCases_ShouldWork()
    {
        string? nullStr = null;
        await Assert.That(ObjectId.TryParse(nullStr, out _)).IsFalse();
        await Assert.That(ObjectId.TryParse("", out _)).IsFalse();
        await Assert.That(ObjectId.TryParse("invalid", out _)).IsFalse();
        
        var oid = ObjectId.NewObjectId();
        await Assert.That(ObjectId.TryParse(oid.ToString(), out var result)).IsTrue();
        await Assert.That(result).IsEqualTo(oid);
    }

    [Test]
    public async Task ObjectId_Creation_ShouldWork()
    {
        var bytes = Enumerable.Repeat((byte)0x01, 12).ToArray();
        var oid = new ObjectId(bytes);
        await Assert.That(oid.ToByteArray().ToArray().SequenceEqual(bytes)).IsTrue();
        await Assert.That(oid.TimestampSeconds).IsGreaterThan(0);
    }
}
