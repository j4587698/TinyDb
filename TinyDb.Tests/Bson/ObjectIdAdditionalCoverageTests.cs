using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class ObjectIdAdditionalCoverageTests
{
    [Test]
    public async Task DefaultObjectId_ToString_CompareTo_GetHashCode_ShouldHandleNullBytes()
    {
        ObjectId def = default;

        await Assert.That(def.ToString()).IsEqualTo("000000000000000000000000");
        await Assert.That(def.CompareTo(default)).IsEqualTo(0);
        await Assert.That(def.CompareTo(ObjectId.Empty)).IsNegative();
        await Assert.That(def.GetHashCode()).IsEqualTo(0);
    }

    [Test]
    public async Task IConvertible_ToType_ShouldSupportObjectAndNullableDateTime_AndRejectOthers()
    {
        var oid = ObjectId.NewObjectId();
        IConvertible c = oid;

        var asObject = c.ToType(typeof(object), null);
        await Assert.That(asObject).IsTypeOf<ObjectId>();
        await Assert.That((ObjectId)asObject).IsEqualTo(oid);

        var asNullableObjectId = c.ToType(typeof(ObjectId?), null);
        await Assert.That(asNullableObjectId).IsTypeOf<ObjectId>();
        await Assert.That((ObjectId)asNullableObjectId).IsEqualTo(oid);

        var asNullableDateTime = c.ToType(typeof(DateTime?), null);
        await Assert.That(asNullableDateTime).IsTypeOf<DateTime>();
        await Assert.That((DateTime)asNullableDateTime).IsEqualTo(oid.Timestamp);

        await Assert.That(() => c.ToType(typeof(int), null)).Throws<InvalidCastException>();
    }
}

