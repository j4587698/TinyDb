using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonWrapperCoverageTests
{
    [Test]
    public async Task BsonDocument_IConvertible_Coverage()
    {
        // BsonDocument directly implements IConvertible - no need for internal BsonDocumentValue
        var doc = new BsonDocument().Set("a", 1);
        var instance = (IConvertible)doc;
        
        await Assert.That(instance.ToBoolean(null)).IsTrue();
        await Assert.That(instance.ToInt32(null)).IsEqualTo(1);
        await Assert.That(instance.ToInt64(null)).IsEqualTo(1L);
        await Assert.That(instance.ToDouble(null)).IsEqualTo(1.0);
        await Assert.That(instance.ToDecimal(null)).IsEqualTo(1m);
        await Assert.That(instance.ToSingle(null)).IsEqualTo(1f);
        
        await Assert.That(instance.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(instance.ToString(null)).IsEqualTo(doc.ToString());
        
        await Assert.That(() => instance.ToDateTime(null)).Throws<InvalidCastException>();
        
        // Cast via ToType
        await Assert.That(instance.ToType(typeof(BsonDocument), null)).IsEqualTo(doc);
    }

    [Test]
    public async Task BsonArray_IConvertible_Coverage()
    {
        // BsonArray directly implements IConvertible - no need for internal BsonArrayValue
        var arr = new BsonArray().AddValue(1).AddValue(2);
        var instance = (IConvertible)arr;
        
        await Assert.That(instance.ToBoolean(null)).IsTrue();
        await Assert.That(instance.ToInt32(null)).IsEqualTo(2); // Count
        await Assert.That(instance.ToInt64(null)).IsEqualTo(2L);
        
        await Assert.That(instance.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(instance.ToString(null)).IsEqualTo(arr.ToString());
        
        await Assert.That(() => instance.ToDateTime(null)).Throws<InvalidCastException>();
    }
}
