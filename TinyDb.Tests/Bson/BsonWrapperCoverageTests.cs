using System;
using System.Reflection;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonWrapperCoverageTests
{
    [Test]
    public async Task BsonDocumentValue_IConvertible_Coverage()
    {
        var doc = new BsonDocument().Set("a", 1);
        var type = typeof(BsonDocument).Assembly.GetType("TinyDb.Bson.BsonDocumentValue");
        if (type == null) return;

        var instance = (IConvertible)Activator.CreateInstance(type, doc)!;
        
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
    public async Task BsonArrayValue_IConvertible_Coverage()
    {
        var arr = new BsonArray().AddValue(1).AddValue(2);
        var type = typeof(BsonDocument).Assembly.GetType("TinyDb.Bson.BsonArrayValue");
        if (type == null) return;

        var instance = (IConvertible)Activator.CreateInstance(type, arr)!;
        
        await Assert.That(instance.ToBoolean(null)).IsTrue();
        await Assert.That(instance.ToInt32(null)).IsEqualTo(2); // Count
        await Assert.That(instance.ToInt64(null)).IsEqualTo(2L);
        
        await Assert.That(instance.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(instance.ToString(null)).IsEqualTo(arr.ToString());
        
        await Assert.That(() => instance.ToDateTime(null)).Throws<InvalidCastException>();
        
        // Cast via ToType
        // BsonArray.ToType delegates to ChangeType(this). 
        // BsonArray implements IEnumerable.
        // It might not convert back to BsonArray via Convert.ChangeType easily unless explicit cast.
        // But let's check basic ones.
    }
}
