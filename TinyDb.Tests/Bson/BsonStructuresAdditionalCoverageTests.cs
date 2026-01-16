using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using System.Collections;

namespace TinyDb.Tests.Bson;

public class BsonStructuresAdditionalCoverageTests
{
    [Test]
    public async Task BsonArray_ReadOnlyMethods_ShouldThrow()
    {
        var array = new BsonArray();
        var list = (IList<BsonValue>)array;
        
        await Assert.That(() => list.Clear()).Throws<NotSupportedException>();
        await Assert.That(() => list.Remove(1)).Throws<NotSupportedException>();
        await Assert.That(() => list.RemoveAt(0)).Throws<NotSupportedException>();
        await Assert.That(() => list.Insert(0, 1)).Throws<NotSupportedException>();
        await Assert.That(() => list.Add(1)).Throws<NotSupportedException>();
        await Assert.That(list.IsReadOnly).IsTrue();
    }

    [Test]
    public async Task BsonArray_CopyTo_ShouldWork()
    {
        var array = new BsonArray().AddValue(1).AddValue(2);
        var target = new BsonValue[2];
        array.CopyTo(target, 0);
        
        await Assert.That(target[0].ToInt32()).IsEqualTo(1);
        await Assert.That(target[1].ToInt32()).IsEqualTo(2);
    }

    [Test]
    public async Task BsonArray_IConvertible_Coverage()
    {
        var array = new BsonArray().AddValue(1);
        
        await Assert.That(array.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(array.ToBoolean(null)).IsTrue();
        await Assert.That(new BsonArray().ToBoolean(null)).IsFalse();
        
        await Assert.That(array.ToInt32(null)).IsEqualTo(1);
        await Assert.That(array.ToInt64(null)).IsEqualTo(1L);
        await Assert.That(() => array.ToDateTime(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task BsonDocument_ReadOnlyMethods_ShouldThrow()
    {
        var doc = new BsonDocument();
        var dict = (IDictionary<string, BsonValue>)doc;
        
        await Assert.That(() => dict.Clear()).Throws<NotSupportedException>();
        await Assert.That(() => dict.Remove("a")).Throws<NotSupportedException>();
        await Assert.That(() => dict.Remove(new KeyValuePair<string, BsonValue>("a", 1))).Throws<NotSupportedException>();
        await Assert.That(() => dict.Add("a", 1)).Throws<NotSupportedException>();
        await Assert.That(() => dict.Add(new KeyValuePair<string, BsonValue>("a", 1))).Throws<NotSupportedException>();
        await Assert.That(dict.IsReadOnly).IsTrue();
    }

    [Test]
    public async Task BsonDocument_CopyTo_ShouldWork()
    {
        var doc = new BsonDocument().Set("a", 1);
        var target = new KeyValuePair<string, BsonValue>[1];
        doc.CopyTo(target, 0);
        
        await Assert.That(target[0].Key).IsEqualTo("a");
        await Assert.That(target[0].Value.ToInt32()).IsEqualTo(1);
    }

    [Test]
    public async Task BsonDocument_IConvertible_Coverage()
    {
        var doc = new BsonDocument().Set("a", 1);
        
        await Assert.That(doc.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(doc.ToBoolean(null)).IsTrue();
        await Assert.That(new BsonDocument().ToBoolean(null)).IsFalse();
        
        await Assert.That(doc.ToInt32(null)).IsEqualTo(1);
        await Assert.That(doc.ToInt64(null)).IsEqualTo(1L);
    }
}
