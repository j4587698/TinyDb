using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class InternalBsonValueTests
{
    [Test]
    public async Task BsonArray_As_BsonValue_Coverage()
    {
        // BsonArray directly inherits BsonValue - no need for reflection
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });
        
        // Use BsonArray directly as BsonValue
        BsonValue instance = array;
        
        await Assert.That(instance).IsNotNull();
        await Assert.That(instance.BsonType).IsEqualTo(BsonType.Array);
        await Assert.That(instance.IsArray).IsTrue();
        await Assert.That(instance.RawValue).IsNotNull();
        
        // ToString
        await Assert.That(instance.ToString()).IsEqualTo(array.ToString());
        
        // Equals
        BsonValue instance2 = new BsonArray(new BsonValue[] { 1, 2, 3 });
        await Assert.That(instance.Equals(instance2)).IsTrue();
        
        // CompareTo
        await Assert.That(instance.CompareTo(instance2)).IsEqualTo(0);
        
        // GetHashCode
        await Assert.That(instance.GetHashCode()).IsEqualTo(array.GetHashCode());
    }

    [Test]
    public async Task BsonDocument_As_BsonValue_Coverage()
    {
        // BsonDocument directly inherits BsonValue - no need for reflection
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { { "a", 1 } });
        
        // Use BsonDocument directly as BsonValue
        BsonValue instance = doc;
        
        await Assert.That(instance).IsNotNull();
        await Assert.That(instance.BsonType).IsEqualTo(BsonType.Document);
        await Assert.That(instance.IsDocument).IsTrue();
        
        // ToString
        await Assert.That(instance.ToString()).IsEqualTo(doc.ToString());
        
        // Equals
        BsonValue instance2 = new BsonDocument(new Dictionary<string, BsonValue> { { "a", 1 } });
        await Assert.That(instance.Equals(instance2)).IsTrue();
        
        // CompareTo
        await Assert.That(instance.CompareTo(instance2)).IsEqualTo(0);
        
        // GetHashCode
        await Assert.That(instance.GetHashCode()).IsEqualTo(doc.GetHashCode());
    }
}