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
    public async Task BsonArrayValue_Coverage()
    {
        // Use constructor because collection initializer uses Add() which throws on immutable BsonArray
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });
        
        // Try reflection first as I am not sure if I can see it.
        var type = typeof(BsonArray).Assembly.GetType("TinyDb.Bson.BsonArrayValue");
        
        if (type != null)
        {
            var instance = Activator.CreateInstance(type, array) as BsonValue;
            
            await Assert.That(instance).IsNotNull();
            await Assert.That(instance!.BsonType).IsEqualTo(BsonType.Array);
            await Assert.That(instance.IsArray).IsTrue();
            await Assert.That(instance.RawValue).IsNotNull();
            
            // ToString
            await Assert.That(instance.ToString()).IsEqualTo(array.ToString());
            
            // Equals
            var instance2 = Activator.CreateInstance(type, array) as BsonValue;
            await Assert.That(instance.Equals(instance2)).IsTrue();
            
            // CompareTo
            await Assert.That(instance.CompareTo(instance2)).IsEqualTo(0);
            
            // GetHashCode
            await Assert.That(instance.GetHashCode()).IsEqualTo(array.GetHashCode());
        }
    }

    [Test]
    public async Task BsonDocumentValue_Coverage()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { { "a", 1 } });
        var type = typeof(BsonDocument).Assembly.GetType("TinyDb.Bson.BsonDocumentValue");
        
        if (type != null)
        {
            var instance = Activator.CreateInstance(type, doc) as BsonValue;
            
            await Assert.That(instance).IsNotNull();
            await Assert.That(instance!.BsonType).IsEqualTo(BsonType.Document);
            await Assert.That(instance.IsDocument).IsTrue();
            
            // ToString
            await Assert.That(instance.ToString()).IsEqualTo(doc.ToString());
            
            // Equals
            var instance2 = Activator.CreateInstance(type, doc) as BsonValue;
            await Assert.That(instance.Equals(instance2)).IsTrue();
            
            // CompareTo
            await Assert.That(instance.CompareTo(instance2)).IsEqualTo(0);
            
            // GetHashCode
            await Assert.That(instance.GetHashCode()).IsEqualTo(doc.GetHashCode());
        }
    }
}