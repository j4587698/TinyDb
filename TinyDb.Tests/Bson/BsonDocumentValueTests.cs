using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonDocumentValueTests
{
    [Test]
    public async Task BsonDocumentValue_Delegates_Correctly()
    {
        // BsonDocumentValue is internal. 
        // We can access it because TinyDb.Tests is a friend assembly.
        
        var doc = new BsonDocument().Set("a", 1);
        
        var type = typeof(BsonDocument).Assembly.GetType("TinyDb.Bson.BsonDocumentValue");
        await Assert.That(type).IsNotNull();
        
        var instance = Activator.CreateInstance(type!, [doc]) as BsonValue;
        await Assert.That(instance).IsNotNull();
        
        // Test Properties
        await Assert.That(instance!.BsonType).IsEqualTo(BsonType.Document);
        await Assert.That(instance.IsNull).IsFalse();
        
        // Test ToString
        await Assert.That(instance.ToString()).IsEqualTo(doc.ToString());
        
        // Test Equals
        var other = Activator.CreateInstance(type!, [new BsonDocument().Set("a", 1)]) as BsonValue;
        await Assert.That(instance.Equals(other)).IsTrue();
        await Assert.That(instance.Equals(doc)).IsTrue(); 
        
        // Test CompareTo
        await Assert.That(instance.CompareTo(other)).IsEqualTo(0);
        
        var smaller = Activator.CreateInstance(type!, [new BsonDocument()]) as BsonValue;
        await Assert.That(instance.CompareTo(smaller)).IsGreaterThan(0);
        
        // Test GetHashCode
        await Assert.That(instance.GetHashCode()).IsEqualTo(doc.GetHashCode());
    }
}
