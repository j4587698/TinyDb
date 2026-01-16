using System;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonStructureTests
{
    [Test]
    public async Task BsonDocument_Manipulations_ShouldWork()
    {
        var doc = new BsonDocument();
        doc = doc.Set("a", 1).Set("b", 2);
        
        await Assert.That(doc.Count).IsEqualTo(2);
        await Assert.That(doc.ContainsKey("a")).IsTrue();
        await Assert.That(doc.ContainsKey("c")).IsFalse();
        
        var doc2 = doc.RemoveKey("a");
        await Assert.That(doc2.Count).IsEqualTo(1);
        await Assert.That(doc2.ContainsKey("a")).IsFalse();
        await Assert.That(doc.Count).IsEqualTo(2); // Original doc unchanged
        
        var clone = doc.Clone();
        await Assert.That(clone.Equals(doc)).IsTrue();
        await Assert.That(ReferenceEquals(clone, doc)).IsFalse();
    }

    [Test]
    public async Task BsonArray_Manipulations_ShouldWork()
    {
        var arr = new BsonArray(new BsonValue[] { 1, 2, 3 });
        
        await Assert.That(arr.Count).IsEqualTo(3);
        await Assert.That(arr.Contains(2)).IsTrue();
        
        var arr2 = arr.AddValue(4);
        await Assert.That(arr2.Count).IsEqualTo(4);
        await Assert.That(arr2[3].ToInt32(null)).IsEqualTo(4);
        
        var arr3 = arr.RemoveValue(2);
        await Assert.That(arr3.Count).IsEqualTo(2);
        await Assert.That(arr3.Contains(2)).IsFalse();
        
        var arr4 = arr.InsertValue(1, 10);
        await Assert.That(arr4[1].ToInt32(null)).IsEqualTo(10);
        await Assert.That(arr4.Count).IsEqualTo(4);
    }

    [Test]
    public async Task BsonDocument_Dictionary_Operations_ShouldWork()
    {
        var dict = new Dictionary<string, BsonValue> { ["x"] = 100 };
        var doc = new BsonDocument(dict);
        
        await Assert.That(doc["x"].ToInt32(null)).IsEqualTo(100);
        
        IDictionary<string, BsonValue> idoc = doc;
        await Assert.That(idoc.IsReadOnly).IsTrue();
        await Assert.That(idoc.Keys).Count().IsEqualTo(1);
        await Assert.That(idoc.Values).Count().IsEqualTo(1);
    }

    [Test]
    public async Task BsonValue_Equality_ShouldWork()
    {
        BsonValue v1 = "test";
        BsonValue v2 = "test";
        BsonValue v3 = "other";
        
        await Assert.That(v1 == v2).IsTrue();
        await Assert.That(v1 != v3).IsTrue();
        // v1.Equals("test") returns true because of implicit conversion of "test" to BsonValue
        await Assert.That(v1.Equals("test")).IsTrue(); 
        await Assert.That(v1.Equals(v2)).IsTrue();
        
        // Test equality with something that doesn't have implicit conversion
        await Assert.That(v1.Equals(123)).IsFalse();
    }
}
