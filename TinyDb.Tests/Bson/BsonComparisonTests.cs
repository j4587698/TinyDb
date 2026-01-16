using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonComparisonTests
{
    [Test]
    public async Task BsonDocument_CompareTo_ShouldWork()
    {
        var doc1 = new BsonDocument().Set("a", 1).Set("b", 2);
        var doc2 = new BsonDocument().Set("a", 1).Set("b", 2);
        var doc3 = new BsonDocument().Set("a", 1).Set("b", 3);
        var doc4 = new BsonDocument().Set("a", 1);
        
        await Assert.That(doc1.CompareTo(doc2)).IsEqualTo(0);
        await Assert.That(doc1.CompareTo(doc3)).IsLessThan(0);
        await Assert.That(doc3.CompareTo(doc1)).IsGreaterThan(0);
        await Assert.That(doc1.CompareTo(doc4)).IsGreaterThan(0);
        await Assert.That(doc1.CompareTo(null)).IsEqualTo(1);
        
        // Different keys
        var doc5 = new BsonDocument().Set("a", 1).Set("c", 0);
        await Assert.That(doc1.CompareTo(doc5)).IsGreaterThan(0); // Because "b" is not in doc5
    }

    [Test]
    public async Task BsonArray_CompareTo_ShouldWork()
    {
        var arr1 = new BsonArray(new BsonValue[] { 1, 2 });
        var arr2 = new BsonArray(new BsonValue[] { 1, 2 });
        var arr3 = new BsonArray(new BsonValue[] { 1, 3 });
        var arr4 = new BsonArray(new BsonValue[] { 1 });
        
        await Assert.That(arr1.CompareTo(arr2)).IsEqualTo(0);
        await Assert.That(arr1.CompareTo(arr3)).IsLessThan(0);
        await Assert.That(arr3.CompareTo(arr1)).IsGreaterThan(0);
        await Assert.That(arr1.CompareTo(arr4)).IsGreaterThan(0);
    }

    [Test]
    public async Task BsonValue_CrossType_Comparison_ShouldWork()
    {
        BsonValue v1 = 10;
        BsonValue v2 = "10";
        
        // Comparison between different types follows BsonType order
        // Int32 (0x10) > String (0x02)
        await Assert.That(v1.CompareTo(v2)).IsNotEqualTo(0);
        await Assert.That(v1.BsonType > v2.BsonType).IsTrue();
        await Assert.That(v1.CompareTo(v2)).IsGreaterThan(0);
    }
}
