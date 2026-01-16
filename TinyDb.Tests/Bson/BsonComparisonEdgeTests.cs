using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonComparisonEdgeTests
{
    [Test]
    public async Task BsonDocument_CompareTo_DifferentLengths()
    {
        var doc1 = new BsonDocument().Set("a", 1);
        var doc2 = new BsonDocument().Set("a", 1).Set("b", 2);
        
        await Assert.That(doc1.CompareTo(doc2)).IsLessThan(0);
        await Assert.That(doc2.CompareTo(doc1)).IsGreaterThan(0);
    }

    [Test]
    public async Task BsonDocument_CompareTo_SameLength_DifferentKeys()
    {
        var doc1 = new BsonDocument().Set("a", 1).Set("b", 2);
        var doc2 = new BsonDocument().Set("a", 1).Set("c", 2);
        
        // Keys are "a", "b" vs "a", "c"
        // In the loop, when kvp.Key is "b", it's not in doc2, returns 1
        await Assert.That(doc1.CompareTo(doc2)).IsEqualTo(1);
    }

    [Test]
    public async Task BsonArray_CompareTo_DifferentLengths()
    {
        var arr1 = new BsonArray(new BsonValue[] { 1 });
        var arr2 = new BsonArray(new BsonValue[] { 1, 2 });
        
        await Assert.That(arr1.CompareTo(arr2)).IsLessThan(0);
        await Assert.That(arr2.CompareTo(arr1)).IsGreaterThan(0);
    }

    [Test]
    public async Task BsonValue_Null_Comparison()
    {
        BsonValue v = 10;
        await Assert.That(v.CompareTo(null)).IsEqualTo(1);
        // BsonNull.CompareTo(null) is 0 because BsonNull is equivalent to null
        await Assert.That(BsonNull.Value.CompareTo(null)).IsEqualTo(0);
    }
}
