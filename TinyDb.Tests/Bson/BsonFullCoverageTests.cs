using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonFullCoverageTests
{
    [Test]
    public async Task BsonDocument_Misc_ShouldWork()
    {
        var doc = new BsonDocument();
        doc = doc.Set("a", 1).Set("b", 2);
        
        await Assert.That(doc.GetHashCode()).IsNotEqualTo(0);
        await Assert.That(doc.ToString()).Contains("\"a\": 1");
        
        var other = new BsonDocument().Set("a", 1).Set("b", 2);
        await Assert.That(doc.Equals(other)).IsTrue();
        await Assert.That(doc.Equals((object)other)).IsTrue();
        
        var diff = new BsonDocument().Set("a", 1).Set("b", 3);
        await Assert.That(doc.Equals(diff)).IsFalse();
        
        var diffKeys = new BsonDocument().Set("a", 1).Set("c", 2);
        await Assert.That(doc.Equals(diffKeys)).IsFalse();
    }

    [Test]
    public async Task BsonArray_Misc_ShouldWork()
    {
        var arr = new BsonArray(new BsonValue[] { 1, 2 });
        await Assert.That(arr.GetHashCode()).IsNotEqualTo(0);
        await Assert.That(arr.ToString()).IsEqualTo("[1, 2]");
        
        var other = new BsonArray(new BsonValue[] { 1, 2 });
        await Assert.That(arr.Equals(other)).IsTrue();
        
        var diff = new BsonArray(new BsonValue[] { 1, 3 });
        await Assert.That(arr.Equals(diff)).IsFalse();
        
        var diffLen = new BsonArray(new BsonValue[] { 1 });
        await Assert.That(arr.Equals(diffLen)).IsFalse();
    }

    [Test]
    public async Task BsonValue_Conversion_EdgeCases()
    {
        BsonValue v = 123;
        await Assert.That(v.As<int>()).IsEqualTo(123);
        await Assert.That(() => v.As<string>()).Throws<InvalidCastException>();
        
        int val;
        await Assert.That(v.TryAs<int>(out val)).IsTrue();
        await Assert.That(val).IsEqualTo(123);
        
        string s;
        await Assert.That(v.TryAs<string>(out s!)).IsFalse();
    }
}
