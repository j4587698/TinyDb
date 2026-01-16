using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonDocumentExtendedTests
{
    [Test]
    public async Task IConvertible_Implementation_ShouldWork()
    {
        var doc = new BsonDocument()
            .Set("key1", 1)
            .Set("key2", 2);

        // Count is 2
        await Assert.That(doc.ToBoolean(null)).IsTrue();
        await Assert.That(doc.ToInt32(null)).IsEqualTo(2);
        await Assert.That(doc.ToDouble(null)).IsEqualTo(2.0);
        
        var json = doc.ToString(null);
        await Assert.That(json).Contains("\"key1\": 1");
        await Assert.That(json).Contains("\"key2\": 2");
        
        await Assert.That(doc.ToByte(null)).IsEqualTo((byte)2);
        // ... cover others if needed, but int/bool/string are key
        
        var emptyDoc = new BsonDocument();
        await Assert.That(emptyDoc.ToBoolean(null)).IsFalse(); // Count 0 -> False
        await Assert.That(emptyDoc.ToInt32(null)).IsEqualTo(0);
    }

    [Test]
    public async Task Dictionary_Conversions_ShouldWork()
    {
        var dict = new Dictionary<string, object?>
        {
            { "str", "value" },
            { "int", 123 },
            { "null", null }
        };

        // Implicit conversion
        BsonDocument doc = dict;
        await Assert.That(doc.Count).IsEqualTo(3);
        await Assert.That(doc["str"].ToString()).IsEqualTo("value");
        await Assert.That(doc["int"]).IsEqualTo(new BsonInt32(123));
        await Assert.That(doc["null"]).IsEqualTo(BsonNull.Value);

        // ToDictionary
        var back = doc.ToDictionary();
        await Assert.That(back.Count).IsEqualTo(3);
        await Assert.That(back["str"]).IsEqualTo("value");
        await Assert.That(back["int"]).IsEqualTo(123);
        await Assert.That(back["null"]).IsNull();
    }

    [Test]
    public async Task CompareTo_And_Equals_EdgeCases()
    {
        var doc1 = new BsonDocument().Set("a", 1);
        var doc2 = new BsonDocument().Set("a", 1);
        var doc3 = new BsonDocument().Set("a", 2);
        var doc4 = new BsonDocument().Set("a", 1).Set("b", 2);

        // Equal
        await Assert.That(doc1.CompareTo(doc2)).IsEqualTo(0);
        await Assert.That(doc1.Equals(doc2)).IsTrue();

        // Count mismatch
        await Assert.That(doc1.CompareTo(doc4)).IsNotEqualTo(0); // doc4 has more items
        await Assert.That(doc1.Equals(doc4)).IsFalse();

        // Value mismatch
        await Assert.That(doc1.CompareTo(doc3)).IsNotEqualTo(0);
        await Assert.That(doc1.Equals(doc3)).IsFalse();
        
        // Key mismatch (implicit in CompareTo loop or Get)
        var doc5 = new BsonDocument().Set("b", 1);
        await Assert.That(doc1.Equals(doc5)).IsFalse();
    }
    
    [Test]
    public async Task Comparison_With_Other_BsonTypes()
    {
        var doc = new BsonDocument();
        var i = new BsonInt32(1);
        
        // Document vs non-document: compare BsonType
        // Document type is usually > Int32 type
        await Assert.That(doc.CompareTo(i)).IsNotEqualTo(0);
    }
}
