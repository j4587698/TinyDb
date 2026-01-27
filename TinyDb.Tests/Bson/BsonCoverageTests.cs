using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Tests.Utils;
using TUnit.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonCoverageTests
{
    // --- BsonDocument Tests ---

    [Test]
    public async Task BsonDocument_IConvertible_Coverage()
    {
        var doc = new BsonDocument().Set("a", 1).Set("b", 2);
        
        // Test ToBoolean
        await Assert.That(doc.ToBoolean(null)).IsTrue();
        await Assert.That(new BsonDocument().ToBoolean(null)).IsFalse();

        // Test Number conversions (returns Count)
        await Assert.That(doc.ToInt32(null)).IsEqualTo(2);
        await Assert.That(doc.ToInt64(null)).IsEqualTo(2L);
        await Assert.That(doc.ToDouble(null)).IsEqualTo(2.0);
        await Assert.That(doc.ToDecimal(null)).IsEqualTo(2m);
        await Assert.That(doc.ToSingle(null)).IsEqualTo(2f);
        await Assert.That(doc.ToByte(null)).IsEqualTo((byte)2);
        await Assert.That(doc.ToInt16(null)).IsEqualTo((short)2);
        
        // Test invalid
        try { doc.ToDateTime(null); Assert.Fail("Should throw InvalidCastException"); } catch (InvalidCastException) {}
    }

    [Test]
    public async Task BsonDocument_Dictionary_Conversion_Coverage()
    {
        var dict = new Dictionary<string, object?>
        {
            { "str", "value" },
            { "int", 123 },
            { "long", 123L },
            { "bool", true },
            { "double", 123.45 },
            { "float", 123.45f },
            { "date", new DateTime(2025, 1, 1) },
            { "oid", ObjectId.NewObjectId() },
            { "null", null },
            { "list", new List<object?> { 1, "a" } },
            { "subdoc", new Dictionary<string, object?> { { "a", 1 } } }
        };

        var doc = BsonDocument.FromDictionary(dict);
        await Assert.That(doc.Count).IsEqualTo(dict.Count);
        
        // Test round trip
        var roundTrip = doc.ToDictionary();
        await Assert.That(roundTrip["str"]).IsEqualTo("value");
        await Assert.That(roundTrip["list"]).IsTypeOf<List<object?>>();
        
        // Test implicit operator
        BsonDocument implicitDoc = dict;
        await Assert.That(implicitDoc.Count).IsEqualTo(dict.Count);
    }
    
    [Test]
    public void BsonDocument_NotSupported_Mutable_Methods()
    {
        var doc = new BsonDocument();
        try { doc.Add("a", 1); Assert.Fail("Add"); } catch (NotSupportedException) {}
        try { doc.Remove("a"); Assert.Fail("Remove"); } catch (NotSupportedException) {}
        try { doc.Clear(); Assert.Fail("Clear"); } catch (NotSupportedException) {}
        try { doc["a"] = 1; Assert.Fail("Indexer Set"); } catch (NotSupportedException) {}
        try { doc.Add(new KeyValuePair<string, BsonValue>("a", 1)); Assert.Fail("KVP Add"); } catch (NotSupportedException) {}
        try { doc.Remove(new KeyValuePair<string, BsonValue>("a", 1)); Assert.Fail("KVP Remove"); } catch (NotSupportedException) {}
    }

    [Test]
    public async Task BsonDocument_Get_Coverage()
    {
        var doc = new BsonDocument().Set("a", 1);
        await Assert.That(doc.Get("a")).IsEqualTo((BsonValue)1);
        await Assert.That(doc.Get("b", 2)).IsEqualTo((BsonValue)2);
        await Assert.That(doc.Get("c")).IsEqualTo(BsonNull.Value);
    }

    // --- BsonArray Tests ---

    [Test]
    public async Task BsonArray_IConvertible_Coverage()
    {
        var arr = new BsonArray().AddValue(1).AddValue(2);

        // Test ToBoolean
        await Assert.That(arr.ToBoolean(null)).IsTrue();
        await Assert.That(new BsonArray().ToBoolean(null)).IsFalse();

        // Test Number conversions (returns Count)
        await Assert.That(arr.ToInt32(null)).IsEqualTo(2);
        await Assert.That(arr.ToInt64(null)).IsEqualTo(2L);
        await Assert.That(arr.ToDouble(null)).IsEqualTo(2.0);
        await Assert.That(arr.ToDecimal(null)).IsEqualTo(2m);
        await Assert.That(arr.ToSingle(null)).IsEqualTo(2f);
        await Assert.That(arr.ToByte(null)).IsEqualTo((byte)2);
        await Assert.That(arr.ToInt16(null)).IsEqualTo((short)2);
        
        // Test invalid
        try { arr.ToDateTime(null); Assert.Fail("Should throw"); } catch (InvalidCastException) {}
    }

    [Test]
    public async Task BsonArray_List_Conversion_Coverage()
    {
        var list = new List<object?> { 1, "a", null, true };
        var arr = BsonArray.FromList(list);
        await Assert.That(arr.Count).IsEqualTo(list.Count);

        var roundTrip = arr.ToList();
        await Assert.That(roundTrip.Count).IsEqualTo(list.Count);
        await Assert.That(roundTrip[1]).IsEqualTo("a");

        // Implicit
        BsonArray implicitArr = list;
        await Assert.That(implicitArr.Count).IsEqualTo(list.Count);
        
        object?[] objArr = new object?[] { 1, 2 };
        BsonArray implicitObjArr = objArr;
        await Assert.That(implicitObjArr.Count).IsEqualTo(2);
    }

    [Test]
    public void BsonArray_NotSupported_Mutable_Methods()
    {
        var arr = new BsonArray();
        try { arr.Add(1); Assert.Fail("Add"); } catch (NotSupportedException) {}
        try { arr.Insert(0, 1); Assert.Fail("Insert"); } catch (NotSupportedException) {}
        try { arr.Remove(1); Assert.Fail("Remove"); } catch (NotSupportedException) {}
        try { arr.RemoveAt(0); Assert.Fail("RemoveAt"); } catch (NotSupportedException) {}
        try { arr.Clear(); Assert.Fail("Clear"); } catch (NotSupportedException) {}
        try { arr[0] = 1; Assert.Fail("Indexer"); } catch (NotSupportedException) {}
    }

    [Test]
    public async Task BsonArray_Get_And_Equality_Coverage()
    {
        var arr = new BsonArray().AddValue(1).AddValue(2);
        await Assert.That(arr.Get(0)).IsEqualTo((BsonValue)1);
        
        var arr2 = new BsonArray().AddValue(1).AddValue(2);
        await Assert.That(arr.Equals(arr2)).IsTrue();
        await Assert.That(arr.GetHashCode()).IsEqualTo(arr2.GetHashCode());
        await Assert.That(arr.CompareTo(arr2)).IsEqualTo(0);
        
        var arr3 = new BsonArray().AddValue(1);
        await Assert.That(arr.Equals(arr3)).IsFalse();
        await Assert.That(arr.CompareTo(arr3)).IsGreaterThan(0); // Count 2 > 1
    }

    [Test]
    [SkipInAot("Uses reflection to instantiate internal type BsonDocumentValue")]
    public async Task BsonDocumentValue_Coverage()
    {
        var doc = new BsonDocument().Set("a", 1);
        // Using reflection to instantiate internal type BsonDocumentValue
        var type = typeof(BsonDocument).Assembly.GetType("TinyDb.Bson.BsonDocumentValue");
        if (type != null)
        {
            var instance = Activator.CreateInstance(type, doc) as BsonValue;
            await Assert.That(instance).IsNotNull();
            await Assert.That(instance!.BsonType).IsEqualTo(BsonType.Document);
            await Assert.That(instance.IsDocument).IsTrue();
            await Assert.That(ReferenceEquals(instance.RawValue, doc)).IsTrue();
            
            // Test wrapper methods
            await Assert.That(instance.ToString()).IsEqualTo(doc.ToString());
            await Assert.That(instance.GetHashCode()).IsEqualTo(doc.GetHashCode());
            
            var instance2 = Activator.CreateInstance(type, doc) as BsonValue;
            await Assert.That(instance.Equals(instance2)).IsTrue();
        }
    }
    
    [Test]
    [SkipInAot("Uses reflection to instantiate internal type BsonArrayValue")]
    public async Task BsonArrayValue_Coverage()
    {
        var arr = new BsonArray().AddValue(1);
        var type = typeof(BsonArray).Assembly.GetType("TinyDb.Bson.BsonArrayValue");
        if (type != null)
        {
            var instance = Activator.CreateInstance(type, arr) as BsonValue;
            await Assert.That(instance).IsNotNull();
            await Assert.That(instance!.BsonType).IsEqualTo(BsonType.Array);
            await Assert.That(instance.IsArray).IsTrue();
            await Assert.That(ReferenceEquals(instance.RawValue, arr)).IsTrue();
            
            await Assert.That(instance.ToString()).IsEqualTo(arr.ToString());
            await Assert.That(instance.GetHashCode()).IsEqualTo(arr.GetHashCode());
            
            var instance2 = Activator.CreateInstance(type, arr) as BsonValue;
            await Assert.That(instance.Equals(instance2)).IsTrue();
        }
    }

    [Test]
    [SkipInAot("Uses reflection to instantiate internal type BsonDocumentValue")]
    public async Task BsonDocumentValue_IConvertible_Coverage()
    {
        var doc = new BsonDocument().Set("a", 1);
        var type = typeof(BsonDocument).Assembly.GetType("TinyDb.Bson.BsonDocumentValue");
        if (type != null)
        {
            var instance = Activator.CreateInstance(type, doc) as BsonValue;
            // Test ToBoolean
            await Assert.That(instance.ToBoolean(null)).IsTrue();
            // Test ToInt32
            await Assert.That(instance.ToInt32(null)).IsEqualTo(1);
        }
    }

    [Test]
    public async Task BsonValue_Types_Coverage()
    {
        // BsonString
        var str = new BsonString("test");
        await Assert.That(str.CompareTo(new BsonString("test"))).IsEqualTo(0);
        await Assert.That(str.CompareTo(new BsonString("other"))).IsGreaterThan(0);
        await Assert.That(str.ToString()).IsEqualTo("test");
        
        // BsonBoolean
        var b = new BsonBoolean(true);
        await Assert.That(b.ToBoolean(null)).IsTrue();
        await Assert.That(b.CompareTo(new BsonBoolean(true))).IsEqualTo(0);
        
        // BsonDateTime
        var now = DateTime.UtcNow;
        var dt = new BsonDateTime(now);
        // Compare ticks or use tolerance if needed, but BsonDateTime stores DateTime directly
        await Assert.That(dt.ToDateTime(null)).IsEqualTo(now);
        await Assert.That(dt.CompareTo(new BsonDateTime(now))).IsEqualTo(0);
        
        // BsonBinary
        var bin = new BsonBinary(new byte[] { 1, 2, 3 });
        await Assert.That(bin.Value.Length).IsEqualTo(3);
        await Assert.That(bin.ToString()).IsNotEmpty();
        await Assert.That(bin.CompareTo(new BsonBinary(new byte[] { 1, 2, 3 }))).IsEqualTo(0);
        
        // BsonNull
        await Assert.That(BsonNull.Value.ToBoolean(null)).IsFalse();
        await Assert.That(BsonNull.Value.CompareTo(BsonNull.Value)).IsEqualTo(0);
    }

    [Test]
    public async Task BsonDocument_Comparison_And_Contains_Coverage()
    {
        var doc1 = new BsonDocument().Set("a", 1);
        var doc2 = new BsonDocument().Set("b", 1);
        
        // Count equal, keys different
        // doc1 has "a", doc2 does not. returns 1.
        await Assert.That(doc1.CompareTo(doc2)).IsGreaterThan(0); 
        
        var doc3 = new BsonDocument().Set("a", 2);
        // Count equal, keys equal, values different
        await Assert.That(doc1.CompareTo(doc3)).IsLessThan(0); // 1 vs 2
        
        // Contains
        await Assert.That(doc1.ContainsKey("a")).IsTrue();
        await Assert.That(doc1.ContainsKey("z")).IsFalse();
        await Assert.That(doc1.Contains(new BsonInt32(1))).IsTrue();
        await Assert.That(doc1.Contains(new BsonInt32(99))).IsFalse();
        await Assert.That(doc1.Contains(new KeyValuePair<string, BsonValue>("a", 1))).IsTrue();
        await Assert.That(doc1.Contains(new KeyValuePair<string, BsonValue>("a", 2))).IsFalse();
    }

    [Test]
    public async Task BsonArray_Extended_Coverage()
    {
        var arr = new BsonArray().AddValue(1).AddValue(2);
        
        // Compare same count, different values
        var arr2 = new BsonArray().AddValue(1).AddValue(3);
        await Assert.That(arr.CompareTo(arr2)).IsLessThan(0);
        
        // IndexOf
        await Assert.That(arr.IndexOf(1)).IsEqualTo(0);
        await Assert.That(arr.IndexOf(99)).IsEqualTo(-1);
        
        // Contains
        await Assert.That(arr.Contains(2)).IsTrue();
        await Assert.That(arr.Contains(99)).IsFalse();
        
        // CopyTo
        var copy = new BsonValue[2];
        arr.CopyTo(copy, 0);
        await Assert.That(copy[0]).IsEqualTo((BsonValue)1);
    }
}