using System;
using System.Globalization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonValueBranchTests
{
    [Test]
    public async Task BsonSymbol_Branch_Coverage()
    {
        var s1 = new BsonSymbol("a");
        var s2 = new BsonSymbol("a");
        var s3 = new BsonSymbol("b");
        
        // Equals(object)
        await Assert.That(s1!.Equals((object)s2))!.IsTrue();
        await Assert.That(s1!.Equals((object)s3))!.IsFalse();
        await Assert.That(s1!.Equals(null!))!.IsFalse();
        await Assert.That(s1!.Equals(new object()))!.IsFalse();
        
        // CompareTo(BsonValue)
        await Assert.That(s1!.CompareTo(null))!.IsEqualTo(1);
        await Assert.That(s1!.CompareTo(new BsonInt32(1)))!.IsNotEqualTo(0); // Different type
    }

    [Test]
    public async Task BsonJavaScript_Branch_Coverage()
    {
        var js1 = new BsonJavaScript("a");
        var js2 = new BsonJavaScript("a");
        var js3 = new BsonJavaScript("b");
        
        await Assert.That(js1!.Equals((object)js2))!.IsTrue();
        await Assert.That(js1!.Equals((object)js3))!.IsFalse();
        await Assert.That(js1!.Equals(null!))!.IsFalse();
        
        await Assert.That(js1!.CompareTo(null))!.IsEqualTo(1);
        await Assert.That(js1!.CompareTo(new BsonInt32(1)))!.IsNotEqualTo(0);
    }

    [Test]
    public async Task BsonJavaScriptWithScope_Branch_Coverage()
    {
        var js1 = new BsonJavaScriptWithScope("a", new BsonDocument());
        var js2 = new BsonJavaScriptWithScope("a", new BsonDocument());
        
        await Assert.That(js1!.Equals((object)js2))!.IsTrue();
        await Assert.That(js1!.Equals(null!))!.IsFalse();
        
        await Assert.That(js1!.CompareTo(null))!.IsEqualTo(1);
        await Assert.That(js1!.CompareTo(new BsonInt32(1)))!.IsNotEqualTo(0);
    }

    [Test]
    public async Task BsonString_Branch_Coverage()
    {
        var s1 = new BsonString("a");
        var s2 = new BsonString("a");
        
        await Assert.That(s1!.Equals((object)s2))!.IsTrue();
        await Assert.That(s1!.Equals(null!))!.IsFalse();
        
        await Assert.That(s1!.CompareTo(null))!.IsEqualTo(1);
        await Assert.That(s1!.CompareTo(new BsonInt32(1)))!.IsNotEqualTo(0);
    }

    [Test]
    public async Task BsonInt32_Branch_Coverage()
    {
        var i1 = new BsonInt32(1);
        var i2 = new BsonInt32(1);
        
        await Assert.That(i1!.Equals((object)i2))!.IsTrue();
        await Assert.That(i1!.Equals(null!))!.IsFalse();
        
        await Assert.That(i1!.CompareTo(null))!.IsEqualTo(1);
        // Compare to other numerics?
        await Assert.That(i1!.CompareTo(new BsonInt64(1)))!.IsEqualTo(0);
        await Assert.That(i1!.CompareTo(new BsonDouble(1.0)))!.IsEqualTo(0);
         await Assert.That(i1!.CompareTo(new BsonString("1")))!.IsNotEqualTo(0);
     }

    [Test]
    public async Task BsonInt32_FromValue_ShouldUseSmallIntCache()
    {
        var cached1 = BsonInt32.FromValue(5);
        var cached2 = BsonInt32.FromValue(5);
        await Assert.That(ReferenceEquals(cached1, cached2)).IsTrue();

        var nonCached1 = BsonInt32.FromValue(1000);
        var nonCached2 = BsonInt32.FromValue(1000);
        await Assert.That(ReferenceEquals(nonCached1, nonCached2)).IsFalse();

        await Assert.That(nonCached1.ToType(typeof(long), CultureInfo.InvariantCulture)).IsEqualTo(1000L);
    }

    [Test]
    public async Task BsonBoolean_Branch_Coverage()
    {
        var t = BsonBoolean.FromValue(true);
        var f = BsonBoolean.FromValue(false);

        await Assert.That(ReferenceEquals(t, BsonBoolean.True)).IsTrue();
        await Assert.That(ReferenceEquals(f, BsonBoolean.False)).IsTrue();

        await Assert.That(t.CompareTo(null)).IsEqualTo(1);
        await Assert.That(t.CompareTo(f)).IsEqualTo(1);
        await Assert.That(t.CompareTo(new BsonInt32(1))).IsLessThan(0);

        await Assert.That(t.GetTypeCode()).IsEqualTo(TypeCode.Boolean);
        await Assert.That(t.ToType(typeof(int), CultureInfo.InvariantCulture)).IsEqualTo(1);
    }

    [Test]
    public async Task BsonDateTime_Branch_Coverage()
    {
        var now = DateTime.UtcNow;
        var d1 = new BsonDateTime(now);
        var d2 = new BsonDateTime(now);
        
        await Assert.That(d1!.Equals((object)d2))!.IsTrue();
        await Assert.That(d1!.Equals(null!))!.IsFalse();
        
        await Assert.That(d1!.CompareTo(null))!.IsEqualTo(1);
        await Assert.That(d1!.CompareTo(new BsonInt32(1)))!.IsNotEqualTo(0);
    }

    [Test]
    public async Task BsonNull_Branch_Coverage()
    {
        var n = BsonNull.Value;
        
        await Assert.That(n!.Equals(BsonNull.Value))!.IsTrue();
        await Assert.That(n!.Equals(null!))!.IsTrue(); // BsonNull equals null (override behavior)
        await Assert.That(n!.Equals(new object()))!.IsFalse();
        
        await Assert.That(n!.CompareTo(null))!.IsEqualTo(0); // BsonNull considers null equal
        await Assert.That(n!.CompareTo(BsonNull.Value))!.IsEqualTo(0);
        await Assert.That(n!.CompareTo(new BsonInt32(1)))!.IsNotEqualTo(0);
    }
    
    [Test]
    public async Task BsonArray_Branch_Coverage()
    {
        var a1 = new BsonArray().AddValue(1);
        var a2 = new BsonArray().AddValue(1);
        
        await Assert.That(a1!.Equals((object)a2))!.IsTrue();
        await Assert.That(a1!.Equals(null!))!.IsFalse();
        
        await Assert.That(a1!.CompareTo(null))!.IsEqualTo(1);
        await Assert.That(a1!.CompareTo(new BsonInt32(1)))!.IsNotEqualTo(0);
    }
}
