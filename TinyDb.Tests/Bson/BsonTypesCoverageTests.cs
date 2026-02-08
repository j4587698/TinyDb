using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonTypesCoverageTests
{
    [Test]
    public async Task BsonJavaScript_Coverage()
    {
        var js1 = new BsonJavaScript("code");
        var js2 = new BsonJavaScript("code");
        var js3 = new BsonJavaScript("other");
        
        await Assert.That(js1.Code).IsEqualTo("code");
        await Assert.That(js1).IsEqualTo(js2);
        await Assert.That(js1).IsNotEqualTo(js3);
        await Assert.That(js1.GetHashCode()).IsEqualTo(js2.GetHashCode());
        await Assert.That(js1.ToString()).IsEqualTo("code");
        await Assert.That(js1.CompareTo(js2)).IsEqualTo(0);
        await Assert.That(js1.CompareTo(js3)).IsLessThan(0); // 'c' < 'o'
    }

    [Test]
    public async Task BsonSymbol_Coverage()
    {
        var s1 = new BsonSymbol("sym");
        var s2 = new BsonSymbol("sym");
        var s3 = new BsonSymbol("other");
        
        await Assert.That(s1.Name).IsEqualTo("sym");
        await Assert.That(s1).IsEqualTo(s2);
        await Assert.That(s1).IsNotEqualTo(s3);
        await Assert.That(s1.GetHashCode()).IsEqualTo(s2.GetHashCode());
        await Assert.That(s1.ToString()).IsEqualTo("sym");
        await Assert.That(s1.CompareTo(s2)).IsEqualTo(0);
        await Assert.That(s1.CompareTo(s3)).IsGreaterThan(0); // 's' > 'o'

        await Assert.That(() => new BsonSymbol(null!))
            .ThrowsExactly<ArgumentNullException>();

        await Assert.That(s1.ToType(typeof(string), provider: null))
            .IsEqualTo("sym");

        await Assert.That(() => s1.ToType(typeof(int), provider: null))
            .Throws<InvalidCastException>();

        await Assert.That(s1.CompareTo((BsonValue?)null)).IsEqualTo(1);
        await Assert.That(s1.CompareTo((BsonValue)new BsonInt32(1))).IsNotEqualTo(0);

        await Assert.That(s1.Equals((BsonValue?)s2)).IsTrue();
        await Assert.That(s1.Equals((BsonValue?)new BsonInt32(1))).IsFalse();
    }

    [Test]
    public async Task BsonJavaScriptWithScope_Coverage()
    {
        var scope1 = new BsonDocument().Set("a", 1);
        var scope2 = new BsonDocument().Set("a", 1);
        var scope3 = new BsonDocument().Set("a", 2);
        
        var js1 = new BsonJavaScriptWithScope("code", scope1);
        var js2 = new BsonJavaScriptWithScope("code", scope2);
        var js3 = new BsonJavaScriptWithScope("code", scope3);
        
        await Assert.That(js1.Code).IsEqualTo("code");
        await Assert.That(js1.Scope).IsEqualTo(scope1);
        await Assert.That(js1).IsEqualTo(js2);
        await Assert.That(js1).IsNotEqualTo(js3);
        await Assert.That(js1.GetHashCode()).IsEqualTo(js2.GetHashCode());
        // ToString might vary
        await Assert.That(js1.CompareTo(js2)).IsEqualTo(0);
    }
}
