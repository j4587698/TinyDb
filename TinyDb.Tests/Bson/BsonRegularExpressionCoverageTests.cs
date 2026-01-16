using System.Text.RegularExpressions;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonRegularExpressionCoverageTests
{
    [Test]
    public async Task Constructor_InvalidArguments_ShouldThrow()
    {
        await Assert.That(() => new BsonRegularExpression(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task FromRegex_AllOptions_ShouldWork()
    {
        var options = RegexOptions.IgnoreCase | RegexOptions.Multiline | 
                      RegexOptions.Singleline | RegexOptions.IgnorePatternWhitespace;
        var regex = new Regex("pattern", options);
        var bson = BsonRegularExpression.FromRegex(regex);
        
        await Assert.That(bson.Pattern).IsEqualTo("pattern");
        await Assert.That(bson.Options).Contains("i");
        await Assert.That(bson.Options).Contains("m");
        await Assert.That(bson.Options).Contains("s");
        await Assert.That(bson.Options).Contains("x");
        
        var back = bson.ToRegex();
        await Assert.That(back.Options.HasFlag(RegexOptions.IgnoreCase)).IsTrue();
        await Assert.That(back.Options.HasFlag(RegexOptions.Multiline)).IsTrue();
        await Assert.That(back.Options.HasFlag(RegexOptions.Singleline)).IsTrue();
        await Assert.That(back.Options.HasFlag(RegexOptions.IgnorePatternWhitespace)).IsTrue();
    }

    [Test]
    public async Task Implicits_And_ToString()
    {
        BsonRegularExpression bson = "mypattern";
        await Assert.That(bson.Pattern).IsEqualTo("mypattern");
        await Assert.That(bson.Options).IsEqualTo("");
        
        BsonRegularExpression bson2 = new Regex("p2");
        await Assert.That(bson2.Pattern).IsEqualTo("p2");
        
        await Assert.That(bson.ToString()).IsEqualTo("/mypattern/");
        await Assert.That(new BsonRegularExpression("p", "i").ToString()).IsEqualTo("/p/i");
    }

    [Test]
    public async Task Comparisons_And_Equality()
    {
        var bson1 = new BsonRegularExpression("a", "i");
        var bson2 = new BsonRegularExpression("a", "m");
        var bson3 = new BsonRegularExpression("b", "i");
        
        await Assert.That(bson1.Equals(new BsonRegularExpression("a", "i"))).IsTrue();
        await Assert.That(bson1.Equals(bson2)).IsFalse();
        
        await Assert.That(bson1.CompareTo(bson2)).IsNegative();
        await Assert.That(bson3.CompareTo(bson1)).IsPositive();
        await Assert.That(bson1.CompareTo(new BsonInt32(1))).IsNotEqualTo(0);
        
        await Assert.That(bson1.GetHashCode()).IsEqualTo(new BsonRegularExpression("a", "i").GetHashCode());
    }

    [Test]
    public async Task IConvertible_Exceptions()
    {
        var bson = new BsonRegularExpression("p");
        await Assert.That(() => bson.ToBoolean(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToByte(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToInt32(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToType(typeof(int), null)).Throws<InvalidCastException>();
    }
}
