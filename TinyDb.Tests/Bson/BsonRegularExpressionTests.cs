using System.Text.RegularExpressions;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonRegularExpressionTests
{
    [Test]
    public async Task Constructor_And_Properties_ShouldWork()
    {
        var regex = new BsonRegularExpression("^test$", "im");
        await Assert.That(regex.Pattern).IsEqualTo("^test$");
        await Assert.That(regex.Options).IsEqualTo("im");
        
        var regex2 = new BsonRegularExpression("^test$");
        await Assert.That(regex2.Options).IsEqualTo("");
        
        var regex3 = BsonRegularExpression.FromRegex(new Regex("^test$", RegexOptions.IgnoreCase | RegexOptions.Multiline));
        await Assert.That(regex3.Pattern).IsEqualTo("^test$");
        await Assert.That(regex3.Options).Contains("i");
        await Assert.That(regex3.Options).Contains("m");
    }

    [Test]
    public async Task ToRegex_ShouldConvertBack()
    {
        var bsonRegex = new BsonRegularExpression("abc", "i");
        var regex = bsonRegex.ToRegex();
        
        await Assert.That(regex.ToString()).IsEqualTo("abc");
        await Assert.That(regex.Options).IsEqualTo(RegexOptions.IgnoreCase);
    }

    [Test]
    public async Task ToString_ShouldFormatCorrectly()
    {
        var regex = new BsonRegularExpression("abc", "im");
        await Assert.That(regex.ToString()).IsEqualTo("/abc/im");
    }
    
    [Test]
    public async Task Comparison_And_Equality()
    {
        var r1 = new BsonRegularExpression("abc", "i");
        var r2 = new BsonRegularExpression("abc", "i");
        var r3 = new BsonRegularExpression("abc", "");
        var r4 = new BsonRegularExpression("def", "i");
        
        await Assert.That(r1).IsEqualTo(r2);
        await Assert.That(r1).IsNotEqualTo(r3);
        await Assert.That(r1).IsNotEqualTo(r4);
        
        await Assert.That(r1.CompareTo(r2)).IsEqualTo(0);
        await Assert.That(r1.CompareTo(r3)).IsGreaterThan(0); // "i" > "" ? Check impl
        // Actually options are compared after pattern.
    }
}
