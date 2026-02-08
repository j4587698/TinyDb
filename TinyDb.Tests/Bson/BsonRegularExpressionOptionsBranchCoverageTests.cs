using System.Text.RegularExpressions;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonRegularExpressionOptionsBranchCoverageTests
{
    [Test]
    public async Task ToRegex_WithUnknownOption_ShouldIgnoreAndStillParseKnownOptions()
    {
        var bson = new BsonRegularExpression("a+", "zim");
        var regex = bson.ToRegex();

        await Assert.That(regex.Options.HasFlag(RegexOptions.IgnoreCase)).IsTrue();
        await Assert.That(regex.Options.HasFlag(RegexOptions.Multiline)).IsTrue();
    }

    [Test]
    public async Task ToRegex_WithUnknownOptionInBetween_ShouldIgnore()
    {
        var bson = new BsonRegularExpression("a+", "n");
        var regex = bson.ToRegex();

        await Assert.That(regex.Options).IsEqualTo(RegexOptions.None);
    }
}
