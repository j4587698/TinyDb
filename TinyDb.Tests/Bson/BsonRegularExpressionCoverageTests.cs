using System.Text.RegularExpressions;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonRegularExpressionCoverageTests
{
    [Test]
    public async Task Ctor_NullPattern_ShouldThrow()
    {
        await Assert.That(() => new BsonRegularExpression(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Ctor_NullOptions_ShouldDefaultToEmpty()
    {
        var bson = new BsonRegularExpression("ab", null!);
        await Assert.That(bson.Options).IsEqualTo(string.Empty);
    }

    [Test]
    public async Task FromRegex_Null_ShouldThrow()
    {
        await Assert.That(() => BsonRegularExpression.FromRegex(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task FromRegex_And_ToRegex_ShouldRoundTripOptions()
    {
        var regex = new Regex(
            "ab",
            RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Singleline | RegexOptions.IgnorePatternWhitespace);

        var bson = BsonRegularExpression.FromRegex(regex);
        var roundtrip = bson.ToRegex();

        await Assert.That(roundtrip.ToString()).IsEqualTo("ab");
        await Assert.That(roundtrip.Options.HasFlag(RegexOptions.IgnoreCase)).IsTrue();
        await Assert.That(roundtrip.Options.HasFlag(RegexOptions.Multiline)).IsTrue();
        await Assert.That(roundtrip.Options.HasFlag(RegexOptions.Singleline)).IsTrue();
        await Assert.That(roundtrip.Options.HasFlag(RegexOptions.IgnorePatternWhitespace)).IsTrue();
    }

    [Test]
    public async Task Operators_Equals_CompareTo_ShouldWork()
    {
        BsonRegularExpression bson = "ab";
        BsonRegularExpression fromRegex = new Regex("ab", RegexOptions.IgnoreCase);

        await Assert.That(bson.Equals(fromRegex)).IsFalse();
        await Assert.That(bson.CompareTo(null)).IsEqualTo(1);
        await Assert.That(bson.CompareTo(fromRegex)).IsLessThan(0);

        await Assert.That(bson.ToString()).IsEqualTo("/ab/");
        await Assert.That(((BsonValue)bson).RawValue).IsEqualTo("ab");
        await Assert.That(bson.GetHashCode()).IsNotEqualTo(0);

        await Assert.That(bson.CompareTo(new BsonInt32(1))).IsNotEqualTo(0);
    }

    [Test]
    public async Task ParseOptions_UnknownFlags_ShouldBeIgnored()
    {
        var bson = new BsonRegularExpression("ab", "imxsJZ");
        var regex = bson.ToRegex();

        await Assert.That(regex.Options.HasFlag(RegexOptions.IgnoreCase)).IsTrue();
        await Assert.That(regex.Options.HasFlag(RegexOptions.Multiline)).IsTrue();
        await Assert.That(regex.Options.HasFlag(RegexOptions.Singleline)).IsTrue();
        await Assert.That(regex.Options.HasFlag(RegexOptions.IgnorePatternWhitespace)).IsTrue();
    }

    [Test]
    public async Task ToRegex_WithEmptyOptions_ShouldNotSetAnyFlags()
    {
        var bson = new BsonRegularExpression("ab", string.Empty);
        var regex = bson.ToRegex();

        await Assert.That(regex.Options.HasFlag(RegexOptions.IgnoreCase)).IsFalse();
        await Assert.That(regex.Options.HasFlag(RegexOptions.Multiline)).IsFalse();
        await Assert.That(regex.Options.HasFlag(RegexOptions.Singleline)).IsFalse();
        await Assert.That(regex.Options.HasFlag(RegexOptions.IgnorePatternWhitespace)).IsFalse();
    }

    [Test]
    public async Task Conversions_ShouldThrowInvalidCast()
    {
        var bson = new BsonRegularExpression("ab");

        await Assert.That(bson.GetTypeCode()).IsEqualTo(TypeCode.Object);
        await Assert.That(bson.ToString(null)).IsEqualTo(bson.ToString());

        await Assert.That(() => bson.ToBoolean(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToByte(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToChar(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToDateTime(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToDecimal(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToDouble(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToInt16(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToInt32(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToInt64(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToSByte(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToSingle(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToUInt16(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToUInt32(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToUInt64(null)).Throws<InvalidCastException>();
        await Assert.That(() => bson.ToType(typeof(int), null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task Equals_NonRegex_ShouldReturnFalse()
    {
        var bson = new BsonRegularExpression("ab");
        await Assert.That(bson.Equals(new BsonInt32(1))).IsFalse();
    }
}
