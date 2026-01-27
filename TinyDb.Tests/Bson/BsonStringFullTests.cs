using System;
using System.Globalization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

/// <summary>
/// Comprehensive tests for BsonString to improve coverage
/// </summary>
public class BsonStringFullTests
{
    #region Constructor and Basic Properties

    [Test]
    public async Task Constructor_ValidString_ShouldSetValue()
    {
        var bsonStr = new BsonString("test");

        await Assert.That(bsonStr.Value).IsEqualTo("test");
        await Assert.That(bsonStr.BsonType).IsEqualTo(BsonType.String);
        await Assert.That(bsonStr.RawValue).IsEqualTo("test");
    }

    [Test]
    public async Task Constructor_NullString_ShouldSetEmpty()
    {
        var bsonStr = new BsonString(null!);
        await Assert.That(bsonStr.Value).IsEqualTo(string.Empty);
    }

    [Test]
    public async Task Constructor_EmptyString_ShouldSetEmpty()
    {
        var bsonStr = new BsonString("");
        await Assert.That(bsonStr.Value).IsEqualTo(string.Empty);
    }

    [Test]
    public async Task ImplicitConversion_FromString_ShouldWork()
    {
        BsonString bsonStr = "hello";
        await Assert.That(bsonStr.Value).IsEqualTo("hello");
    }

    [Test]
    public async Task ImplicitConversion_ToString_ShouldWork()
    {
        var bsonStr = new BsonString("hello");
        string str = bsonStr;
        await Assert.That(str).IsEqualTo("hello");
    }

    #endregion

    #region CompareTo Tests

    [Test]
    public async Task CompareTo_SameValue_ShouldReturnZero()
    {
        var bsonStr1 = new BsonString("test");
        var bsonStr2 = new BsonString("test");

        await Assert.That(bsonStr1.CompareTo(bsonStr2)).IsEqualTo(0);
    }

    [Test]
    public async Task CompareTo_Lesser_ShouldReturnNegative()
    {
        var bsonStr1 = new BsonString("aaa");
        var bsonStr2 = new BsonString("bbb");

        await Assert.That(bsonStr1.CompareTo(bsonStr2)).IsLessThan(0);
    }

    [Test]
    public async Task CompareTo_Greater_ShouldReturnPositive()
    {
        var bsonStr1 = new BsonString("bbb");
        var bsonStr2 = new BsonString("aaa");

        await Assert.That(bsonStr1.CompareTo(bsonStr2)).IsGreaterThan(0);
    }

    [Test]
    public async Task CompareTo_Null_ShouldReturn1()
    {
        var bsonStr = new BsonString("test");
        await Assert.That(bsonStr.CompareTo(null)).IsEqualTo(1);
    }

    [Test]
    public async Task CompareTo_DifferentBsonType_ShouldCompareByType()
    {
        var bsonStr = new BsonString("test");
        var bsonInt = new BsonInt32(123);

        var result = bsonStr.CompareTo(bsonInt);
        await Assert.That(result).IsNotEqualTo(0);
    }

    [Test]
    public async Task CompareTo_CaseSensitive_ShouldDistinguishCase()
    {
        var bsonStr1 = new BsonString("ABC");
        var bsonStr2 = new BsonString("abc");

        // Ordinal comparison - uppercase comes before lowercase
        await Assert.That(bsonStr1.CompareTo(bsonStr2)).IsNotEqualTo(0);
    }

    #endregion

    #region Equals Tests

    [Test]
    public async Task Equals_SameValue_ShouldReturnTrue()
    {
        var bsonStr1 = new BsonString("test");
        var bsonStr2 = new BsonString("test");

        await Assert.That(bsonStr1.Equals(bsonStr2)).IsTrue();
    }

    [Test]
    public async Task Equals_DifferentValue_ShouldReturnFalse()
    {
        var bsonStr1 = new BsonString("test1");
        var bsonStr2 = new BsonString("test2");

        await Assert.That(bsonStr1.Equals(bsonStr2)).IsFalse();
    }

    [Test]
    public async Task Equals_DifferentType_ShouldReturnFalse()
    {
        var bsonStr = new BsonString("123");
        var bsonInt = new BsonInt32(123);

        await Assert.That(bsonStr.Equals(bsonInt)).IsFalse();
    }

    [Test]
    public async Task Equals_CaseSensitive_ShouldReturnFalse()
    {
        var bsonStr1 = new BsonString("Test");
        var bsonStr2 = new BsonString("test");

        await Assert.That(bsonStr1.Equals(bsonStr2)).IsFalse();
    }

    #endregion

    #region GetHashCode Tests

    [Test]
    public async Task GetHashCode_SameValue_ShouldBeSame()
    {
        var bsonStr1 = new BsonString("test");
        var bsonStr2 = new BsonString("test");

        await Assert.That(bsonStr1.GetHashCode()).IsEqualTo(bsonStr2.GetHashCode());
    }

    #endregion

    #region ToString Tests

    [Test]
    public async Task ToString_ShouldReturnValue()
    {
        var bsonStr = new BsonString("hello world");
        await Assert.That(bsonStr.ToString()).IsEqualTo("hello world");
    }

    #endregion

    #region IConvertible Tests

    [Test]
    public async Task GetTypeCode_ShouldReturnString()
    {
        var bsonStr = new BsonString("test");
        await Assert.That(bsonStr.GetTypeCode()).IsEqualTo(TypeCode.String);
    }

    [Test]
    public async Task ToBoolean_True_ShouldReturnTrue()
    {
        var bsonStr = new BsonString("true");
        await Assert.That(bsonStr.ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task ToBoolean_False_ShouldReturnFalse()
    {
        var bsonStr = new BsonString("false");
        await Assert.That(bsonStr.ToBoolean(null)).IsFalse();
    }

    [Test]
    public async Task ToBoolean_InvalidString_ShouldThrow()
    {
        var bsonStr = new BsonString("invalid");
        await Assert.That(() => bsonStr.ToBoolean(null)).Throws<FormatException>();
    }

    [Test]
    public async Task ToByte_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("255");
        await Assert.That(bsonStr.ToByte(null)).IsEqualTo((byte)255);
    }

    [Test]
    public async Task ToChar_SingleChar_ShouldConvert()
    {
        var bsonStr = new BsonString("A");
        await Assert.That(bsonStr.ToChar(null)).IsEqualTo('A');
    }

    [Test]
    public async Task ToDateTime_ValidDate_ShouldConvert()
    {
        var bsonStr = new BsonString("2024-01-15");
        var result = bsonStr.ToDateTime(CultureInfo.InvariantCulture);
        await Assert.That(result.Year).IsEqualTo(2024);
        await Assert.That(result.Month).IsEqualTo(1);
        await Assert.That(result.Day).IsEqualTo(15);
    }

    [Test]
    public async Task ToDecimal_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("123.456");
        await Assert.That(bsonStr.ToDecimal(CultureInfo.InvariantCulture)).IsEqualTo(123.456m);
    }

    [Test]
    public async Task ToDouble_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("123.456");
        await Assert.That(bsonStr.ToDouble(CultureInfo.InvariantCulture)).IsEqualTo(123.456);
    }

    [Test]
    public async Task ToInt16_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("12345");
        await Assert.That(bsonStr.ToInt16(null)).IsEqualTo((short)12345);
    }

    [Test]
    public async Task ToInt32_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("123456");
        await Assert.That(bsonStr.ToInt32(null)).IsEqualTo(123456);
    }

    [Test]
    public async Task ToInt64_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("123456789012");
        await Assert.That(bsonStr.ToInt64(null)).IsEqualTo(123456789012L);
    }

    [Test]
    public async Task ToSByte_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("127");
        await Assert.That(bsonStr.ToSByte(null)).IsEqualTo((sbyte)127);
    }

    [Test]
    public async Task ToSingle_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("123.5");
        await Assert.That(bsonStr.ToSingle(CultureInfo.InvariantCulture)).IsEqualTo(123.5f);
    }

    [Test]
    public async Task ToStringWithProvider_ShouldReturnValue()
    {
        var bsonStr = new BsonString("test");
        await Assert.That(bsonStr.ToString(CultureInfo.InvariantCulture)).IsEqualTo("test");
    }

    [Test]
    public async Task ToUInt16_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("65535");
        await Assert.That(bsonStr.ToUInt16(null)).IsEqualTo((ushort)65535);
    }

    [Test]
    public async Task ToUInt32_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("4294967295");
        await Assert.That(bsonStr.ToUInt32(null)).IsEqualTo(4294967295u);
    }

    [Test]
    public async Task ToUInt64_ValidNumber_ShouldConvert()
    {
        var bsonStr = new BsonString("18446744073709551615");
        await Assert.That(bsonStr.ToUInt64(null)).IsEqualTo(18446744073709551615ul);
    }

    [Test]
    public async Task ToType_String_ShouldReturnValue()
    {
        var bsonStr = new BsonString("test");
        var result = bsonStr.ToType(typeof(string), null);
        await Assert.That(result).IsEqualTo("test");
    }

    [Test]
    public async Task ToType_BsonString_ShouldReturnSelf()
    {
        var bsonStr = new BsonString("test");
        var result = bsonStr.ToType(typeof(BsonString), null);
        // ToType returns 'this' for BsonString type
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task ToType_Object_ShouldReturnSelf()
    {
        var bsonStr = new BsonString("test");
        var result = bsonStr.ToType(typeof(object), null);
        // ToType returns 'this' for object type
        await Assert.That(result).IsNotNull();
    }

    [Test]
    public async Task ToType_Int_ShouldConvert()
    {
        var bsonStr = new BsonString("123");
        var result = bsonStr.ToType(typeof(int), null);
        await Assert.That(result).IsEqualTo(123);
    }

    [Test]
    public async Task ToType_Double_ShouldConvert()
    {
        var bsonStr = new BsonString("123.456");
        var result = bsonStr.ToType(typeof(double), CultureInfo.InvariantCulture);
        await Assert.That(result).IsEqualTo(123.456);
    }

    #endregion

    #region Edge Cases

    [Test]
    public async Task ToString_WithUnicode_ShouldPreserve()
    {
        var bsonStr = new BsonString("Hello 世界 🌍");
        await Assert.That(bsonStr.ToString()).IsEqualTo("Hello 世界 🌍");
    }

    [Test]
    public async Task ToString_WithNewlines_ShouldPreserve()
    {
        var bsonStr = new BsonString("line1\nline2\r\nline3");
        await Assert.That(bsonStr.Value).Contains("\n");
    }

    [Test]
    public async Task Compare_EmptyStrings_ShouldBeEqual()
    {
        var bsonStr1 = new BsonString("");
        var bsonStr2 = new BsonString("");

        await Assert.That(bsonStr1.CompareTo(bsonStr2)).IsEqualTo(0);
        await Assert.That(bsonStr1.Equals(bsonStr2)).IsTrue();
    }

    #endregion
}
