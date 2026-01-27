using System;
using System.Globalization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

/// <summary>
/// Comprehensive tests for BsonJavaScriptWithScope to improve coverage
/// </summary>
public class BsonJavaScriptWithScopeTests
{
    #region Constructor Tests

    [Test]
    public async Task Constructor_ValidInput_ShouldCreateInstance()
    {
        var scope = new BsonDocument().Set("x", 1);
        var js = new BsonJavaScriptWithScope("return x + 1;", scope);

        await Assert.That(js.Code).IsEqualTo("return x + 1;");
        await Assert.That(js.Scope).IsEqualTo(scope);
        await Assert.That(js.BsonType).IsEqualTo(BsonType.JavaScriptWithScope);
        await Assert.That(js.RawValue).IsEqualTo("return x + 1;");
    }

    [Test]
    public async Task Constructor_NullCode_ShouldThrow()
    {
        var scope = new BsonDocument();
        await Assert.That(() => new BsonJavaScriptWithScope(null!, scope))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Constructor_NullScope_ShouldThrow()
    {
        await Assert.That(() => new BsonJavaScriptWithScope("code", null!))
            .Throws<ArgumentNullException>();
    }

    #endregion

    #region CompareTo Tests

    [Test]
    public async Task CompareTo_SameCode_ShouldCompareByScope()
    {
        var scope1 = new BsonDocument().Set("a", 1);
        var scope2 = new BsonDocument().Set("b", 2);
        var js1 = new BsonJavaScriptWithScope("code", scope1);
        var js2 = new BsonJavaScriptWithScope("code", scope2);

        var result = js1.CompareTo(js2);
        // Result depends on BsonDocument comparison
        await Assert.That(result).IsNotEqualTo(0);
    }

    [Test]
    public async Task CompareTo_DifferentCode_ShouldCompareByCode()
    {
        var scope = new BsonDocument();
        var js1 = new BsonJavaScriptWithScope("aaa", scope);
        var js2 = new BsonJavaScriptWithScope("bbb", scope);

        await Assert.That(js1.CompareTo(js2)).IsLessThan(0);
        await Assert.That(js2.CompareTo(js1)).IsGreaterThan(0);
    }

    [Test]
    public async Task CompareTo_Null_ShouldReturn1()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(js.CompareTo((BsonJavaScriptWithScope?)null)).IsEqualTo(1);
    }

    [Test]
    public async Task CompareTo_BsonValue_Null_ShouldReturn1()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(js.CompareTo((BsonValue?)null)).IsEqualTo(1);
    }

    [Test]
    public async Task CompareTo_DifferentBsonType_ShouldCompareByType()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        var str = new BsonString("test");

        var result = js.CompareTo(str);
        // Should compare by BsonType
        await Assert.That(result).IsNotEqualTo(0);
    }

    #endregion

    #region Equals Tests

    [Test]
    public async Task Equals_SameCodeAndScope_ShouldReturnTrue()
    {
        var scope1 = new BsonDocument().Set("x", 1);
        var scope2 = new BsonDocument().Set("x", 1);
        var js1 = new BsonJavaScriptWithScope("code", scope1);
        var js2 = new BsonJavaScriptWithScope("code", scope2);

        await Assert.That(js1.Equals(js2)).IsTrue();
    }

    [Test]
    public async Task Equals_DifferentCode_ShouldReturnFalse()
    {
        var scope = new BsonDocument().Set("x", 1);
        var js1 = new BsonJavaScriptWithScope("code1", scope);
        var js2 = new BsonJavaScriptWithScope("code2", scope);

        await Assert.That(js1.Equals(js2)).IsFalse();
    }

    [Test]
    public async Task Equals_DifferentScope_ShouldReturnFalse()
    {
        var scope1 = new BsonDocument().Set("x", 1);
        var scope2 = new BsonDocument().Set("x", 2);
        var js1 = new BsonJavaScriptWithScope("code", scope1);
        var js2 = new BsonJavaScriptWithScope("code", scope2);

        await Assert.That(js1.Equals(js2)).IsFalse();
    }

    [Test]
    public async Task Equals_Null_ShouldReturnFalse()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(js.Equals((BsonJavaScriptWithScope?)null)).IsFalse();
    }

    [Test]
    public async Task Equals_Object_ShouldWork()
    {
        var js1 = new BsonJavaScriptWithScope("code", new BsonDocument().Set("x", 1));
        var js2 = new BsonJavaScriptWithScope("code", new BsonDocument().Set("x", 1));
        object obj = js2;

        await Assert.That(js1.Equals(obj)).IsTrue();
    }

    [Test]
    public async Task Equals_BsonValue_ShouldWork()
    {
        var js1 = new BsonJavaScriptWithScope("code", new BsonDocument());
        BsonValue js2 = new BsonJavaScriptWithScope("code", new BsonDocument());

        await Assert.That(js1.Equals(js2)).IsTrue();
    }

    [Test]
    public async Task Equals_DifferentType_ShouldReturnFalse()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        var str = new BsonString("code");

        await Assert.That(js.Equals(str)).IsFalse();
    }

    #endregion

    #region GetHashCode Tests

    [Test]
    public async Task GetHashCode_SameInstance_ShouldBeSame()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument().Set("x", 1));
        await Assert.That(js.GetHashCode()).IsEqualTo(js.GetHashCode());
    }

    [Test]
    public async Task GetHashCode_EqualInstances_ShouldBeSame()
    {
        var js1 = new BsonJavaScriptWithScope("code", new BsonDocument().Set("x", 1));
        var js2 = new BsonJavaScriptWithScope("code", new BsonDocument().Set("x", 1));

        await Assert.That(js1.GetHashCode()).IsEqualTo(js2.GetHashCode());
    }

    #endregion

    #region IConvertible Tests

    [Test]
    public async Task ToBoolean_NonEmptyCode_ShouldReturnTrue()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(js.ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task ToBoolean_EmptyCode_ShouldReturnFalse()
    {
        var js = new BsonJavaScriptWithScope("", new BsonDocument());
        await Assert.That(js.ToBoolean(null)).IsFalse();
    }

    [Test]
    public async Task ToString_ShouldReturnCode()
    {
        var js = new BsonJavaScriptWithScope("return x;", new BsonDocument());
        await Assert.That(js.ToString(null)).IsEqualTo("return x;");
    }

    [Test]
    public async Task GetTypeCode_ShouldReturnObject()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(js.GetTypeCode()).IsEqualTo(TypeCode.Object);
    }

    [Test]
    public async Task ToType_String_ShouldReturnCode()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        var result = js.ToType(typeof(string), null);
        await Assert.That(result).IsEqualTo("code");
    }

    [Test]
    public async Task ToType_InvalidType_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToType(typeof(int), null))
            .Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToDouble_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToDouble(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToDecimal_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToDecimal(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToInt32_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToInt32(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToInt64_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToInt64(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToInt16_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToInt16(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToUInt16_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToUInt16(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToUInt32_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToUInt32(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToUInt64_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToUInt64(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToByte_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToByte(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToSByte_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToSByte(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToSingle_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToSingle(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToChar_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToChar(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToDateTime_ShouldThrow()
    {
        var js = new BsonJavaScriptWithScope("code", new BsonDocument());
        await Assert.That(() => js.ToDateTime(null)).Throws<InvalidCastException>();
    }

    #endregion
}
