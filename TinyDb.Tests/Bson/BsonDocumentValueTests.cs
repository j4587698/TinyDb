using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonDocumentValueTests
{
    [Test]
    public async Task BsonDocument_As_BsonValue_Delegates_Correctly()
    {
        // BsonDocument directly inherits BsonValue - no need for reflection
        var doc = new BsonDocument().Set("a", 1);
        
        // Use BsonDocument directly as BsonValue
        BsonValue instance = doc;
        await Assert.That(instance).IsNotNull();
        
        // Test Properties
        await Assert.That(instance.BsonType).IsEqualTo(BsonType.Document);
        await Assert.That(instance.IsNull).IsFalse();
        
        // Test ToString
        await Assert.That(instance.ToString()).IsEqualTo(doc.ToString());
        
        // Test Equals
        BsonValue other = new BsonDocument().Set("a", 1);
        await Assert.That(instance.Equals(other)).IsTrue();
        await Assert.That(instance.Equals(doc)).IsTrue(); 
        
        // Test CompareTo
        await Assert.That(instance.CompareTo(other)).IsEqualTo(0);
        
        BsonValue smaller = new BsonDocument();
        await Assert.That(instance.CompareTo(smaller)).IsGreaterThan(0);
        
        // Test GetHashCode
        await Assert.That(instance.GetHashCode()).IsEqualTo(doc.GetHashCode());
    }
}

/// <summary>
/// Extended tests for BsonDocumentValue (internal wrapper class) to improve coverage
/// </summary>
public class BsonDocumentValueExtendedTests
{
    #region Constructor Tests

    [Test]
    public async Task Constructor_WithDocument_ShouldSetValue()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue>
        {
            ["name"] = "test",
            ["age"] = 25
        });
        var docValue = new BsonDocumentValue(doc);

        await Assert.That(docValue.Value).IsEqualTo(doc);
    }

    [Test]
    public async Task Constructor_Default_ShouldCreateEmptyDocument()
    {
        var docValue = new BsonDocumentValue();

        await Assert.That(docValue.Value).IsNotNull();
        await Assert.That(docValue.Value.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Constructor_WithNull_ShouldThrow()
    {
        await Assert.That(() => new BsonDocumentValue(null!)).Throws<ArgumentNullException>();
    }

    #endregion

    #region BsonType and Properties Tests

    [Test]
    public async Task BsonType_ShouldBeDocument()
    {
        var docValue = new BsonDocumentValue();

        await Assert.That(docValue.BsonType).IsEqualTo(BsonType.Document);
    }

    [Test]
    public async Task IsDocument_ShouldBeTrue()
    {
        var docValue = new BsonDocumentValue();

        await Assert.That(docValue.IsDocument).IsTrue();
    }

    [Test]
    public async Task RawValue_ShouldReturnDocument()
    {
        var doc = new BsonDocument();
        var docValue = new BsonDocumentValue(doc);

        await Assert.That(docValue.RawValue).IsEqualTo(doc);
    }

    #endregion

    #region CompareTo Tests

    [Test]
    public async Task CompareTo_Null_ShouldReturn1()
    {
        var docValue = new BsonDocumentValue();

        await Assert.That(docValue.CompareTo(null)).IsEqualTo(1);
    }

    [Test]
    public async Task CompareTo_SameDocumentValue_ShouldReturn0()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue1 = new BsonDocumentValue(doc);
        var docValue2 = new BsonDocumentValue(doc);

        await Assert.That(docValue1.CompareTo(docValue2)).IsEqualTo(0);
    }

    [Test]
    public async Task CompareTo_BsonDocument_ShouldCompareCorrectly()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue = new BsonDocumentValue(doc);

        await Assert.That(docValue.CompareTo(doc)).IsEqualTo(0);
    }

    [Test]
    public async Task CompareTo_DifferentBsonType_ShouldCompareByType()
    {
        var docValue = new BsonDocumentValue();
        var stringValue = new BsonString("test");

        var result = docValue.CompareTo(stringValue);
        await Assert.That(result).IsNotEqualTo(0);
    }

    [Test]
    public async Task CompareTo_DifferentDocumentValues_ShouldCompareByContent()
    {
        var doc1 = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var doc2 = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1, ["y"] = 2 });
        var docValue1 = new BsonDocumentValue(doc1);
        var docValue2 = new BsonDocumentValue(doc2);

        await Assert.That(docValue1.CompareTo(docValue2)).IsLessThan(0);
    }

    #endregion

    #region Equals Tests

    [Test]
    public async Task Equals_Null_ShouldReturnFalse()
    {
        var docValue = new BsonDocumentValue();

        await Assert.That(docValue.Equals(null)).IsFalse();
    }

    [Test]
    public async Task Equals_SameBsonDocumentValue_ShouldReturnTrue()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["key"] = "value" });
        var docValue1 = new BsonDocumentValue(doc);
        var docValue2 = new BsonDocumentValue(doc);

        await Assert.That(docValue1.Equals(docValue2)).IsTrue();
    }

    [Test]
    public async Task Equals_BsonDocument_ShouldReturnTrue()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["key"] = "value" });
        var docValue = new BsonDocumentValue(doc);

        await Assert.That(docValue.Equals(doc)).IsTrue();
    }

    [Test]
    public async Task Equals_DifferentBsonType_ShouldReturnFalse()
    {
        var docValue = new BsonDocumentValue();
        var intValue = new BsonInt32(42);

        await Assert.That(docValue.Equals(intValue)).IsFalse();
    }

    [Test]
    public async Task Equals_DifferentContent_ShouldReturnFalse()
    {
        var doc1 = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var doc2 = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 2 });
        var docValue1 = new BsonDocumentValue(doc1);
        var docValue2 = new BsonDocumentValue(doc2);

        await Assert.That(docValue1.Equals(docValue2)).IsFalse();
    }

    #endregion

    #region GetHashCode and ToString Tests

    [Test]
    public async Task GetHashCode_SameContent_ShouldBeSame()
    {
        var doc1 = new BsonDocument(new Dictionary<string, BsonValue> { ["a"] = 1 });
        var doc2 = new BsonDocument(new Dictionary<string, BsonValue> { ["a"] = 1 });
        var docValue1 = new BsonDocumentValue(doc1);
        var docValue2 = new BsonDocumentValue(doc2);

        await Assert.That(docValue1.GetHashCode()).IsEqualTo(docValue2.GetHashCode());
    }

    [Test]
    public async Task ToString_ShouldReturnJsonFormat()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["name"] = "test" });
        var docValue = new BsonDocumentValue(doc);

        var str = docValue.ToString();
        await Assert.That(str).Contains("name");
        await Assert.That(str).Contains("test");
    }

    #endregion

    #region IConvertible Tests

    [Test]
    public async Task GetTypeCode_ShouldReturnObject()
    {
        var docValue = new BsonDocumentValue();
        await Assert.That(docValue.GetTypeCode()).IsEqualTo(TypeCode.Object);
    }

    [Test]
    public async Task ToBoolean_EmptyDoc_ShouldReturnFalse()
    {
        var docValue = new BsonDocumentValue();
        await Assert.That(docValue.ToBoolean(null)).IsFalse();
    }

    [Test]
    public async Task ToBoolean_NonEmptyDoc_ShouldReturnTrue()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task ToInt32_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["a"] = 1, ["b"] = 2, ["c"] = 3 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToInt32(null)).IsEqualTo(3);
    }

    [Test]
    public async Task ToInt64_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1, ["y"] = 2 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToInt64(null)).IsEqualTo(2L);
    }

    [Test]
    public async Task ToByte_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToByte(null)).IsEqualTo((byte)1);
    }

    [Test]
    public async Task ToChar_ShouldConvertCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That((int)docValue.ToChar(null)).IsEqualTo(1);
    }

    [Test]
    public async Task ToDateTime_ShouldThrow()
    {
        var docValue = new BsonDocumentValue();
        await Assert.That(() => docValue.ToDateTime(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToDecimal_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["a"] = 1, ["b"] = 2 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToDecimal(null)).IsEqualTo(2m);
    }

    [Test]
    public async Task ToDouble_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToDouble(null)).IsEqualTo(1.0);
    }

    [Test]
    public async Task ToInt16_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1, ["y"] = 2 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToInt16(null)).IsEqualTo((short)2);
    }

    [Test]
    public async Task ToSByte_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToSByte(null)).IsEqualTo((sbyte)1);
    }

    [Test]
    public async Task ToSingle_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToSingle(null)).IsEqualTo(1.0f);
    }

    [Test]
    public async Task ToStringWithProvider_ShouldReturnJsonFormat()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["key"] = "val" });
        var docValue = new BsonDocumentValue(doc);
        var str = docValue.ToString(null);
        await Assert.That(str).Contains("key");
    }

    [Test]
    public async Task ToUInt16_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToUInt16(null)).IsEqualTo((ushort)1);
    }

    [Test]
    public async Task ToUInt32_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1, ["y"] = 2 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToUInt32(null)).IsEqualTo(2u);
    }

    [Test]
    public async Task ToUInt64_ShouldReturnCount()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue = new BsonDocumentValue(doc);
        await Assert.That(docValue.ToUInt64(null)).IsEqualTo(1ul);
    }

    [Test]
    public async Task ToType_BsonDocument_ShouldReturnValue()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = 1 });
        var docValue = new BsonDocumentValue(doc);
        var result = docValue.ToType(typeof(BsonDocument), null);
        await Assert.That(result).IsEqualTo(doc);
    }

    [Test]
    public async Task ToType_Dictionary_ShouldConvert()
    {
        var doc = new BsonDocument(new Dictionary<string, BsonValue> { ["x"] = (BsonValue)1 });
        var docValue = new BsonDocumentValue(doc);
        var result = docValue.ToType(typeof(Dictionary<string, object?>), null) as Dictionary<string, object?>;
        await Assert.That(result).IsNotNull();
        await Assert.That(result!.ContainsKey("x")).IsTrue();
    }

    #endregion
}

/// <summary>
/// Tests for BsonArrayValue (internal wrapper class) to improve coverage
/// </summary>
public class BsonArrayValueExtendedTests
{
    #region Constructor Tests

    [Test]
    public async Task Constructor_WithArray_ShouldSetValue()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });
        var arrayValue = new BsonArrayValue(array);

        await Assert.That(arrayValue.Value).IsEqualTo(array);
    }

    [Test]
    public async Task Constructor_Default_ShouldCreateEmptyArray()
    {
        var arrayValue = new BsonArrayValue();

        await Assert.That(arrayValue.Value).IsNotNull();
        await Assert.That(arrayValue.Value.Count).IsEqualTo(0);
    }

    [Test]
    public async Task Constructor_WithNull_ShouldThrow()
    {
        await Assert.That(() => new BsonArrayValue(null!)).Throws<ArgumentNullException>();
    }

    #endregion

    #region BsonType and Properties Tests

    [Test]
    public async Task BsonType_ShouldBeArray()
    {
        var arrayValue = new BsonArrayValue();
        await Assert.That(arrayValue.BsonType).IsEqualTo(BsonType.Array);
    }

    [Test]
    public async Task IsArray_ShouldBeTrue()
    {
        var arrayValue = new BsonArrayValue();
        await Assert.That(arrayValue.IsArray).IsTrue();
    }

    [Test]
    public async Task RawValue_ShouldReturnArray()
    {
        var array = new BsonArray();
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.RawValue).IsEqualTo(array);
    }

    #endregion

    #region CompareTo Tests

    [Test]
    public async Task CompareTo_Null_ShouldReturn1()
    {
        var arrayValue = new BsonArrayValue();
        await Assert.That(arrayValue.CompareTo(null)).IsEqualTo(1);
    }

    [Test]
    public async Task CompareTo_SameArrayValue_ShouldReturn0()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2 });
        var arrayValue1 = new BsonArrayValue(array);
        var arrayValue2 = new BsonArrayValue(array);

        await Assert.That(arrayValue1.CompareTo(arrayValue2)).IsEqualTo(0);
    }

    [Test]
    public async Task CompareTo_BsonArray_ShouldCompareCorrectly()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2 });
        var arrayValue = new BsonArrayValue(array);

        await Assert.That(arrayValue.CompareTo(array)).IsEqualTo(0);
    }

    [Test]
    public async Task CompareTo_DifferentBsonType_ShouldCompareByType()
    {
        var arrayValue = new BsonArrayValue();
        var intValue = new BsonInt32(42);

        var result = arrayValue.CompareTo(intValue);
        await Assert.That(result).IsNotEqualTo(0);
    }

    [Test]
    public async Task CompareTo_DifferentArrayValues_ShouldCompareByContent()
    {
        var array1 = new BsonArray(new BsonValue[] { 1 });
        var array2 = new BsonArray(new BsonValue[] { 1, 2 });
        var arrayValue1 = new BsonArrayValue(array1);
        var arrayValue2 = new BsonArrayValue(array2);

        await Assert.That(arrayValue1.CompareTo(arrayValue2)).IsLessThan(0);
    }

    #endregion

    #region Equals Tests

    [Test]
    public async Task Equals_SameBsonArrayValue_ShouldReturnTrue()
    {
        var array = new BsonArray(new BsonValue[] { "a", "b" });
        var arrayValue1 = new BsonArrayValue(array);
        var arrayValue2 = new BsonArrayValue(array);

        await Assert.That(arrayValue1.Equals(arrayValue2)).IsTrue();
    }

    [Test]
    public async Task Equals_DifferentContent_ShouldReturnFalse()
    {
        var array1 = new BsonArray(new BsonValue[] { 1 });
        var array2 = new BsonArray(new BsonValue[] { 2 });
        var arrayValue1 = new BsonArrayValue(array1);
        var arrayValue2 = new BsonArrayValue(array2);

        await Assert.That(arrayValue1.Equals(arrayValue2)).IsFalse();
    }

    [Test]
    public async Task Equals_Null_ShouldReturnFalse()
    {
        var arrayValue = new BsonArrayValue();
        await Assert.That(arrayValue.Equals(null)).IsFalse();
    }

    [Test]
    public async Task Equals_DifferentType_ShouldReturnFalse()
    {
        var arrayValue = new BsonArrayValue();
        var docValue = new BsonDocumentValue();

        await Assert.That(arrayValue.Equals(docValue)).IsFalse();
    }

    #endregion

    #region GetHashCode and ToString Tests

    [Test]
    public async Task GetHashCode_SameContent_ShouldBeSame()
    {
        var array1 = new BsonArray(new BsonValue[] { 1, 2 });
        var array2 = new BsonArray(new BsonValue[] { 1, 2 });
        var arrayValue1 = new BsonArrayValue(array1);
        var arrayValue2 = new BsonArrayValue(array2);

        await Assert.That(arrayValue1.GetHashCode()).IsEqualTo(arrayValue2.GetHashCode());
    }

    [Test]
    public async Task ToString_ShouldReturnArrayFormat()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });
        var arrayValue = new BsonArrayValue(array);

        var str = arrayValue.ToString();
        await Assert.That(str).Contains("[");
        await Assert.That(str).Contains("]");
    }

    #endregion

    #region IConvertible Tests

    [Test]
    public async Task GetTypeCode_ShouldReturnObject()
    {
        var arrayValue = new BsonArrayValue();
        await Assert.That(arrayValue.GetTypeCode()).IsEqualTo(TypeCode.Object);
    }

    [Test]
    public async Task ToBoolean_EmptyArray_ShouldReturnFalse()
    {
        var arrayValue = new BsonArrayValue();
        await Assert.That(arrayValue.ToBoolean(null)).IsFalse();
    }

    [Test]
    public async Task ToBoolean_NonEmptyArray_ShouldReturnTrue()
    {
        var array = new BsonArray(new BsonValue[] { 1 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task ToInt32_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToInt32(null)).IsEqualTo(3);
    }

    [Test]
    public async Task ToInt64_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToInt64(null)).IsEqualTo(2L);
    }

    [Test]
    public async Task ToByte_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToByte(null)).IsEqualTo((byte)1);
    }

    [Test]
    public async Task ToChar_ShouldConvertCount()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That((int)arrayValue.ToChar(null)).IsEqualTo(2);
    }

    [Test]
    public async Task ToDateTime_ShouldThrow()
    {
        var arrayValue = new BsonArrayValue();
        await Assert.That(() => arrayValue.ToDateTime(null)).Throws<InvalidCastException>();
    }

    [Test]
    public async Task ToDecimal_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2, 3 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToDecimal(null)).IsEqualTo(3m);
    }

    [Test]
    public async Task ToDouble_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToDouble(null)).IsEqualTo(1.0);
    }

    [Test]
    public async Task ToInt16_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToInt16(null)).IsEqualTo((short)2);
    }

    [Test]
    public async Task ToSByte_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToSByte(null)).IsEqualTo((sbyte)1);
    }

    [Test]
    public async Task ToSingle_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToSingle(null)).IsEqualTo(1.0f);
    }

    [Test]
    public async Task ToStringWithProvider_ShouldReturnArrayFormat()
    {
        var array = new BsonArray(new BsonValue[] { "a" });
        var arrayValue = new BsonArrayValue(array);
        var str = arrayValue.ToString(null);
        await Assert.That(str).Contains("[");
    }

    [Test]
    public async Task ToUInt16_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToUInt16(null)).IsEqualTo((ushort)1);
    }

    [Test]
    public async Task ToUInt32_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToUInt32(null)).IsEqualTo(2u);
    }

    [Test]
    public async Task ToUInt64_ShouldReturnCount()
    {
        var array = new BsonArray(new BsonValue[] { 1 });
        var arrayValue = new BsonArrayValue(array);
        await Assert.That(arrayValue.ToUInt64(null)).IsEqualTo(1ul);
    }

    [Test]
    public async Task ToType_BsonArray_ShouldReturnValue()
    {
        var array = new BsonArray(new BsonValue[] { 1, 2 });
        var arrayValue = new BsonArrayValue(array);
        var result = arrayValue.ToType(typeof(BsonArray), null);
        await Assert.That(result).IsEqualTo(array);
    }

    [Test]
    public async Task ToType_List_ShouldConvert()
    {
        var array = new BsonArray(new BsonValue[] { (BsonValue)1, (BsonValue)2 });
        var arrayValue = new BsonArrayValue(array);
        var result = arrayValue.ToType(typeof(List<object?>), null) as List<object?>;
        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Count).IsEqualTo(2);
    }

    #endregion
}
