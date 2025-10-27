using System.Diagnostics.CodeAnalysis;
using SimpleDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Bson;

public class BsonValueTests
{
    [Test]
    public async Task BsonValue_Should_Create_Null_Value()
    {
        // Arrange & Act
        var value = BsonNull.Value;

        // Assert
        await Assert.That(value.BsonType).IsEqualTo(BsonType.Null);
        await Assert.That(value.IsNull).IsTrue();
    }

    [Test]
    public async Task BsonValue_Should_Create_Int32_Value()
    {
        // Arrange & Act
        var value = new BsonInt32(42);

        // Assert
        await Assert.That(value.BsonType).IsEqualTo(BsonType.Int32);
        await Assert.That(value.IsNull).IsFalse();
        await Assert.That(value.ToInt32(null)).IsEqualTo(42);
    }

    [Test]
    public async Task BsonValue_Should_Create_Int64_Value()
    {
        // Arrange & Act
        var value = new BsonInt64(9876543210L);

        // Assert
        await Assert.That(value.BsonType).IsEqualTo(BsonType.Int64);
        await Assert.That(value.ToInt64(null)).IsEqualTo(9876543210L);
    }

    [Test]
    public async Task BsonValue_Should_Create_Double_Value()
    {
        // Arrange & Act
        var value = new BsonDouble(3.14159);

        // Assert
        await Assert.That(value.BsonType).IsEqualTo(BsonType.Double);
        await Assert.That(value.ToDouble(null)).IsEqualTo(3.14159);
    }

    [Test]
    public async Task BsonValue_Should_Create_String_Value()
    {
        // Arrange & Act
        var value = new BsonString("Hello World");

        // Assert
        await Assert.That(value.BsonType).IsEqualTo(BsonType.String);
        await Assert.That(value.ToString(null)).IsEqualTo("Hello World");
        await Assert.That(value.IsString).IsTrue();
    }

    [Test]
    public async Task BsonValue_Should_Create_Bool_Value()
    {
        // Arrange & Act
        var trueValue = new BsonBoolean(true);
        var falseValue = new BsonBoolean(false);

        // Assert
        await Assert.That(trueValue.BsonType).IsEqualTo(BsonType.Boolean);
        await Assert.That(trueValue.ToBoolean(null)).IsTrue();
        await Assert.That(trueValue.IsBoolean).IsTrue();
        await Assert.That(falseValue.ToBoolean(null)).IsFalse();
    }

    [Test]
    public async Task BsonValue_Should_Create_ObjectId_Value()
    {
        // Arrange & Act
        var objectId = ObjectId.NewObjectId();
        var value = new BsonObjectId(objectId);

        // Assert
        await Assert.That(value.BsonType).IsEqualTo(BsonType.ObjectId);
        await Assert.That(value.ToString()).IsEqualTo(objectId.ToString());
    }

    [Test]
    public async Task BsonValue_Equality_Should_Work_Correctly()
    {
        // Arrange
        var value1 = new BsonInt32(42);
        var value2 = new BsonInt32(42);
        var value3 = new BsonInt32(43);
        var value4 = new BsonString("42");

        // Act & Assert
        await Assert.That(value1.Equals(value2)).IsTrue();
        await Assert.That(value1 == value2).IsTrue();
        await Assert.That(value1.Equals(value3)).IsFalse();
        await Assert.That(value1 == value3).IsFalse();
        await Assert.That(value1.Equals(value4)).IsFalse();
    }

    [Test]
    public async Task BsonValue_GetHashCode_Should_Be_Consistent()
    {
        // Arrange
        var value1 = new BsonInt32(42);
        var value2 = new BsonInt32(42);
        var value3 = new BsonInt32(43);

        // Act & Assert
        await Assert.That(value1.GetHashCode()).IsEqualTo(value2.GetHashCode());
        await Assert.That(value1.GetHashCode()).IsNotEqualTo(value3.GetHashCode());
    }

    [Test]
    public async Task BsonValue_Implicit_Conversion_Should_Work()
    {
        // Arrange & Act
        BsonValue intValue = 42;
        BsonValue longValue = 123L;
        BsonValue doubleValue = 3.14;
        BsonValue stringValue = "test";
        BsonValue boolValue = true;

        // Assert
        await Assert.That(intValue.ToInt32(null)).IsEqualTo(42);
        await Assert.That(longValue.ToInt64(null)).IsEqualTo(123L);
        await Assert.That(doubleValue.ToDouble(null)).IsEqualTo(3.14);
        await Assert.That(stringValue.ToString(null)).IsEqualTo("test");
        await Assert.That(boolValue.ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task BsonValue_ToString_Should_Return_Representation()
    {
        // Arrange & Act
        var intValue = new BsonInt32(42);
        var stringValue = new BsonString("test");
        var nullValue = BsonNull.Value;

        // Assert
        await Assert.That(intValue.ToString()).IsEqualTo("42");
        await Assert.That(stringValue.ToString()).IsEqualTo("test");
        await Assert.That(nullValue.ToString()).IsEqualTo("null");
    }

    [Test]
    public async Task BsonValue_Type_Checks_Should_Work()
    {
        // Arrange & Act
        var intValue = new BsonInt32(42);
        var stringValue = new BsonString("test");
        var arrayValue = new BsonArray();
        var docValue = new BsonDocument();

        // Assert
        await Assert.That(intValue.IsNumeric).IsTrue();
        await Assert.That(intValue.IsString).IsFalse();

        await Assert.That(stringValue.IsString).IsTrue();
        await Assert.That(stringValue.IsNumeric).IsFalse();

        await Assert.That(arrayValue.IsArray).IsTrue();
        await Assert.That(arrayValue.IsDocument).IsFalse();

        await Assert.That(docValue.IsDocument).IsTrue();
        await Assert.That(docValue.IsArray).IsFalse();
    }

    [Test]
    public async Task BsonValue_Comparison_Should_Work()
    {
        // Arrange
        var value1 = new BsonInt32(10);
        var value2 = new BsonInt32(20);
        var value3 = new BsonString("test");

        // Act & Assert
        await Assert.That(value1.CompareTo(value2)).IsNegative();
        await Assert.That(value2.CompareTo(value1)).IsPositive();
        await Assert.That(value1.CompareTo(value1)).IsEqualTo(0);

        // Different types should compare by BsonType
        await Assert.That(value1.CompareTo(value3)).IsNotEqualTo(0);
    }
}