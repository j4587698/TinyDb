using System.Diagnostics.CodeAnalysis;
using SimpleDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Bson;

public class ObjectIdTests
{
    [Test]
    public async Task ObjectId_NewObjectId_Should_Generate_Unique_Ids()
    {
        // Arrange & Act
        var id1 = ObjectId.NewObjectId();
        var id2 = ObjectId.NewObjectId();

        // Assert
        await Assert.That(id1).IsNotEqualTo(id2);
        await Assert.That(id1.ToString()).IsNotEqualTo("000000000000000000000000");
        await Assert.That(id2.ToString()).IsNotEqualTo("000000000000000000000000");
    }

    [Test]
    public async Task ObjectId_Empty_Should_Be_Valid()
    {
        // Arrange & Act
        var emptyId = ObjectId.Empty;

        // Assert
        await Assert.That(emptyId.ToString()).IsEqualTo("000000000000000000000000");
    }

    [Test]
    public async Task ObjectId_Parse_Should_Parse_Valid_String()
    {
        // Arrange
        var idString = "507f1f77bcf86cd799439011";

        // Act
        var objectId = ObjectId.Parse(idString);

        // Assert
        await Assert.That(objectId.ToString()).IsEqualTo(idString);
    }

    [Test]
    public async Task ObjectId_TryParse_Should_Handle_Valid_And_Invalid_Strings()
    {
        // Arrange
        var validString = "507f1f77bcf86cd799439011";
        var invalidString = "invalid";

        // Act & Assert
        var validResult = ObjectId.TryParse(validString, out var validObjectId);
        await Assert.That(validResult).IsTrue();
        await Assert.That(validObjectId.ToString()).IsEqualTo(validString);

        var invalidResult = ObjectId.TryParse(invalidString, out var invalidObjectId);
        await Assert.That(invalidResult).IsFalse();
    }

    [Test]
    public async Task ObjectId_Parse_Should_Throw_On_Invalid_String()
    {
        // Arrange
        var invalidString = "invalid";

        // Act & Assert
        await Assert.That(() => ObjectId.Parse(invalidString)).Throws<ArgumentException>();
    }

    [Test]
    public async Task ObjectId_FromBytes_Should_Create_Valid_ObjectId()
    {
        // Arrange
        var bytes = new byte[] { 0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11 };

        // Act
        var objectId = new ObjectId(bytes);

        // Assert
        await Assert.That(objectId.ToString()).IsEqualTo("507f1f77bcf86cd799439011");
    }

    [Test]
    public async Task ObjectId_ToByteArray_Should_Return_Correct_Length()
    {
        // Arrange
        var objectId = new ObjectId("507f1f77bcf86cd799439011");

        // Act
        var bytes = objectId.ToByteArray();

        // Assert
        await Assert.That(bytes.Length).IsEqualTo(12);
    }

    [Test]
    public async Task ObjectId_Equality_Should_Work_Correctly()
    {
        // Arrange
        var id1 = new ObjectId("507f1f77bcf86cd799439011");
        var id2 = new ObjectId("507f1f77bcf86cd799439011");
        var id3 = new ObjectId("507f1f77bcf86cd799439012");

        // Act & Assert
        await Assert.That(id1.Equals(id2)).IsTrue();
        await Assert.That(id1 == id2).IsTrue();
        await Assert.That(id1.Equals(id3)).IsFalse();
        await Assert.That(id1 == id3).IsFalse();
        await Assert.That(id1 != id3).IsTrue();
    }

    [Test]
    public async Task ObjectId_GetHashCode_Should_Be_Consistent()
    {
        // Arrange
        var id1 = new ObjectId("507f1f77bcf86cd799439011");
        var id2 = new ObjectId("507f1f77bcf86cd799439011");

        // Act & Assert
        await Assert.That(id1.GetHashCode()).IsEqualTo(id2.GetHashCode());
    }

    [Test]
    public async Task ObjectId_Comparison_Should_Work_Correctly()
    {
        // Arrange
        var id1 = new ObjectId("507f1f77bcf86cd799439011");
        var id2 = new ObjectId("507f1f77bcf86cd799439012");

        // Act & Assert
        await Assert.That(id1.CompareTo(id2)).IsNegative();
        await Assert.That(id2.CompareTo(id1)).IsPositive();
        await Assert.That(id1.CompareTo(id1)).IsEqualTo(0);
    }

    [Test]
    public async Task ObjectId_ToString_Should_Return_Correct_Format()
    {
        // Arrange
        var objectId = new ObjectId("507f1f77bcf86cd799439011");

        // Act
        var stringRepresentation = objectId.ToString();

        // Assert
        await Assert.That(stringRepresentation).IsEqualTo("507f1f77bcf86cd799439011");
        await Assert.That(stringRepresentation.Length).IsEqualTo(24);
    }

    [Test]
    public async Task ObjectId_Implicit_String_Conversion_Should_Work()
    {
        // Arrange
        var objectId = new ObjectId("507f1f77bcf86cd799439011");

        // Act
        string idString = objectId.ToString();

        // Assert
        await Assert.That(idString).IsEqualTo("507f1f77bcf86cd799439011");
    }

    [Test]
    public async Task ObjectId_Constructor_Should_Handle_Empty_Bytes()
    {
        // Arrange
        var emptyBytes = new byte[12];

        // Act
        var objectId = new ObjectId(emptyBytes);

        // Assert
        await Assert.That(objectId.ToString()).IsEqualTo("000000000000000000000000");
    }

    [Test]
    public async Task ObjectId_Constructor_Should_Throw_On_Null_Bytes()
    {
        // Arrange & Act & Assert
        await Assert.That(() => new ObjectId((byte[])null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ObjectId_Constructor_Should_Throw_On_Invalid_Length()
    {
        // Arrange
        var invalidBytes = new byte[10];

        // Act & Assert
        await Assert.That(() => new ObjectId(invalidBytes)).Throws<ArgumentException>();
    }
}