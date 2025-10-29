using System.Diagnostics.CodeAnalysis;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BasicBsonTypeTests
{
    [Test]
    public async Task Binary_Should_Serialize_And_Deserialize_Correctly()
    {
        // Arrange
        var originalData = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        var binary = new BsonBinary(originalData, BsonBinary.BinarySubType.Generic);

        // Act
        var serialized = BsonSerializer.Serialize(binary);
        var deserialized = BsonSerializer.Deserialize(serialized);

        // Assert
        await Assert.That(deserialized.GetType()).IsEqualTo(typeof(BsonBinary));
        var binaryResult = (BsonBinary)deserialized;
        await Assert.That(binaryResult.Bytes.SequenceEqual(originalData)).IsTrue();
        await Assert.That(binaryResult.SubType).IsEqualTo(BsonBinary.BinarySubType.Generic);
    }

    [Test]
    public async Task RegularExpression_Should_Serialize_And_Deserialize_Correctly()
    {
        // Arrange
        var regex = new BsonRegularExpression(@"^test.*regex$", "im");

        // Act
        var serialized = BsonSerializer.Serialize(regex);
        var deserialized = BsonSerializer.Deserialize(serialized);

        // Assert
        await Assert.That(deserialized.GetType()).IsEqualTo(typeof(BsonRegularExpression));
        var regexResult = (BsonRegularExpression)deserialized;
        await Assert.That(regexResult.Pattern).IsEqualTo(@"^test.*regex$");
        await Assert.That(regexResult.Options).IsEqualTo("im");
    }

    [Test]
    public async Task Timestamp_Should_Serialize_And_Deserialize_Correctly()
    {
        // Arrange
        var timestamp = new BsonTimestamp(1634567890, 12345);

        // Act
        var serialized = BsonSerializer.Serialize(timestamp);
        var deserialized = BsonSerializer.Deserialize(serialized);

        // Assert
        await Assert.That(deserialized.GetType()).IsEqualTo(typeof(BsonTimestamp));
        var timestampResult = (BsonTimestamp)deserialized;
        await Assert.That(timestampResult.Timestamp).IsEqualTo(1634567890);
        await Assert.That(timestampResult.Increment).IsEqualTo(12345);
    }

    [Test]
    public async Task Document_With_New_Types_Should_Serialize_And_Deserialize_Correctly()
    {
        // Arrange
        var document = new BsonDocument()
            .Set("nullValue", BsonNull.Value)
            .Set("stringValue", "hello world")
            .Set("intValue", 42)
            .Set("binary", new BsonBinary(new byte[] { 1, 2, 3, 4 }))
            .Set("regex", new BsonRegularExpression(@"\d+", "i"))
            .Set("timestamp", BsonTimestamp.CreateCurrent());

        // Act
        var serialized = BsonSerializer.Serialize(document);
        var deserialized = BsonSerializer.Deserialize(serialized);

        // Assert
        await Assert.That(deserialized.GetType()).IsEqualTo(typeof(BsonDocument));
        var docResult = (BsonDocument)deserialized;

        await Assert.That(docResult["nullValue"]).IsEqualTo(BsonNull.Value);
        await Assert.That(docResult["binary"].GetType()).IsEqualTo(typeof(BsonBinary));
        await Assert.That(docResult["regex"].GetType()).IsEqualTo(typeof(BsonRegularExpression));
        await Assert.That(docResult["timestamp"].GetType()).IsEqualTo(typeof(BsonTimestamp));
    }

    [Test]
    public async Task Array_With_New_Types_Should_Serialize_And_Deserialize_Correctly()
    {
        // Arrange
        var array = new BsonArray()
            .AddValue(BsonNull.Value)
            .AddValue("string")
            .AddValue(42)
            .AddValue(new BsonBinary(new byte[] { 5, 6, 7 }))
            .AddValue(new BsonRegularExpression(@"test.*", "i"))
            .AddValue(BsonTimestamp.CreateCurrent());

        // Act
        var serialized = BsonSerializer.Serialize(array);
        var deserialized = BsonSerializer.Deserialize(serialized);

        // Assert
        await Assert.That(deserialized.GetType()).IsEqualTo(typeof(BsonArray));
        var arrayResult = (BsonArray)deserialized;
        await Assert.That(arrayResult.Count).IsEqualTo(6);
        await Assert.That(arrayResult[0]).IsEqualTo(BsonNull.Value);
        await Assert.That(arrayResult[3].GetType()).IsEqualTo(typeof(BsonBinary));
        await Assert.That(arrayResult[4].GetType()).IsEqualTo(typeof(BsonRegularExpression));
        await Assert.That(arrayResult[5].GetType()).IsEqualTo(typeof(BsonTimestamp));
    }
}