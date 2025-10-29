using System.Diagnostics.CodeAnalysis;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class CompleteBsonTypeTests
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
    public async Task Binary_Should_Support_Different_SubTypes()
    {
        // Arrange
        var testData = new byte[] { 0x12, 0x34, 0x56, 0x78 };
        var uuidBinary = new BsonBinary(testData, BsonBinary.BinarySubType.Uuid);

        // Act
        var serialized = BsonSerializer.Serialize(uuidBinary);
        var deserialized = BsonSerializer.Deserialize(serialized);

        // Assert
        await Assert.That(deserialized.GetType()).IsEqualTo(typeof(BsonBinary));
        var result = (BsonBinary)deserialized;
        await Assert.That(result.SubType).IsEqualTo(BsonBinary.BinarySubType.Uuid);
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
    public async Task RegularExpression_Should_Convert_To_And_From_DotNet_Regex()
    {
        // Arrange
        var dotNetRegex = new System.Text.RegularExpressions.Regex(@"\d+", System.Text.RegularExpressions.RegexOptions.IgnoreCase);

        // Act
        var bsonRegex = BsonRegularExpression.FromRegex(dotNetRegex);
        var convertedBack = bsonRegex.ToRegex();

        // Assert
        await Assert.That(bsonRegex.Pattern).IsEqualTo(@"\d+");
        await Assert.That(convertedBack.Options).IsEqualTo(System.Text.RegularExpressions.RegexOptions.IgnoreCase);
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
        await Assert.That(timestampResult.Value).IsEqualTo(timestamp.Value);
    }

    [Test]
    public async Task Timestamp_Should_Create_Current_Timestamp()
    {
        // Arrange & Act
        var currentTimestamp = BsonTimestamp.CreateCurrent(100);

        // Assert
        await Assert.That(currentTimestamp.Increment).IsEqualTo(100);
        await Assert.That(currentTimestamp.Timestamp).IsGreaterThan(1600000000); // 2020年之后的时间戳
    }

    [Test]
    public async Task Document_With_Supported_Bson_Types_Should_Serialize_And_Deserialize_Correctly()
    {
        // Arrange
        var document = new BsonDocument()
            .Set("nullValue", BsonNull.Value)
            .Set("stringValue", "hello world")
            .Set("intValue", 42)
            .Set("longValue", 1234567890L)
            .Set("doubleValue", 3.14159)
            .Set("boolValue", true)
            .Set("objectId", ObjectId.NewObjectId())
            .Set("dateTime", DateTime.UtcNow)
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
        await Assert.That(docResult["stringValue"].GetType()).IsEqualTo(typeof(BsonString));
        await Assert.That(docResult["intValue"].GetType()).IsEqualTo(typeof(BsonInt32));
        await Assert.That(docResult["longValue"].GetType()).IsEqualTo(typeof(BsonInt64));
        await Assert.That(docResult["doubleValue"].GetType()).IsEqualTo(typeof(BsonDouble));
        await Assert.That(docResult["boolValue"].GetType()).IsEqualTo(typeof(BsonBoolean));
        await Assert.That(docResult["binary"].GetType()).IsEqualTo(typeof(BsonBinary));
        await Assert.That(docResult["regex"].GetType()).IsEqualTo(typeof(BsonRegularExpression));
        await Assert.That(docResult["timestamp"].GetType()).IsEqualTo(typeof(BsonTimestamp));
    }

    [Test]
    public async Task Array_With_Supported_Types_Should_Serialize_And_Deserialize_Correctly()
    {
        // Arrange
        var array = new BsonArray()
            .AddValue(BsonNull.Value)
            .AddValue("string")
            .AddValue(42)
            .AddValue(3.14)
            .AddValue(true)
            .AddValue(ObjectId.NewObjectId())
            .AddValue(DateTime.UtcNow)
            .AddValue(new BsonBinary(new byte[] { 5, 6, 7 }))
            .AddValue(new BsonRegularExpression(@"test.*", "i"))
            .AddValue(BsonTimestamp.CreateCurrent());

        // Act
        var serialized = BsonSerializer.Serialize(array);
        var deserialized = BsonSerializer.Deserialize(serialized);

        // Assert
        await Assert.That(deserialized.GetType()).IsEqualTo(typeof(BsonArray));
        var arrayResult = (BsonArray)deserialized;
        await Assert.That(arrayResult.Count).IsEqualTo(10);
        await Assert.That(arrayResult[0]).IsEqualTo(BsonNull.Value);
        await Assert.That(arrayResult[1].GetType()).IsEqualTo(typeof(BsonString));
        await Assert.That(arrayResult[2].GetType()).IsEqualTo(typeof(BsonInt32));
        await Assert.That(arrayResult[9].GetType()).IsEqualTo(typeof(BsonTimestamp));
    }

    // TODO: 添加其他BSON类型的测试，当它们被实现时：
    // - BsonJavaScript
    // - BsonDecimal128
    // - BsonSymbol
    // - BsonUndefined
    // - BsonMinKey
    // - BsonMaxKey
}