using System.Diagnostics.CodeAnalysis;
using System.Linq;
using SimpleDb.Bson;
using SimpleDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Bson;

public class SimpleBsonTest
{
    [Test]
    public async Task SimpleBinaryTest()
    {
        // Arrange
        var binary = new BsonBinary(new byte[] { 1, 2, 3, 4 }, BsonBinary.BinarySubType.Generic);

        // Act
        var serialized = BsonSerializer.Serialize(binary);
        Console.WriteLine($"Serialized {serialized.Length} bytes: [{string.Join(", ", serialized)}]");

        var deserialized = BsonSerializer.Deserialize(serialized);

        // Assert
        await Assert.That(deserialized.GetType()).IsEqualTo(typeof(BsonBinary));
        var binaryResult = (BsonBinary)deserialized;
        await Assert.That(binaryResult.Bytes.SequenceEqual(new byte[] { 1, 2, 3, 4 })).IsTrue();
    }

    [Test]
    public async Task SimpleRegexTest()
    {
        // Arrange
        var regex = new BsonRegularExpression(@"\d+", "i");

        // Act
        var serialized = BsonSerializer.Serialize(regex);
        Console.WriteLine($"Serialized {serialized.Length} bytes: [{string.Join(", ", serialized)}]");

        var deserialized = BsonSerializer.Deserialize(serialized);

        // Assert
        await Assert.That(deserialized.GetType()).IsEqualTo(typeof(BsonRegularExpression));
        var regexResult = (BsonRegularExpression)deserialized;
        await Assert.That(regexResult.Pattern).IsEqualTo(@"\d+");
        await Assert.That(regexResult.Options).IsEqualTo("i");
    }
}