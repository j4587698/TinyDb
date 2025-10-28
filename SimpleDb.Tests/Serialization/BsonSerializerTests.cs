using System.Diagnostics.CodeAnalysis;
using SimpleDb.Serialization;
using SimpleDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Serialization;

public class BsonSerializerTests
{
    [Test]
    public async Task Serialize_Should_Convert_BsonString_To_Bytes()
    {
        // Arrange
        var stringValue = new BsonString("Hello World");

        // Act
        var bytes = BsonSerializer.Serialize(stringValue);

        // Assert
        await Assert.That(bytes).IsNotNull();
        await Assert.That(bytes.Length).IsGreaterThan(0);
    }

    [Test]
    public async Task Serialize_Should_Convert_BsonInt32_To_Bytes()
    {
        // Arrange
        var intValue = new BsonInt32(42);

        // Act
        var bytes = BsonSerializer.Serialize(intValue);

        // Assert
        await Assert.That(bytes).IsNotNull();
        await Assert.That(bytes.Length).IsGreaterThan(0);
    }

    [Test]
    public async Task Serialize_Should_Convert_BsonDocument_To_Bytes()
    {
        // Arrange
        var document = new BsonDocument();
        document = document.Set("name", new BsonString("Test"));
        document = document.Set("age", new BsonInt32(25));
        document = document.Set("active", new BsonBoolean(true));

        // Act
        var bytes = BsonSerializer.Serialize(document);

        // Assert
        await Assert.That(bytes).IsNotNull();
        await Assert.That(bytes.Length).IsGreaterThan(0);
    }

    [Test]
    public async Task Serialize_Should_Convert_BsonArray_To_Bytes()
    {
        // Arrange
        var array = new BsonArray();
        array = array.AddValue("item1");
        array = array.AddValue(123);
        array = array.AddValue(true);

        // Act
        var bytes = BsonSerializer.Serialize(array);

        // Assert
        await Assert.That(bytes).IsNotNull();
        await Assert.That(bytes.Length).IsGreaterThan(0);
    }

    [Test]
    public async Task Deserialize_Should_Convert_Bytes_Back_To_BsonString()
    {
        // Arrange
        var originalValue = new BsonString("Hello World");
        var bytes = BsonSerializer.Serialize(originalValue);

        // Act
        var deserializedValue = BsonSerializer.Deserialize(bytes);

        // Assert
        await Assert.That(deserializedValue).IsNotNull();
        await Assert.That(deserializedValue.BsonType).IsEqualTo(BsonType.String);
        await Assert.That(deserializedValue.ToString()).IsEqualTo("Hello World");
    }

    [Test]
    public async Task Deserialize_Should_Convert_Bytes_Back_To_BsonInt32()
    {
        // Arrange
        var originalValue = new BsonInt32(42);
        var bytes = BsonSerializer.Serialize(originalValue);

        // Act
        var deserializedValue = BsonSerializer.Deserialize(bytes);

        // Assert
        await Assert.That(deserializedValue).IsNotNull();
        await Assert.That(deserializedValue.BsonType).IsEqualTo(BsonType.Int32);
        await Assert.That(deserializedValue.ToInt32(null)).IsEqualTo(42);
    }

    [Test]
    public async Task Deserialize_Should_Convert_Bytes_Back_To_BsonDocument()
    {
        // Arrange
        var originalDocument = new BsonDocument();
        originalDocument = originalDocument.Set("name", new BsonString("Test"));
        originalDocument = originalDocument.Set("age", new BsonInt32(25));
        originalDocument = originalDocument.Set("active", new BsonBoolean(true));
        var bytes = BsonSerializer.Serialize(originalDocument);

        // Act
        var deserializedDocument = BsonSerializer.Deserialize(bytes) as BsonDocument;

        // Assert
        await Assert.That(deserializedDocument).IsNotNull();
        await Assert.That(deserializedDocument!.BsonType).IsEqualTo(BsonType.Document);
        await Assert.That(deserializedDocument["name"].ToString()).IsEqualTo("Test");
        await Assert.That(deserializedDocument["age"].ToInt32(null)).IsEqualTo(25);
        await Assert.That(deserializedDocument["active"].ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task Deserialize_Should_Convert_Bytes_Back_To_BsonArray()
    {
        // Arrange
        var originalArray = new BsonArray();
        originalArray = originalArray.AddValue("item1");
        originalArray = originalArray.AddValue(123);
        originalArray = originalArray.AddValue(true);
        var bytes = BsonSerializer.Serialize(originalArray);

        // Act
        var deserializedArray = BsonSerializer.Deserialize(bytes) as BsonArray;

        // Assert
        await Assert.That(deserializedArray).IsNotNull();
        await Assert.That(deserializedArray!.BsonType).IsEqualTo(BsonType.Array);
        await Assert.That(deserializedArray.Count).IsEqualTo(3);
        await Assert.That(deserializedArray[0].ToString()).IsEqualTo("item1");
        await Assert.That(deserializedArray[1].ToInt32(null)).IsEqualTo(123);
        await Assert.That(deserializedArray[2].ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task Serialize_Should_Preserve_BsonDecimal128_Precision()
    {
        // Arrange
        const decimal original = 1234567890123456.987654321m;
        var decimalValue = new BsonDecimal128(original);

        // Act
        var bytes = BsonSerializer.Serialize(decimalValue);
        var deserialized = BsonSerializer.Deserialize(bytes) as BsonDecimal128;

        // Assert
        await Assert.That(deserialized).IsNotNull();
        await Assert.That(deserialized!.Value).IsEqualTo(original);
    }

    [Test]
    public async Task Document_With_Decimal_Should_RoundTrip()
    {
        // Arrange
        const decimal price = 199.99m;
        var document = new BsonDocument().Set("price", new BsonDecimal128(price));

        // Act
        var bytes = BsonSerializer.Serialize(document);
        var deserialized = BsonSerializer.Deserialize(bytes) as BsonDocument;

        // Assert
        await Assert.That(deserialized).IsNotNull();
        var priceValue = deserialized!["price"];
        await Assert.That(priceValue.GetType()).IsEqualTo(typeof(BsonDecimal128));
        await Assert.That(((BsonDecimal128)priceValue).Value).IsEqualTo(price);
    }

    [Test]
    public async Task Deserialize_Should_Return_BsonNull_For_Empty_Bytes()
    {
        // Arrange
        var emptyBytes = new byte[0];

        // Act
        var result = BsonSerializer.Deserialize(emptyBytes);

        // Assert
        await Assert.That(result).IsNotNull();
        await Assert.That(result.BsonType).IsEqualTo(BsonType.Null);
    }

    [Test]
    public async Task Deserialize_Should_Throw_For_Null_Bytes()
    {
        // Act & Assert
        await Assert.That(() => BsonSerializer.Deserialize(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task RoundTrip_Should_Preserve_All_BsonTypes()
    {
        // Arrange & Act - Test all BSON types
        var testCases = new List<BsonValue>
        {
            new BsonString("Test String"),
            new BsonInt32(42),
            new BsonInt64(9876543210L),
            new BsonDouble(3.14159),
            new BsonBoolean(true),
            new BsonBoolean(false),
            BsonNull.Value,
            new BsonObjectId(ObjectId.NewObjectId()),
            new BsonDateTime(DateTime.UtcNow),
            new BsonDecimal128(6543210.123456789m)
        };

        foreach (var originalValue in testCases)
        {
            // Act
            var bytes = BsonSerializer.Serialize(originalValue);
            var deserializedValue = BsonSerializer.Deserialize(bytes);

            // Assert
            await Assert.That(deserializedValue).IsNotNull();
            await Assert.That(deserializedValue.BsonType).IsEqualTo(originalValue.BsonType);

            // More detailed comparison with better error message
            bool areEqual;
            if (originalValue is BsonDateTime && deserializedValue is BsonDateTime)
            {
                // For DateTime, compare with tolerance for precision differences
                var originalDt = ((BsonDateTime)originalValue).Value;
                var deserializedDt = ((BsonDateTime)deserializedValue).Value;
                areEqual = Math.Abs((originalDt - deserializedDt).TotalMilliseconds) < 1;
            }
            else
            {
                areEqual = deserializedValue.Equals(originalValue);
            }

            if (!areEqual)
            {
                await Assert.That($"Equals failed for type {originalValue.BsonType}: original='{originalValue}', deserialized='{deserializedValue}'").IsEqualTo("Should not reach here");
            }
        }
    }

    [Test]
    public async Task Serialize_Deserialize_Should_Handle_Complex_Document()
    {
        // Arrange
        var complexDocument = new BsonDocument();
        complexDocument = complexDocument.Set("user", new BsonDocument()
            .Set("name", new BsonString("John Doe"))
            .Set("age", new BsonInt32(30))
            .Set("tags", new BsonArray()
                .AddValue("developer")
                .AddValue("admin")));
        complexDocument = complexDocument.Set("metadata", new BsonDocument()
            .Set("created", new BsonDateTime(DateTime.UtcNow))
            .Set("version", new BsonDouble(1.0)));

        // Act
        var bytes = BsonSerializer.Serialize(complexDocument);
        var deserializedDocument = BsonSerializer.Deserialize(bytes) as BsonDocument;

        // Assert
        await Assert.That(deserializedDocument).IsNotNull();
        await Assert.That(deserializedDocument!.BsonType).IsEqualTo(BsonType.Document);
        await Assert.That(deserializedDocument.ContainsKey("user")).IsTrue();
        await Assert.That(deserializedDocument.ContainsKey("metadata")).IsTrue();

        var userDoc = deserializedDocument["user"] as BsonDocument;
        await Assert.That(userDoc).IsNotNull();
        await Assert.That(userDoc!["name"].ToString()).IsEqualTo("John Doe");
        await Assert.That(userDoc["age"].ToInt32(null)).IsEqualTo(30);

        var tagsArray = userDoc["tags"] as BsonArray;
        await Assert.That(tagsArray).IsNotNull();
        await Assert.That(tagsArray!.Count).IsEqualTo(2);
        await Assert.That(tagsArray[0].ToString()).IsEqualTo("developer");
        await Assert.That(tagsArray[1].ToString()).IsEqualTo("admin");
    }

    [Test]
    public async Task Serialize_Should_Handle_Large_Values()
    {
        // Arrange
        var largeString = new string('A', 10000);
        var largeArray = new BsonArray();
        for (int i = 0; i < 1000; i++)
        {
            largeArray = largeArray.AddValue($"item_{i}");
        }

        // Act
        var stringBytes = BsonSerializer.Serialize(new BsonString(largeString));
        var arrayBytes = BsonSerializer.Serialize(largeArray);

        // Assert
        await Assert.That(stringBytes.Length).IsGreaterThan(10000);
        await Assert.That(arrayBytes.Length).IsGreaterThan(1000);

        // Verify round trip
        var deserializedString = BsonSerializer.Deserialize(stringBytes);
        var deserializedArray = BsonSerializer.Deserialize(arrayBytes) as BsonArray;

        await Assert.That(deserializedString.ToString()).IsEqualTo(largeString);
        await Assert.That(deserializedArray).IsNotNull();
        await Assert.That(deserializedArray!.Count).IsEqualTo(1000);
        await Assert.That(deserializedArray[999].ToString()).IsEqualTo("item_999");
    }

    [Test]
    public async Task Serialize_Should_Handle_Special_Characters()
    {
        // Arrange
        var specialChars = "Hello 🌍 World! \n\t\r\"'\\中文测试";

        // Act
        var bytes = BsonSerializer.Serialize(new BsonString(specialChars));
        var deserialized = BsonSerializer.Deserialize(bytes);

        // Assert
        await Assert.That(deserialized.ToString()).IsEqualTo(specialChars);
    }

    [Test]
    public async Task Multiple_Serialize_Deserialize_Should_Be_Consistent()
    {
        // Arrange
        var originalDocument = new BsonDocument();
        originalDocument = originalDocument.Set("counter", new BsonInt32(1));
        originalDocument = originalDocument.Set("data", new BsonString("test"));

        // Act
        var bytes1 = BsonSerializer.Serialize(originalDocument);
        var bytes2 = BsonSerializer.Serialize(originalDocument);
        var doc1 = BsonSerializer.Deserialize(bytes1) as BsonDocument;
        var doc2 = BsonSerializer.Deserialize(bytes2) as BsonDocument;

        // Assert
        await Assert.That(bytes1).IsNotNull();
        await Assert.That(bytes2).IsNotNull();
        await Assert.That(bytes1.Length).IsEqualTo(bytes2.Length);
        await Assert.That(doc1!.Equals(doc2)).IsTrue();
    }
}
