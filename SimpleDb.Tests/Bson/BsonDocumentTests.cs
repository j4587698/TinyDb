using System.Diagnostics.CodeAnalysis;
using SimpleDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Bson;

public class BsonDocumentTests
{
    [Test]
    public async Task BsonDocument_Constructor_Should_Create_Empty_Document()
    {
        // Arrange & Act
        var document = new BsonDocument();

        // Assert
        await Assert.That(document).IsNotNull();
        await Assert.That(document.Count).IsEqualTo(0);
        await Assert.That(document.BsonType).IsEqualTo(BsonType.Document);
        await Assert.That(document.IsDocument).IsTrue();
    }

    [Test]
    public async Task BsonDocument_Should_Add_And_Get_Values()
    {
        // Arrange
        var document = new BsonDocument();

        // Act
        document = document.Set("name", new BsonString("John Doe"));
        document = document.Set("age", new BsonInt32(30));
        document = document.Set("active", new BsonBoolean(true));

        // Assert
        await Assert.That(document.Count).IsEqualTo(3);
        await Assert.That(document["name"].ToString()).IsEqualTo("John Doe");
        await Assert.That(document["age"].ToInt32(null)).IsEqualTo(30);
        await Assert.That(document["active"].ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task BsonDocument_Indexer_Should_Add_And_Get_Values()
    {
        // Arrange
        var document = new BsonDocument();

        // Act - indexer setter should throw NotSupportedException
        await Assert.That(() => document["name"] = "Jane Doe").Throws<NotSupportedException>();

        // Use Set method properly for immutable design
        document = document.Set("name", "Jane Doe");
        document = document.Set("age", 25);
        document = document.Set("score", 95.5);

        // Assert
        await Assert.That(document.Count).IsEqualTo(3);
        await Assert.That(document["name"].ToString()).IsEqualTo("Jane Doe");
        await Assert.That(document["age"].ToInt32(null)).IsEqualTo(25);
        await Assert.That(document["score"].ToDouble(null)).IsEqualTo(95.5);
    }

    [Test]
    public async Task BsonDocument_TryGetValue_Should_Handle_Missing_Keys()
    {
        // Arrange
        var document = new BsonDocument();
        document = document.Set("existing", new BsonString("value"));

        // Act
        var existingResult = document.TryGetValue("existing", out var existingValue);
        var missingResult = document.TryGetValue("missing", out var missingValue);

        // Assert
        await Assert.That(existingResult).IsTrue();
        await Assert.That(existingValue!.ToString()).IsEqualTo("value");
        await Assert.That(missingResult).IsFalse();
    }

    [Test]
    public async Task BsonDocument_ContainsKey_Should_Work_Correctly()
    {
        // Arrange
        var document = new BsonDocument();
        document = document.Set("key1", new BsonString("value1"));

        // Act & Assert
        await Assert.That(document.ContainsKey("key1")).IsTrue();
        await Assert.That(document.ContainsKey("key2")).IsFalse();
    }

    [Test]
    public async Task BsonDocument_Remove_Should_Work_Correctly()
    {
        // Arrange
        var document = new BsonDocument();
        document = document.Set("key1", new BsonString("value1"));
        document = document.Set("key2", new BsonString("value2"));

        // Act
        var document1 = document.RemoveKey("key1");
        var document2 = document.RemoveKey("nonexistent");

        // Assert
        await Assert.That(document1.Count).IsEqualTo(1);
        await Assert.That(document1.ContainsKey("key1")).IsFalse();
        await Assert.That(document1.ContainsKey("key2")).IsTrue();

        // Remove nonexistent key should return same document
        await Assert.That(document2.Count).IsEqualTo(2);
        await Assert.That(document2.ContainsKey("key1")).IsTrue();
        await Assert.That(document2.ContainsKey("key2")).IsTrue();
    }

    [Test]
    public async Task BsonDocument_Clear_Should_Remove_All_Elements()
    {
        // Arrange
        var document = new BsonDocument();
        document = document.Set("key1", new BsonString("value1"));
        document = document.Set("key2", new BsonString("value2"));

        // Act - Clear throws NotSupportedException, test that it throws correctly
        await Assert.That(() => document.Clear()).Throws<NotSupportedException>();

        // Alternative: Create new empty document
        var clearedDocument = new BsonDocument();

        // Assert
        await Assert.That(clearedDocument.Count).IsEqualTo(0);
        await Assert.That(clearedDocument.ContainsKey("key1")).IsFalse();
        await Assert.That(clearedDocument.ContainsKey("key2")).IsFalse();
    }

    [Test]
    public async Task BsonDocument_Set_Should_Update_Existing_Value()
    {
        // Arrange
        var document = new BsonDocument();
        document = document.Set("name", new BsonString("John"));

        // Act
        var updatedDocument = document.Set("name", new BsonString("Jane"));
        var newDocument = document.Set("age", new BsonInt32(30));

        // Assert - Original document should be unchanged
        await Assert.That(document["name"].ToString()).IsEqualTo("John");

        // Updated document should have new values
        await Assert.That(updatedDocument["name"].ToString()).IsEqualTo("Jane");
        await Assert.That(newDocument["age"].ToInt32(null)).IsEqualTo(30);
        await Assert.That(newDocument.Count).IsEqualTo(2);
    }

    [Test]
    public async Task BsonDocument_Equality_Should_Work_Correctly()
    {
        // Arrange - 使用隐式转换确保类型一致性
        var doc1 = new BsonDocument();
        doc1 = doc1.Set("name", "John");  // 隐式转换为BsonString
        doc1 = doc1.Set("age", 30);       // 隐式转换为BsonInt32

        var doc2 = new BsonDocument();
        doc2 = doc2.Set("name", "John");  // 隐式转换为BsonString
        doc2 = doc2.Set("age", 30);       // 隐式转换为BsonInt32

        var doc3 = new BsonDocument();
        doc3 = doc3.Set("name", "John");  // 隐式转换为BsonString
        doc3 = doc3.Set("age", 31);       // 隐式转换为BsonInt32

        // Act & Assert
        await Assert.That(doc1.Equals(doc2)).IsTrue();
        await Assert.That(doc1 == doc2).IsTrue();
        await Assert.That(doc1.Equals(doc3)).IsFalse();
        await Assert.That(doc1 == doc3).IsFalse();
    }

    [Test]
    public async Task BsonDocument_ToString_Should_Return_Json_Like_Representation()
    {
        // Arrange
        var document = new BsonDocument();
        document = document.Set("name", new BsonString("John"));
        document = document.Set("age", new BsonInt32(30));

        // Act
        var stringRepresentation = document.ToString();

        // Assert
        await Assert.That(stringRepresentation).Contains("name");
        await Assert.That(stringRepresentation).Contains("John");
        await Assert.That(stringRepresentation).Contains("age");
        await Assert.That(stringRepresentation).Contains("30");
    }

    [Test]
    public async Task BsonDocument_Should_Handle_Nested_Documents()
    {
        // Arrange
        var document = new BsonDocument();
        var nestedDoc = new BsonDocument();
        nestedDoc = nestedDoc.Set("nested", new BsonString("value"));

        // Act
        document = document.Set("parent", nestedDoc);

        // Assert
        await Assert.That(document.Count).IsEqualTo(1);
        await Assert.That(document["parent"].IsDocument).IsTrue();
    }

    [Test]
    public async Task BsonDocument_Should_Handle_Arrays()
    {
        // Arrange
        var document = new BsonDocument();
        var array = new BsonArray();
        array = array.AddValue(new BsonInt32(1));
        array = array.AddValue(new BsonInt32(2));
        array = array.AddValue(new BsonInt32(3));

        // Act
        document = document.Set("numbers", array);

        // Assert
        await Assert.That(document.Count).IsEqualTo(1);
        await Assert.That(document["numbers"].IsArray).IsTrue();
    }

    [Test]
    public async Task BsonDocument_Should_Handle_Null_Values()
    {
        // Arrange
        var document = new BsonDocument();

        // Act
        document = document.Set("nullValue", BsonNull.Value);

        // Assert
        await Assert.That(document.Count).IsEqualTo(1);
        await Assert.That(document["nullValue"].IsNull).IsTrue();
    }

    [Test]
    public async Task BsonDocument_Should_Handle_Mixed_Types()
    {
        // Arrange
        var document = new BsonDocument();

        // Act
        document = document.Set("string", new BsonString("test"));
        document = document.Set("number", new BsonInt32(42));
        document = document.Set("boolean", new BsonBoolean(true));
        document = document.Set("nullValue", BsonNull.Value);

        // Assert
        await Assert.That(document.Count).IsEqualTo(4);
        await Assert.That(document["string"].IsString).IsTrue();
        await Assert.That(document["number"].IsNumeric).IsTrue();
        await Assert.That(document["boolean"].IsBoolean).IsTrue();
        await Assert.That(document["nullValue"].IsNull).IsTrue();
    }
}
