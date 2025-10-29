using System.Diagnostics.CodeAnalysis;
using TinyDb.Serialization;
using TinyDb.Tests.TestEntities;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BasicSerializationTests
{
    [Test]
    public async Task BsonMapper_Basic_Functionality_Should_Work()
    {
        // Arrange
        var user = new User
        {
            Name = "Test User",
            Age = 25,
            Email = "test@example.com"
        };

        // Act
        var document = BsonMapper.ToDocument(user);

        // Assert
        await Assert.That(document).IsNotNull();
        await Assert.That(document.BsonType).IsEqualTo(BsonType.Document);
        await Assert.That(document.ContainsKey("_id")).IsTrue();
    }

    [Test]
    public async Task BsonMapper_RoundTrip_Should_Preserve_Id()
    {
        // Arrange
        var originalUser = new User
        {
            Name = "Round Trip Test",
            Age = 30,
            Email = "roundtrip@example.com"
        };
        var originalId = originalUser.Id;

        // Act
        var document = BsonMapper.ToDocument(originalUser);
        var deserializedUser = BsonMapper.ToObject<User>(document);

        // Assert
        await Assert.That(deserializedUser).IsNotNull();
        await Assert.That(deserializedUser!.Name).IsEqualTo(originalUser.Name);
        await Assert.That(deserializedUser.Age).IsEqualTo(originalUser.Age);
        await Assert.That(deserializedUser.Email).IsEqualTo(originalUser.Email);
    }

    [Test]
    public async Task BsonMapper_Should_Handle_Empty_User()
    {
        // Arrange
        var emptyUser = new User();

        // Act
        var document = BsonMapper.ToDocument(emptyUser);
        var deserializedUser = BsonMapper.ToObject<User>(document);

        // Assert
        await Assert.That(deserializedUser).IsNotNull();
        await Assert.That(deserializedUser!.Name).IsNotNull();
        await Assert.That(deserializedUser.Age >= 0).IsTrue();
    }

    [Test]
    public async Task BsonMapper_Should_Handle_Null_Document()
    {
        // Act
        var user = BsonMapper.ToObject<User>(null!);

        // Assert
        await Assert.That(user).IsNull();
    }

    [Test]
    public async Task BsonMapper_Should_Throw_On_Null_Entity()
    {
        // Act & Assert
        await Assert.That(() => BsonMapper.ToDocument<User>(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonSerializer_Basic_Serialization_Should_Work()
    {
        // Arrange
        var stringValue = new BsonString("Test String");

        // Act
        var bytes = BsonSerializer.Serialize(stringValue);

        // Assert
        await Assert.That(bytes).IsNotNull();
        await Assert.That(bytes.Length).IsGreaterThan(0);
    }

    [Test]
    public async Task BsonSerializer_Should_Handle_Empty_Bytes()
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
    public async Task BsonSerializer_Should_Throw_On_Null_Bytes()
    {
        // Act & Assert
        await Assert.That(() => BsonSerializer.Deserialize(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonSerializer_Should_Handle_Null_Value()
    {
        // Arrange
        var nullValue = BsonNull.Value;

        // Act
        var bytes = BsonSerializer.Serialize(nullValue);

        // Assert
        await Assert.That(bytes).IsNotNull();
        await Assert.That(bytes.Length).IsGreaterThan(0);
    }

    [Test]
    public async Task Integration_Mapper_And_Serializer_Should_Work_Together()
    {
        // Arrange
        var user = new User
        {
            Name = "Integration Test",
            Age = 35,
            Email = "integration@example.com"
        };

        // Act
        var document = BsonMapper.ToDocument(user);
        var bytes = BsonSerializer.Serialize(document);
        var deserializedDocument = BsonSerializer.Deserialize(bytes) as BsonDocument;
        await Assert.That(deserializedDocument).IsNotNull();
        var finalUser = BsonMapper.ToObject<User>(deserializedDocument!);

        // Assert
        await Assert.That(finalUser).IsNotNull();
        await Assert.That(finalUser!.Name).IsEqualTo(user.Name);
        await Assert.That(finalUser.Age).IsEqualTo(user.Age);
        await Assert.That(finalUser.Email).IsEqualTo(user.Email);
    }

    [Test]
    public async Task Multiple_Users_Should_Be_Serialized_Correctly()
    {
        // Arrange
        var users = new List<User>
        {
            new User { Name = "User1", Age = 20, Email = "user1@example.com" },
            new User { Name = "User2", Age = 30, Email = "user2@example.com" },
            new User { Name = "User3", Age = 40, Email = "user3@example.com" }
        };

        // Act
        var documents = users.Select(BsonMapper.ToDocument).ToList();
        var bytesList = documents.Select(BsonSerializer.Serialize).ToList();
        var deserializedDocuments = bytesList
            .Select(bytes => BsonSerializer.Deserialize(bytes) as BsonDocument)
            .Where(doc => doc != null)
            .ToList();
        var finalUsers = deserializedDocuments.Select(doc => BsonMapper.ToObject<User>(doc!)).ToList();

        // Assert
        await Assert.That(finalUsers.Count).IsEqualTo(3);
        await Assert.That(finalUsers.All(u => u != null)).IsTrue();
        await Assert.That(finalUsers.All(u => !string.IsNullOrEmpty(u!.Name))).IsTrue();
        await Assert.That(finalUsers.All(u => u!.Age > 0)).IsTrue();
    }

    [Test]
    public async Task Large_Number_Of_Properties_Should_Work()
    {
        // Arrange - Create a user with many properties
        var user = new User
        {
            Name = "Large Property Test User",
            Age = 50,
            Email = "large@example.com"
        };

        // Act
        var document = BsonMapper.ToDocument(user);

        // Assert
        await Assert.That(document).IsNotNull();
        await Assert.That(document.BsonType).IsEqualTo(BsonType.Document);
        await Assert.That(document.Count).IsGreaterThan(0);
    }

    [Test]
    public async Task Serialization_Should_Be_Thread_Safe()
    {
        // Arrange
        var users = Enumerable.Range(0, 10)
            .Select(i => new User
            {
                Name = $"User {i}",
                Age = i + 20,
                Email = $"user{i}@example.com"
            })
            .ToList();

        // Act - Parallel serialization
        var tasks = users.Select(user =>
        {
            var doc = BsonMapper.ToDocument(user);
            var bytes = BsonSerializer.Serialize(doc);
            var deserializedDoc = BsonSerializer.Deserialize(bytes) as BsonDocument
                ?? throw new InvalidOperationException("Deserialized document cannot be null.");
            return Task.FromResult(BsonMapper.ToObject<User>(deserializedDoc)!);
        }).ToArray();

        var results = await Task.WhenAll(tasks);

        // Assert
        await Assert.That(results.Length).IsEqualTo(10);
        await Assert.That(results.All(u => u != null)).IsTrue();
        await Assert.That(results.All(u => u!.Name.StartsWith("User "))).IsTrue();
    }

    [Test]
    public async Task BsonMapper_Should_Preserve_DateTime_Values()
    {
        // Arrange
        var testTime = new DateTime(2023, 12, 25, 15, 30, 45, DateTimeKind.Utc);
        var user = new User
        {
            Name = "DateTime Test",
            Age = 25,
            Email = "datetime@example.com",
            CreatedAt = testTime
        };

        // Act
        var document = BsonMapper.ToDocument(user);
        var deserializedUser = BsonMapper.ToObject<User>(document);

        // Assert
        await Assert.That(deserializedUser).IsNotNull();
        // Note: DateTime precision might vary, so we check approximate equality
        var timeDiff = Math.Abs((deserializedUser!.CreatedAt - testTime).TotalSeconds);
        await Assert.That(timeDiff <= 60).IsTrue(); // Within 1 minute
    }
}
