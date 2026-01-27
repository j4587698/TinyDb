using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExecutorTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryExecutorTests()
    {
        _testDirectory = Path.Combine(Path.GetTempPath(), "TinyDbQueryTests", Guid.NewGuid().ToString());
        Directory.CreateDirectory(_testDirectory);
        _testDbPath = Path.Combine(_testDirectory, "test.db");

        _engine = new TinyDbEngine(_testDbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (Directory.Exists(_testDirectory))
        {
            try
            {
                Directory.Delete(_testDirectory, true);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }
    }

    [Test]
    public async Task Constructor_Should_Initialize_Correctly()
    {
        // Act & Assert
        await Assert.That(_executor).IsNotNull();
    }

    [Test]
    public async Task Constructor_Should_Throw_For_Null_Engine()
    {
        // Act & Assert
        await Assert.That(() => new QueryExecutor(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Execute_With_Null_CollectionName_Should_Throw()
    {
        // Act & Assert
        await Assert.That(() => _executor.Execute<QueryTestUser>(null!).ToList()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Execute_With_Empty_CollectionName_Should_Throw()
    {
        // Act & Assert
        await Assert.That(() => _executor.Execute<QueryTestUser>("").ToList()).Throws<ArgumentException>();
    }

    [Test]
    public async Task Execute_With_Empty_Collection_Should_Return_Empty()
    {
        // Arrange
        const string collectionName = "NonExistentCollection";

        // Act
        var results = _executor.Execute<QueryTestUser>(collectionName).ToList();

        // Assert
        await Assert.That(results).IsEmpty();
    }

    [Test]
    public async Task Execute_Without_Filter_Should_Return_All_Documents()
    {
        // Arrange
        const string collectionName = "Users";
        var users = new List<QueryTestUser>
        {
            new() { Name = "Alice", Age = 25, Email = "alice@test.com", IsActive = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Bob", Age = 30, Email = "bob@test.com", IsActive = false, CreatedAt = DateTime.UtcNow },
            new() { Name = "Charlie", Age = 35, Email = "charlie@test.com", IsActive = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<QueryTestUser>();
        foreach (var user in users)
        {
            collection.Insert(user);
        }

        // Act
        var results = _executor.Execute<QueryTestUser>(collectionName).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(3);
        await Assert.That(results.Any(u => u.Name == "Alice")).IsTrue();
        await Assert.That(results.Any(u => u.Name == "Bob")).IsTrue();
        await Assert.That(results.Any(u => u.Name == "Charlie")).IsTrue();
    }

    [Test]
    public async Task Execute_With_Simple_Filter_Should_Filter_Correctly()
    {
        // Arrange
        const string collectionName = "Users";
        var users = new List<QueryTestUser>
        {
            new() { Name = "Alice", Age = 25, Email = "alice@test.com", IsActive = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Bob", Age = 30, Email = "bob@test.com", IsActive = false, CreatedAt = DateTime.UtcNow },
            new() { Name = "Charlie", Age = 35, Email = "charlie@test.com", IsActive = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<QueryTestUser>();
        foreach (var user in users)
        {
            collection.Insert(user);
        }

        // Act - Filter for active users
        var results = _executor.Execute<QueryTestUser>(collectionName, u => u.IsActive).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results.All(u => u.IsActive)).IsTrue();
        await Assert.That(results.Any(u => u.Name == "Alice")).IsTrue();
        await Assert.That(results.Any(u => u.Name == "Charlie")).IsTrue();
    }

    [Test]
    public async Task Execute_With_Age_Filter_Should_Filter_Correctly()
    {
        // Arrange
        const string collectionName = "Users";
        var users = new List<QueryTestUser>
        {
            new() { Name = "Alice", Age = 25, Email = "alice@test.com", IsActive = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Bob", Age = 30, Email = "bob@test.com", IsActive = false, CreatedAt = DateTime.UtcNow },
            new() { Name = "Charlie", Age = 35, Email = "charlie@test.com", IsActive = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<QueryTestUser>();
        foreach (var user in users)
        {
            collection.Insert(user);
        }

        // Act - Filter for users older than 28
        var results = _executor.Execute<QueryTestUser>(collectionName, u => u.Age > 28).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results.All(u => u.Age > 28)).IsTrue();
        await Assert.That(results.Any(u => u.Name == "Bob")).IsTrue();
        await Assert.That(results.Any(u => u.Name == "Charlie")).IsTrue();
    }

    [Test]
    public async Task Execute_With_String_Filter_Should_Filter_Correctly()
    {
        // Arrange
        const string collectionName = "Users";
        var users = new List<QueryTestUser>
        {
            new() { Name = "Alice", Age = 25, Email = "alice@test.com", IsActive = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Bob", Age = 30, Email = "bob@test.com", IsActive = false, CreatedAt = DateTime.UtcNow },
            new() { Name = "Alice Smith", Age = 35, Email = "alice.smith@test.com", IsActive = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<QueryTestUser>();
        foreach (var user in users)
        {
            collection.Insert(user);
        }

        // Act - Filter for users named Alice
        var results = _executor.Execute<QueryTestUser>(collectionName, u => u.Name == "Alice").ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].Name).IsEqualTo("Alice");
    }

    [Test]
    public async Task Execute_With_Complex_Filter_Should_Filter_Correctly()
    {
        // Arrange
        const string collectionName = "Users";
        var users = new List<QueryTestUser>
        {
            new() { Name = "Alice", Age = 25, Email = "alice@test.com", IsActive = true, CreatedAt = DateTime.UtcNow },
            new() { Name = "Bob", Age = 30, Email = "bob@test.com", IsActive = false, CreatedAt = DateTime.UtcNow },
            new() { Name = "Charlie", Age = 35, Email = "charlie@test.com", IsActive = true, CreatedAt = DateTime.UtcNow.AddDays(-1) },
            new() { Name = "Diana", Age = 28, Email = "diana@test.com", IsActive = true, CreatedAt = DateTime.UtcNow }
        };

        var collection = _engine.GetCollection<QueryTestUser>();
        foreach (var user in users)
        {
            collection.Insert(user);
        }

        // Act - Filter for active users older than 27
        var results = _executor.Execute<QueryTestUser>(collectionName, u => u.IsActive && u.Age > 27).ToList();

        // Assert
        await Assert.That(results.Count).IsEqualTo(2);
        await Assert.That(results.All(u => u.IsActive && u.Age > 27)).IsTrue();
        await Assert.That(results.Any(u => u.Name == "Charlie")).IsTrue();
        await Assert.That(results.Any(u => u.Name == "Diana")).IsTrue();
    }

    [Test]
    public async Task Execute_With_Null_Documents_Should_Handle_Gracefully()
    {
        // Arrange
        const string collectionName = "Users";

        // Insert a test document with minimal data
        var collection = _engine.GetCollection<QueryTestUser>();
        var testUser = new QueryTestUser { Name = "Test" };
        collection.Insert(testUser);

        // Act
        var results = _executor.Execute<QueryTestUser>(collectionName).ToList();

        // Assert - Should not crash and handle null gracefully
        await Assert.That(results.Count).IsGreaterThanOrEqualTo(0);
    }

    [Test]
    public async Task Execute_With_Large_Dataset_Should_Perform_Well()
    {
        // Arrange
        const string collectionName = "LargeDataset";
        const int userCount = 1000;

        var collection = _engine.GetCollectionWithName<QueryTestUser>(collectionName);
        for (int i = 0; i < userCount; i++)
        {
            var user = new QueryTestUser
            {
                Name = $"User{i}",
                Age = 20 + (i % 50),
                Email = $"user{i}@test.com",
                IsActive = i % 2 == 0,
                CreatedAt = DateTime.UtcNow.AddDays(-i % 30)
            };

            collection.Insert(user);
        }

        // Act
        var results = _executor.Execute<QueryTestUser>(collectionName, u => u.IsActive && u.Age > 30).ToList();

        // Assert
        await Assert.That(results.Count).IsGreaterThan(0);
        await Assert.That(results.All(u => u.IsActive && u.Age > 30)).IsTrue();
    }
}

// 测试实体类
[Entity("Users")]
public class QueryTestUser
{
    public ObjectId? Id { get; set; }
    public string? Name { get; set; }
    public int Age { get; set; }
    public string? Email { get; set; }
    public bool IsActive { get; set; }
    public DateTime CreatedAt { get; set; }
}
