using System.Diagnostics.CodeAnalysis;
using System.Linq;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Tests.TestEntities;
using SimpleDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Core;

public class SimpleDatabaseTests
{
    private string _testDbPath = null!;
    private SimpleDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_db_{Guid.NewGuid():N}.db");
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        var options = new SimpleDbOptions
        {
            DatabaseName = "TestDb",
            PageSize = 4096,
            CacheSize = 100,
            EnableJournaling = true
        };

        _engine = new SimpleDbEngine(_testDbPath, options);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    [Test]
    public async Task Database_Should_Create_Successfully()
    {
        // Arrange & Act
        var users = _engine.GetCollection<User>("users");

        // Assert
        await Assert.That(users).IsNotNull();
        await Assert.That(users.CollectionName).IsEqualTo("users");
    }

    [Test]
    public async Task Database_Should_Handle_Multiple_Collections()
    {
        // Arrange & Act
        var users = _engine.GetCollection<User>("users");
        var products = _engine.GetCollection<Product>("products");

        // Assert
        await Assert.That(users).IsNotNull();
        await Assert.That(products).IsNotNull();
        await Assert.That(users.CollectionName).IsEqualTo("users");
        await Assert.That(products.CollectionName).IsEqualTo("products");
    }

    [Test]
    public async Task Insert_Should_Create_User_With_ValidId()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");
        var user = new User
        {
            Name = "Test User",
            Age = 25,
            Email = "test@example.com"
        };

        // Act
        var id = users.Insert(user);

        // Assert
        await Assert.That(id).IsNotNull();
        await Assert.That(id.IsNull).IsFalse();
        await Assert.That(user.Id).IsNotEqualTo(ObjectId.Empty);
    }

    [Test]
    public async Task GetCollection_Should_Return_Same_Instance_For_Same_Name()
    {
        var first = _engine.GetCollection<User>("users");
        var second = _engine.GetCollection<User>("users");

        await Assert.That(ReferenceEquals(first, second)).IsTrue();
        await Assert.That(_engine.CollectionCount).IsEqualTo(1);
    }

    [Test]
    public async Task DropCollection_Should_Remove_Metadata_From_Current_Session()
    {
        _engine.GetCollection<User>("users");
        _engine.GetCollection<Product>("products");

        var dropResult = _engine.DropCollection("users");

        await Assert.That(dropResult).IsTrue();
        await Assert.That(_engine.CollectionExists("users")).IsFalse();
        await Assert.That(_engine.GetCollectionNames().Any(n => n == "users")).IsFalse();
        await Assert.That(_engine.CollectionExists("products")).IsTrue();
    }

    [Test]
    public async Task DropCollection_Should_Return_False_For_Nonexistent_Collection()
    {
        var result = _engine.DropCollection("missing");

        await Assert.That(result).IsFalse();
    }
}
