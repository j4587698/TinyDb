using System.Diagnostics.CodeAnalysis;
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
}