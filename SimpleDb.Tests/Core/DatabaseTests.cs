using System.Diagnostics.CodeAnalysis;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Tests.TestEntities;
using Xunit;

namespace SimpleDb.Tests.Core;

public class DatabaseTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly SimpleDbEngine _engine;

    public DatabaseTests()
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

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    [Fact]
    public void Database_Should_Create_Successfully()
    {
        // Arrange & Act
        var users = _engine.GetCollection<User>("users");

        // Assert
        Assert.NotNull(users);
        Assert.Equal("users", users.CollectionName);
    }

    [Fact]
    public void Database_Should_Return_CorrectStatistics()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");

        // Act
        var stats = _engine.GetStatistics();

        // Assert
        Assert.NotNull(stats);
        Assert.Equal("TestDb", stats.DatabaseName);
    }

    [Fact]
    public void Database_Should_Handle_Multiple_Collections()
    {
        // Arrange & Act
        var users = _engine.GetCollection<User>("users");
        var products = _engine.GetCollection<Product>("products");
        var orders = _engine.GetCollection<Order>("orders");

        // Assert
        Assert.NotNull(users);
        Assert.NotNull(products);
        Assert.NotNull(orders);
        Assert.Equal("users", users.CollectionName);
        Assert.Equal("products", products.CollectionName);
        Assert.Equal("orders", orders.CollectionName);
    }
}