using System.Diagnostics.CodeAnalysis;
using System.Linq;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Tests.TestEntities;
using SimpleDb.Bson;
using Xunit;
using UpdateType = SimpleDb.Collections.UpdateType;

namespace SimpleDb.Tests.Core;

public class QueryTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly SimpleDbEngine _engine;
    private readonly ILiteCollection<User> _users;
    private readonly ILiteCollection<Product> _products;

    public QueryTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"query_test_{Guid.NewGuid():N}.db");
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        var options = new SimpleDbOptions
        {
            DatabaseName = "QueryTestDb",
            PageSize = 4096,
            CacheSize = 100,
            EnableJournaling = true
        };

        _engine = new SimpleDbEngine(_testDbPath, options);
        _users = _engine.GetCollection<User>("users");
        _products = _engine.GetCollection<Product>("products");

        // Setup test data
        SetupTestData();
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    private void SetupTestData()
    {
        // Insert test users
        var testUsers = new[]
        {
            new User { Name = "Alice", Age = 25, Email = "alice@example.com" },
            new User { Name = "Bob", Age = 30, Email = "bob@example.com" },
            new User { Name = "Charlie", Age = 35, Email = "charlie@example.com" },
            new User { Name = "Diana", Age = 28, Email = "diana@example.com" },
            new User { Name = "Eve", Age = 22, Email = "eve@example.com" }
        };
        _users.Insert(testUsers);

        // Insert test products
        var testProducts = new[]
        {
            new Product { Name = "Laptop", Price = 999.99m, InStock = true },
            new Product { Name = "Mouse", Price = 19.99m, InStock = true },
            new Product { Name = "Keyboard", Price = 79.99m, InStock = false },
            new Product { Name = "Monitor", Price = 299.99m, InStock = true }
        };
        _products.Insert(testProducts);
    }

    [Fact]
    public void Find_With_Predicate_Should_Return_Matching_Users()
    {
        // Act
        var adults = _users.Find(u => u.Age >= 30).ToList();

        // Assert
        Assert.Equal(2, adults.Count);
        foreach (var adult in adults)
        {
            Assert.True(adult.Age >= 30);
        }
    }

    [Fact]
    public void FindOne_Should_Return_Single_Matching_User()
    {
        // Act
        var user = _users.FindOne(u => u.Name == "Alice");

        // Assert
        Assert.NotNull(user);
        Assert.Equal("Alice", user.Name);
        Assert.Equal(25, user.Age);
    }

    [Fact]
    public void Count_With_Predicate_Should_Return_Correct_Count()
    {
        // Act
        var adultCount = _users.Count(u => u.Age >= 30);

        // Assert
        Assert.Equal(2, adultCount);
    }

    [Fact]
    public void Exists_Should_Return_True_For_Matching_User()
    {
        // Act
        var exists = _users.Exists(u => u.Name == "Bob");

        // Assert
        Assert.True(exists);
    }

    [Fact]
    public void Exists_Should_Return_False_For_NonMatching_User()
    {
        // Act
        var exists = _users.Exists(u => u.Name == "NonExistent");

        // Assert
        Assert.False(exists);
    }

    [Fact]
    public void Query_Should_Support_Chaining()
    {
        // Act
        var result = _users.Query()
            .Where(u => u.Age >= 25)
            .Where(u => u.Name.Contains("a"))
            .OrderBy(u => u.Age)
            .ToList();

        // Assert
        Assert.Single(result);
        Assert.Equal("Alice", result.First().Name);
    }

    [Fact]
    public void DeleteMany_Should_Remove_Matching_Users()
    {
        // Act
        var deleteCount = _users.DeleteMany(u => u.Age < 25);

        // Assert
        Assert.Equal(1, deleteCount); // Eve (age 22)

        var remainingUsers = _users.FindAll().ToList();
        Assert.Equal(4, remainingUsers.Count);
        Assert.DoesNotContain(remainingUsers, u => u.Name == "Eve");
    }

    [Fact]
    public void Upsert_Should_Insert_New_User()
    {
        // Arrange
        var newUser = new User { Name = "Frank", Age = 40, Email = "frank@example.com" };

        // Act
        var (updateType, count) = _users.Upsert(newUser);

        // Assert
        Assert.Equal(UpdateType.Insert, updateType);
        Assert.Equal(1, count);
        Assert.NotEqual(ObjectId.Empty, newUser.Id);
    }

    [Fact]
    public void Upsert_Should_Update_Existing_User()
    {
        // Arrange
        var existingUser = _users.FindOne(u => u.Name == "Alice");
        Assert.NotNull(existingUser);
        existingUser.Age = 26;

        // Act
        var (updateType, count) = _users.Upsert(existingUser);

        // Assert
        Assert.Equal(UpdateType.Update, updateType);
        Assert.Equal(1, count);

        var updatedUser = _users.FindById(existingUser.Id);
        Assert.NotNull(updatedUser);
        Assert.Equal(26, updatedUser.Age);
    }

    [Fact]
    public void Query_Should_Work_With_Different_Collection()
    {
        // Act
        var inStockProducts = _products.Query()
            .Where(p => p.InStock)
            .OrderBy(p => p.Price)
            .ToList();

        // Assert
        Assert.Equal(3, inStockProducts.Count);
        Assert.Equal("Mouse", inStockProducts.First().Name);
        Assert.Equal(19.99m, inStockProducts.First().Price);
    }
}