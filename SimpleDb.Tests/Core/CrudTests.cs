using System.Diagnostics.CodeAnalysis;
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Tests.TestEntities;
using SimpleDb.Bson;
using Xunit;

namespace SimpleDb.Tests.Core;

public class CrudTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly SimpleDbEngine _engine;

    public CrudTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"crud_test_{Guid.NewGuid():N}.db");
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        var options = new SimpleDbOptions
        {
            DatabaseName = "CrudTestDb",
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
    public void Insert_Should_Create_User_With_ValidId()
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
        Assert.NotNull(id);
        Assert.False(id.IsNull);
        Assert.NotEqual(ObjectId.Empty, user.Id);
    }

    [Fact]
    public void InsertMultiple_Should_Create_All_Users()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");
        var testUsers = new[]
        {
            new User { Name = "User 1", Age = 25, Email = "user1@example.com" },
            new User { Name = "User 2", Age = 30, Email = "user2@example.com" },
            new User { Name = "User 3", Age = 35, Email = "user3@example.com" }
        };

        // Act
        var count = users.Insert(testUsers);

        // Assert
        Assert.Equal(3, count);
        foreach (var user in testUsers)
        {
            Assert.NotEqual(ObjectId.Empty, user.Id);
        }
    }

    [Fact]
    public void FindById_Should_Return_Correct_User()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");
        var user = new User { Name = "Find Test", Age = 28, Email = "find@example.com" };
        var id = users.Insert(user);

        // Act
        var foundUser = users.FindById(id);

        // Assert
        Assert.NotNull(foundUser);
        Assert.Equal("Find Test", foundUser.Name);
        Assert.Equal(28, foundUser.Age);
        Assert.Equal("find@example.com", foundUser.Email);
    }

    [Fact]
    public void FindAll_Should_Return_All_Users()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");
        users.Insert(new User { Name = "User 1", Age = 25, Email = "user1@example.com" });
        users.Insert(new User { Name = "User 2", Age = 30, Email = "user2@example.com" });

        // Act
        var allUsers = users.FindAll().ToList();

        // Assert
        Assert.Equal(2, allUsers.Count);
    }

    [Fact]
    public void Update_Should_Modify_User_Data()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");
        var user = new User { Name = "Update Test", Age = 25, Email = "update@example.com" };
        users.Insert(user);

        user.Age = 26;
        user.Email = "updated@example.com";

        // Act
        var updateCount = users.Update(user);

        // Assert
        Assert.Equal(1, updateCount);

        var updatedUser = users.FindById(user.Id);
        Assert.NotNull(updatedUser);
        Assert.Equal(26, updatedUser.Age);
        Assert.Equal("updated@example.com", updatedUser.Email);
    }

    [Fact]
    public void Delete_Should_Remove_User()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");
        var user = new User { Name = "Delete Test", Age = 25, Email = "delete@example.com" };
        var id = users.Insert(user);

        // Act
        var deleteCount = users.Delete(id);

        // Assert
        Assert.Equal(1, deleteCount);

        var deletedUser = users.FindById(id);
        Assert.Null(deletedUser);
    }

    [Fact]
    public void Count_Should_Return_Correct_Number()
    {
        // Arrange
        var users = _engine.GetCollection<User>("users");
        users.Insert(new User { Name = "User 1", Age = 25, Email = "user1@example.com" });
        users.Insert(new User { Name = "User 2", Age = 30, Email = "user2@example.com" });

        // Act
        var count = users.Count();

        // Assert
        Assert.Equal(2, count);
    }
}