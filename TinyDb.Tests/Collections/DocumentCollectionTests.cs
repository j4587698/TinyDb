using System.Diagnostics.CodeAnalysis;
using TinyDb.Collections;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Collections;

public class DocumentCollectionTests
{
    private string _testDbPath = null!;
    private TinyDbEngine _engine = null!;
    private ILiteCollection<User> _usersCollection = null!;

    [Before(Test)]
    public void Setup()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"test_collection_db_{Guid.NewGuid():N}.db");
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);

        var options = new TinyDbOptions
        {
            DatabaseName = "TestCollectionDb",
            PageSize = 4096,
            CacheSize = 100,
            EnableJournaling = true
        };

        _engine = new TinyDbEngine(_testDbPath, options);
        _usersCollection = _engine.GetCollection<User>("users");
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_testDbPath))
            File.Delete(_testDbPath);
    }

    [Test]
    public async Task DocumentCollection_Should_Have_Correct_Name()
    {
        // Act & Assert
        await Assert.That(_usersCollection.CollectionName).IsEqualTo("users");
    }

    [Test]
    public async Task Insert_Should_Add_Single_Document_With_ValidId()
    {
        // Arrange
        var user = new User
        {
            Name = "John Doe",
            Age = 30,
            Email = "john@example.com"
        };
        var originalId = user.Id;

        // Act
        var insertedId = _usersCollection.Insert(user);

        // Assert
        await Assert.That(insertedId).IsNotNull();
        await Assert.That(user.Id.ToString()).IsNotEqualTo(ObjectId.Empty.ToString());
        // Note: The ID assignment behavior may depend on the implementation
        await Assert.That(insertedId.ToString()).IsEqualTo(user.Id.ToString());
    }

    [Test]
    public async Task Insert_Should_Add_Multiple_Documents()
    {
        // Arrange
        var users = new List<User>
        {
            new User { Name = "Alice", Age = 25, Email = "alice@example.com" },
            new User { Name = "Bob", Age = 35, Email = "bob@example.com" },
            new User { Name = "Charlie", Age = 28, Email = "charlie@example.com" }
        };

        // Act
        var insertCount = _usersCollection.Insert(users);

        // Assert
        await Assert.That(insertCount).IsEqualTo(3);
        await Assert.That(users.All(u => u.Id.ToString() != ObjectId.Empty.ToString())).IsTrue();
        await Assert.That(users.DistinctBy(u => u.Id.ToString()).Count()).IsEqualTo(3);
    }

    [Test]
    public async Task FindById_Should_Return_Correct_Document()
    {
        // Arrange
        var user = new User
        {
            Name = "Test User",
            Age = 25,
            Email = "test@example.com"
        };
        var insertedId = _usersCollection.Insert(user);

        // Act
        var foundUser = _usersCollection.FindById(insertedId);

        // Assert
        await Assert.That(foundUser).IsNotNull();
        await Assert.That(foundUser!.Id.ToString()).IsEqualTo(insertedId.ToString());
        await Assert.That(foundUser.Name).IsEqualTo("Test User");
        await Assert.That(foundUser.Age).IsEqualTo(25);
        await Assert.That(foundUser.Email).IsEqualTo("test@example.com");
    }

    [Test]
    public async Task FindById_Should_Return_Null_For_NonExistent_Id()
    {
        // Arrange
        var nonExistentId = ObjectId.NewObjectId();

        // Act
        var foundUser = _usersCollection.FindById(nonExistentId);

        // Assert
        await Assert.That(foundUser).IsNull();
    }

    [Test]
    public async Task FindAll_Should_Return_All_Documents()
    {
        // Arrange
        var users = new List<User>
        {
            new User { Name = "User1", Age = 20, Email = "user1@example.com" },
            new User { Name = "User2", Age = 30, Email = "user2@example.com" },
            new User { Name = "User3", Age = 40, Email = "user3@example.com" }
        };
        _usersCollection.Insert(users);

        // Act
        var allUsers = _usersCollection.FindAll().ToList();

        // Assert
        await Assert.That(allUsers.Count).IsEqualTo(3);
        await Assert.That(allUsers.All(u => !string.IsNullOrEmpty(u.Name))).IsTrue();
    }

    [Test]
    public async Task Find_With_Predicate_Should_Return_Matching_Documents()
    {
        // Arrange
        var users = new List<User>
        {
            new User { Name = "Alice", Age = 25, Email = "alice@example.com" },
            new User { Name = "Bob", Age = 35, Email = "bob@example.com" },
            new User { Name = "Charlie", Age = 25, Email = "charlie@example.com" },
            new User { Name = "Diana", Age = 40, Email = "diana@example.com" }
        };
        _usersCollection.Insert(users);

        // Act
        var youngUsers = _usersCollection.Find(u => u.Age == 25).ToList();

        // Assert
        await Assert.That(youngUsers.Count).IsEqualTo(2);
        await Assert.That(youngUsers.All(u => u.Age == 25)).IsTrue();
        await Assert.That(youngUsers.Any(u => u.Name == "Alice")).IsTrue();
        await Assert.That(youngUsers.Any(u => u.Name == "Charlie")).IsTrue();
    }

    [Test]
    public async Task FindOne_Should_Return_First_Matching_Document()
    {
        // Arrange
        var users = new List<User>
        {
            new User { Name = "Alice", Age = 25, Email = "alice@example.com" },
            new User { Name = "Bob", Age = 35, Email = "bob@example.com" },
            new User { Name = "Charlie", Age = 25, Email = "charlie@example.com" }
        };
        _usersCollection.Insert(users);

        // Act
        var firstYoungUser = _usersCollection.FindOne(u => u.Age == 25);

        // Assert
        await Assert.That(firstYoungUser).IsNotNull();
        await Assert.That(firstYoungUser!.Age).IsEqualTo(25);
        await Assert.That(firstYoungUser.Name == "Alice" || firstYoungUser.Name == "Charlie").IsTrue();
    }

    [Test]
    public async Task FindOne_Should_Return_Null_For_No_Match()
    {
        // Arrange
        var user = new User { Name = "Test", Age = 30, Email = "test@example.com" };
        _usersCollection.Insert(user);

        // Act
        var result = _usersCollection.FindOne(u => u.Age > 100);

        // Assert
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task Update_Should_Modify_Existing_Document()
    {
        // Arrange
        var user = new User
        {
            Name = "Original Name",
            Age = 25,
            Email = "original@example.com"
        };
        var insertedId = _usersCollection.Insert(user);

        // Act
        user.Name = "Updated Name";
        user.Age = 26;
        var updateCount = _usersCollection.Update(user);

        // Assert
        await Assert.That(updateCount).IsEqualTo(1);

        var updatedUser = _usersCollection.FindById(insertedId);
        await Assert.That(updatedUser).IsNotNull();
        await Assert.That(updatedUser!.Name).IsEqualTo("Updated Name");
        await Assert.That(updatedUser.Age).IsEqualTo(26);
        await Assert.That(updatedUser.Email).IsEqualTo("original@example.com"); // Unchanged
    }

    [Test]
    public async Task Update_Should_Return_Zero_For_NonExistent_Document()
    {
        // Arrange
        var user = new User
        {
            Name = "Non-existent",
            Age = 30,
            Email = "nonexistent@example.com"
        };
        // Don't insert the user

        // Act
        var updateCount = _usersCollection.Update(user);

        // Assert
        await Assert.That(updateCount).IsEqualTo(0);
    }

    [Test]
    public async Task Delete_Should_Remove_Document_By_Id()
    {
        // Arrange
        var user = new User
        {
            Name = "To Delete",
            Age = 30,
            Email = "delete@example.com"
        };
        var insertedId = _usersCollection.Insert(user);

        // Act
        var deleteCount = _usersCollection.Delete(insertedId);

        // Assert
        await Assert.That(deleteCount).IsEqualTo(1);
        await Assert.That(_usersCollection.FindById(insertedId)).IsNull();
    }

    [Test]
    public async Task Delete_Should_Return_Zero_For_NonExistent_Id()
    {
        // Arrange
        var nonExistentId = ObjectId.NewObjectId();

        // Act
        var deleteCount = _usersCollection.Delete(nonExistentId);

        // Assert
        await Assert.That(deleteCount).IsEqualTo(0);
    }

    [Test]
    public async Task DeleteAll_Should_Remove_All_Documents()
    {
        // Arrange
        var users = new List<User>
        {
            new User { Name = "User1", Age = 20, Email = "user1@example.com" },
            new User { Name = "User2", Age = 30, Email = "user2@example.com" },
            new User { Name = "User3", Age = 40, Email = "user3@example.com" }
        };
        _usersCollection.Insert(users);

        // Act
        var deleteCount = _usersCollection.DeleteAll();

        // Assert
        await Assert.That(deleteCount).IsEqualTo(3);
        await Assert.That(_usersCollection.FindAll()).IsEmpty();
    }

    [Test]
    public async Task Count_Should_Return_Total_Document_Count()
    {
        // Arrange
        var users = new List<User>
        {
            new User { Name = "User1", Age = 20, Email = "user1@example.com" },
            new User { Name = "User2", Age = 30, Email = "user2@example.com" }
        };
        _usersCollection.Insert(users);

        // Act
        var count = _usersCollection.Count();

        // Assert
        await Assert.That(count).IsEqualTo(2);
    }

    [Test]
    public async Task Count_With_Predicate_Should_Return_Matching_Document_Count()
    {
        // Arrange
        var users = new List<User>
        {
            new User { Name = "Alice", Age = 25, Email = "alice@example.com" },
            new User { Name = "Bob", Age = 35, Email = "bob@example.com" },
            new User { Name = "Charlie", Age = 25, Email = "charlie@example.com" }
        };
        _usersCollection.Insert(users);

        // Act
        var youngUserCount = _usersCollection.Count(u => u.Age == 25);

        // Assert
        await Assert.That(youngUserCount).IsEqualTo(2);
    }

    [Test]
    public async Task Exists_Should_Return_True_For_Matching_Document()
    {
        // Arrange
        var user = new User { Name = "Test User", Age = 30, Email = "test@example.com" };
        _usersCollection.Insert(user);

        // Act
        var exists = _usersCollection.Exists(u => u.Name == "Test User");

        // Assert
        await Assert.That(exists).IsTrue();
    }

    [Test]
    public async Task Exists_Should_Return_False_For_No_Matching_Document()
    {
        // Arrange
        var user = new User { Name = "Test User", Age = 30, Email = "test@example.com" };
        _usersCollection.Insert(user);

        // Act
        var exists = _usersCollection.Exists(u => u.Name == "Non-existent User");

        // Assert
        await Assert.That(exists).IsFalse();
    }

    [Test]
    public async Task Upsert_Should_Insert_New_Document()
    {
        // Arrange
        var user = new User
        {
            Name = "New User",
            Age = 25,
            Email = "newuser@example.com"
        };
        // Use a specific ID to test upsert behavior
        var testId = ObjectId.NewObjectId();
        user.Id = testId;

        // Act
        var (updateType, count) = _usersCollection.Upsert(user);

        // Assert
        await Assert.That(updateType).IsEqualTo(UpdateType.Insert);
        await Assert.That(count).IsEqualTo(1);

        var foundUser = _usersCollection.FindById(testId);
        await Assert.That(foundUser).IsNotNull();
        await Assert.That(foundUser!.Name).IsEqualTo("New User");
    }

    [Test]
    public async Task Upsert_Should_Update_Existing_Document()
    {
        // Arrange
        var user = new User
        {
            Name = "Original Name",
            Age = 25,
            Email = "original@example.com"
        };
        var insertedId = _usersCollection.Insert(user);

        // Act
        user.Name = "Updated Name";
        user.Age = 26;
        var (updateType, count) = _usersCollection.Upsert(user);

        // Assert
        await Assert.That(updateType).IsEqualTo(UpdateType.Update);
        await Assert.That(count).IsEqualTo(1);
        await Assert.That(user.Id.ToString()).IsEqualTo(insertedId.ToString());

        var foundUser = _usersCollection.FindById(insertedId);
        await Assert.That(foundUser).IsNotNull();
        await Assert.That(foundUser!.Name).IsEqualTo("Updated Name");
        await Assert.That(foundUser.Age).IsEqualTo(26);
    }

    [Test]
    public async Task Query_Should_Return_IQueryable_For_Complex_Queries()
    {
        // Arrange
        var users = new List<User>
        {
            new User { Name = "Alice", Age = 25, Email = "alice@example.com" },
            new User { Name = "Bob", Age = 35, Email = "bob@example.com" },
            new User { Name = "Charlie", Age = 25, Email = "charlie@example.com" },
            new User { Name = "Diana", Age = 40, Email = "diana@example.com" }
        };
        _usersCollection.Insert(users);

        // Act
        var query = _usersCollection.Query();
        var youngUsers = query.Where(u => u.Age < 30).ToList();

        // Assert
        await Assert.That(youngUsers.Count).IsEqualTo(2);
        await Assert.That(youngUsers.All(u => u.Age < 30)).IsTrue();
        await Assert.That(youngUsers.Any(u => u.Name == "Alice")).IsTrue();
        await Assert.That(youngUsers.Any(u => u.Name == "Charlie")).IsTrue();
    }

    [Test]
    public async Task DeleteMany_Should_Remove_Matching_Documents()
    {
        // Arrange
        var users = new List<User>
        {
            new User { Name = "Alice", Age = 25, Email = "alice@example.com" },
            new User { Name = "Bob", Age = 35, Email = "bob@example.com" },
            new User { Name = "Charlie", Age = 25, Email = "charlie@example.com" },
            new User { Name = "Diana", Age = 40, Email = "diana@example.com" }
        };
        _usersCollection.Insert(users);

        // Act
        var deleteCount = _usersCollection.DeleteMany(u => u.Age == 25);

        // Assert
        await Assert.That(deleteCount).IsEqualTo(2);
        await Assert.That(_usersCollection.Count()).IsEqualTo(2);

        var remainingUsers = _usersCollection.FindAll().ToList();
        await Assert.That(remainingUsers.All(u => u.Age != 25)).IsTrue();
    }

    [Test]
    public async Task Collection_Should_Handle_Empty_Database()
    {
        // Act & Assert - All operations should work gracefully on empty collection
        await Assert.That(_usersCollection.Count()).IsEqualTo(0);
        await Assert.That(_usersCollection.FindAll()).IsEmpty();
        await Assert.That(_usersCollection.FindById(ObjectId.NewObjectId())).IsNull();
        await Assert.That(_usersCollection.FindOne(u => u.Age > 0)).IsNull();
        await Assert.That(_usersCollection.Exists(u => u.Name == "test")).IsFalse();
        await Assert.That(_usersCollection.DeleteAll()).IsEqualTo(0);
    }
}