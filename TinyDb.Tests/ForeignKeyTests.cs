using System;
using System.Threading.Tasks;
using TinyDb.Attributes;
using TinyDb.Core;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests;

public class ForeignKeyTests : IDisposable
{
    private string _testDbPath = string.Empty;

    [Test]
    public async Task ForeignKey_Validation_ShouldFail_WhenReferencedDocumentDoesNotExist()
    {
        // Arrange
        _testDbPath = $"test_fk_fail_{Guid.NewGuid():N}.db";
        using var engine = new TinyDbEngine(_testDbPath);
        var metadataManager = new MetadataManager(engine);

        // Save Metadata to enable FK validation
        metadataManager.SaveEntityMetadata(typeof(ForeignKeyUser));
        metadataManager.SaveEntityMetadata(typeof(ForeignKeyOrder));

        var orders = engine.GetCollection<ForeignKeyOrder>("Orders");

        // Act
        using var transaction = engine.BeginTransaction();

        var order = new ForeignKeyOrder { UserId = "non-existent-user-id" };
        orders.Insert(order);

        // Assert
        try
        {
            transaction.Commit();
            Assert.Fail("Should have thrown InvalidOperationException");
        }
        catch (InvalidOperationException ex)
        {
            await Assert.That(ex.Message).Contains("Foreign key constraint violation");
        }
        catch (Exception ex)
        {
            Assert.Fail($"Wrong exception type: {ex.GetType().Name}, Message: {ex.Message}");
        }
    }

    [Test]
    public async Task ForeignKey_Validation_ShouldSucceed_WhenReferencedDocumentExists()
    {
         // Arrange
        _testDbPath = $"test_fk_success_{Guid.NewGuid():N}.db";
        using var engine = new TinyDbEngine(_testDbPath);
        var metadataManager = new MetadataManager(engine);

        metadataManager.SaveEntityMetadata(typeof(ForeignKeyUser));
        metadataManager.SaveEntityMetadata(typeof(ForeignKeyOrder));

        var users = engine.GetCollection<ForeignKeyUser>("Users");
        var orders = engine.GetCollection<ForeignKeyOrder>("Orders");

        // Act
        var user = new ForeignKeyUser { Name = "John" };
        users.Insert(user); // Insert user first so it exists

        using var transaction = engine.BeginTransaction();

        var order = new ForeignKeyOrder { UserId = user.Id };
        orders.Insert(order);

        // Assert
        transaction.Commit(); // Should not throw

        var savedOrder = orders.FindById(order.Id);
        await Assert.That(savedOrder).IsNotNull();
    }

    public void Dispose()
    {
         if (System.IO.File.Exists(_testDbPath)) System.IO.File.Delete(_testDbPath);
         var walPath = _testDbPath + "-wal";
         if (System.IO.File.Exists(walPath)) System.IO.File.Delete(walPath);
         var shmPath = _testDbPath + "-shm";
         if (System.IO.File.Exists(shmPath)) System.IO.File.Delete(shmPath);
    }
}

[Entity("Users")]
public class ForeignKeyUser
{
    [Id]
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string Name { get; set; } = string.Empty;
}

[Entity("Orders")]
public class ForeignKeyOrder
{
    [Id]
    public string Id { get; set; } = Guid.NewGuid().ToString();

    [ForeignKey("Users")]
    public string UserId { get; set; } = string.Empty;
}
