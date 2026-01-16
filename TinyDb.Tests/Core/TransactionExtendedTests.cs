using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TransactionExtendedTests : IDisposable
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;

    public TransactionExtendedTests()
    {
        _testFile = Path.GetTempFileName();
        _engine = new TinyDbEngine(_testFile);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_testFile))
        {
            File.Delete(_testFile);
        }
    }

    [Test]
    public async Task Savepoints_Should_Allow_Partial_Rollback()
    {
        var collection = _engine.GetCollection<UserWithIntId>();
        using var transaction = _engine.BeginTransaction();

        // Op 1
        collection.Insert(new UserWithIntId { Id = 1, Name = "User1" });

        // Savepoint 1
        var sp1 = transaction.CreateSavepoint("SP1");

        // Op 2
        collection.Insert(new UserWithIntId { Id = 2, Name = "User2" });

        // Rollback to SP1 (undoes Op 2)
        transaction.RollbackToSavepoint(sp1);

        // Op 3
        collection.Insert(new UserWithIntId { Id = 3, Name = "User3" });

        // Commit
        transaction.Commit();

        // Assert
        var users = collection.FindAll().OrderBy(u => u.Id).ToList();
        await Assert.That(users.Count).IsEqualTo(2);
        await Assert.That(users[0].Name).IsEqualTo("User1");
        await Assert.That(users[1].Name).IsEqualTo("User3");
    }

    [Test]
    public async Task Duplicate_Insert_In_Transaction_Should_Fail_Validation_On_Commit()
    {
        var collection = _engine.GetCollection<UserWithIntId>();
        using var transaction = _engine.BeginTransaction();

        // Insert same ID twice in transaction
        var user1 = new UserWithIntId { Id = 10, Name = "User1" };
        collection.Insert(user1);
        
        var user2 = new UserWithIntId { Id = 10, Name = "User1_Duplicate" };
        collection.Insert(user2);

        // Commit should fail due to duplicate ID validation
        await Assert.That(() => transaction.Commit()).Throws<InvalidOperationException>()
            .WithMessage("Failed to commit transaction: Duplicate document IDs detected in transaction");
    }

    [Test]
    public async Task Commit_Already_Committed_Transaction_Should_Throw()
    {
        using var transaction = _engine.BeginTransaction();
        transaction.Commit();

        await Assert.That(() => transaction.Commit()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Rollback_Already_RolledBack_Transaction_Should_Throw()
    {
        using var transaction = _engine.BeginTransaction();
        transaction.Rollback();

        await Assert.That(() => transaction.Rollback()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Savepoint_Operations_On_Inactive_Transaction_Should_Throw()
    {
        using var transaction = _engine.BeginTransaction();
        var sp = transaction.CreateSavepoint("SP1");
        transaction.Commit();

        await Assert.That(() => transaction.CreateSavepoint("SP2")).Throws<InvalidOperationException>();
        await Assert.That(() => transaction.RollbackToSavepoint(sp)).Throws<InvalidOperationException>();
        await Assert.That(() => transaction.ReleaseSavepoint(sp)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Transaction_Index_Operations_Should_Be_Handled_Gracefully()
    {
        using var trans = (Transaction)_engine.BeginTransaction();
        
        // Get TransactionManager via reflection
        var managerField = typeof(TinyDbEngine).GetField("_transactionManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var manager = (TransactionManager)managerField!.GetValue(_engine)!;
        
        // Create Index Op
        var opCreate = new TransactionOperation(TransactionOperationType.CreateIndex, "Users");
        manager.RecordOperation(trans, opCreate);
        
        // Commit (hits ApplySingleOperation -> CreateIndex break)
        trans.Commit();
        
        // Rollback Index Op
        using var trans2 = (Transaction)_engine.BeginTransaction();
        var opDrop = new TransactionOperation(TransactionOperationType.DropIndex, "Users");
        manager.RecordOperation(trans2, opDrop);
        trans2.Rollback(); // hits RollbackSingleOperation -> DropIndex break
    }
}
