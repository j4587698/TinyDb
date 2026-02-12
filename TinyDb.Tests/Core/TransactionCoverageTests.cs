using TinyDb.Core;
using TinyDb.Bson;
using System.Reflection;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TransactionCoverageTests : IDisposable
{
    private readonly List<string> _dbFiles = new();

    private TinyDbEngine CreateTestEngine()
    {
        var dbPath = $"test_transaction_coverage_{Guid.NewGuid():N}.db";
        _dbFiles.Add(dbPath);
        return new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
    }

    public void Dispose()
    {
        foreach (var file in _dbFiles)
        {
            if (File.Exists(file))
            {
                try { File.Delete(file); } catch { }
            }
            if (File.Exists(file + ".wal"))
            {
                try { File.Delete(file + ".wal"); } catch { }
            }
        }
    }

    [Test]
    public async Task Commit_InvalidState_ShouldThrow()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();
        
        trans.Commit(); // State becomes Committed
        
        await Assert.That(() => trans.Commit()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Transaction_Ctor_WithNullManager_ShouldThrow()
    {
        await Assert.That(() => new Transaction(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Rollback_InvalidState_ShouldThrow()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();
        
        trans.Commit(); // State becomes Committed
        
        await Assert.That(() => trans.Rollback()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Rollback_WhenStateIsFailed_ShouldResetAndRollback()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();

        var t = (Transaction)trans;
        t.State = TransactionState.Failed;

        trans.Rollback();

        await Assert.That(trans.State).IsEqualTo(TransactionState.RolledBack);
    }

    [Test]
    public async Task CreateSavepoint_InvalidArguments_ShouldThrow()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();
        
        await Assert.That(() => trans.CreateSavepoint(null!)).Throws<ArgumentException>();
        await Assert.That(() => trans.CreateSavepoint("")).Throws<ArgumentException>();
    }

    [Test]
    public async Task CreateSavepoint_InvalidState_ShouldThrow()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();
        trans.Commit();
        
        await Assert.That(() => trans.CreateSavepoint("sp1")).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task RollbackToSavepoint_InvalidState_ShouldThrow()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();
        var sp = trans.CreateSavepoint("sp1");
        trans.Commit();
        
        await Assert.That(() => trans.RollbackToSavepoint(sp)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ReleaseSavepoint_InvalidState_ShouldThrow()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();
        var sp = trans.CreateSavepoint("sp1");
        trans.Commit();
        
        await Assert.That(() => trans.ReleaseSavepoint(sp)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task RecordOperations_InvalidState_ShouldThrow()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();
        trans.Commit();
        
        var t = (Transaction)trans;
        
        await Assert.That(() => t.RecordInsert("col", new BsonDocument())).Throws<InvalidOperationException>();
        await Assert.That(() => t.RecordUpdate("col", new BsonDocument(), new BsonDocument())).Throws<InvalidOperationException>();
        await Assert.That(() => t.RecordDelete("col", new BsonDocument())).Throws<InvalidOperationException>();
        await Assert.That(() => t.RecordCreateIndex("col", "idx", new string[]{"a"}, false)).Throws<InvalidOperationException>();
        await Assert.That(() => t.RecordDropIndex("col", "idx")).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Dispose_ShouldAutoRollback_IfActive()
    {
        using var engine = CreateTestEngine();
        var col = engine.GetBsonCollection("users");
        
        var trans = engine.BeginTransaction();
        col.Insert(new BsonDocument().Set("_id", 1).Set("name", "active"));
        
        // Don't commit, just dispose
        trans.Dispose();
        
        // Verify rollback
        var count = col.Count();
        await Assert.That(count).IsEqualTo(0);
        await Assert.That(trans.State).IsEqualTo(TransactionState.RolledBack);
    }

    [Test]
    public async Task Dispose_UntrackedTransaction_ShouldSwallowRollbackErrors()
    {
        using var engine = CreateTestEngine();

        var managerField = typeof(TinyDbEngine).GetField("_transactionManager", BindingFlags.Instance | BindingFlags.NonPublic);
        await Assert.That(managerField).IsNotNull();

        var manager = (TransactionManager)managerField!.GetValue(engine)!;
        var untracked = new Transaction(manager);

        await Assert.That(() => untracked.Dispose()).ThrowsNothing();
        await Assert.That(() => untracked.Commit()).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task ToString_ShouldReturnCorrectFormat()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();
        trans.CreateSavepoint("sp1");
        
        var str = trans.ToString();
        await Assert.That(str).Contains("Transaction[");
        await Assert.That(str).Contains("Active");
        await Assert.That(str).Contains("savepoints");
    }

    [Test]
    public async Task GetStatistics_ShouldReturnValidData()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();
        
        var col = engine.GetBsonCollection("users");
        col.Insert(new BsonDocument().Set("a", 1));
        trans.CreateSavepoint("sp1");
        
        var t = (Transaction)trans;
        var stats = t.GetStatistics();
        
        await Assert.That(stats.OperationCount).IsGreaterThan(0);
        await Assert.That(stats.SavepointCount).IsEqualTo(1);
        await Assert.That(stats.IsReadOnly).IsFalse();
        await Assert.That(stats.State).IsEqualTo(TransactionState.Active);
        
        var statsStr = stats.ToString();
        await Assert.That(statsStr).Contains("ops");
        await Assert.That(statsStr).Contains("read-write");
    }

    [Test]
    public async Task GetStatistics_WhenNoOperations_ShouldReportReadOnly()
    {
        using var engine = CreateTestEngine();
        using var trans = engine.BeginTransaction();

        var t = (Transaction)trans;
        var stats = t.GetStatistics();

        await Assert.That(stats.IsReadOnly).IsTrue();
        await Assert.That(stats.ToString()).Contains("read-only");
    }

    [Test]
    public async Task RecordDropIndex_ShouldAttemptToGetIndexInfo()
    {
        using var engine = CreateTestEngine();
        var col = engine.GetBsonCollection("users");
        engine.EnsureIndex("users", "name", "idx_name");
        
        using var trans = engine.BeginTransaction();
        var t = (Transaction)trans;
        
        // Manual call to internal method to verify it captures info
        t.RecordDropIndex("users", "idx_name");
        
        // Verify operation recorded
        var op = t.Operations.LastOrDefault(o => o.OperationType == TransactionOperationType.DropIndex);
        
        await Assert.That(op).IsNotNull();
        // IndexFields should be captured if IndexManager works
        // We can't access op.IndexFields directly easily as it is public property?
        // TransactionOperation properties are public.
        await Assert.That(op!.IndexName).IsEqualTo("idx_name");
        await Assert.That(op.IndexFields).IsNotNull();
        await Assert.That(op.IndexFields!.Length).IsEqualTo(1);
        await Assert.That(op.IndexFields![0]).IsEqualTo("name");
    }

    [Test]
    public async Task RecordDropIndex_WhenIndexLookupThrows_ShouldStillRecordOperation()
    {
        using var engine = CreateTestEngine();
        engine.GetBsonCollection("users");

        using var trans = engine.BeginTransaction();
        var t = (Transaction)trans;

        t.RecordDropIndex("users", null!);

        var op = t.Operations.LastOrDefault(o => o.OperationType == TransactionOperationType.DropIndex);
        await Assert.That(op).IsNotNull();
        await Assert.That(op!.IndexName).IsNull();
    }
}
