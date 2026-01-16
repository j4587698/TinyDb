using System;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

public class TransactionAdvancedTests
{
    [Test]
    public async Task TransactionOperation_Clone_ShouldWork()
    {
        var op = new TransactionOperation(
            TransactionOperationType.Insert,
            "col",
            1,
            null,
            new BsonDocument().Set("a", 1)
        );
        
        var clone = op.Clone();
        await Assert.That(clone.OperationId).IsNotEqualTo(op.OperationId);
        await Assert.That(clone.OperationType).IsEqualTo(op.OperationType);
        await Assert.That(clone.CollectionName).IsEqualTo(op.CollectionName);
        await Assert.That(clone.DocumentId!.ToInt32(null)).IsEqualTo(1);
        await Assert.That(clone.ToString()).Contains("Insert");
    }

    [Test]
    public async Task TransactionSavepoint_ToString_ShouldWork()
    {
        var sp = new TransactionSavepoint("test", 5);
        await Assert.That(sp.Name).IsEqualTo("test");
        await Assert.That(sp.OperationCount).IsEqualTo(5);
        await Assert.That(sp.ToString()).Contains("test");
        await Assert.That(sp.ToString()).Contains("5 operations");
    }

    [Test]
    public async Task Transaction_Savepoints_ShouldWork()
    {
        var dbFile = Path.Combine(Path.GetTempPath(), $"trans_sp_{Guid.NewGuid():N}.db");
        try 
        {
            using var engine = new TinyDbEngine(dbFile);
            var col = engine.GetCollection<UserWithIntId>();
            
            using var trans = engine.BeginTransaction();
            col.Insert(new UserWithIntId { Id = 1, Name = "U1" });
            
            var sp1 = trans.CreateSavepoint("sp1");
            col.Insert(new UserWithIntId { Id = 2, Name = "U2" });
            
            await Assert.That(col.Count()).IsEqualTo(2);
            
            trans.RollbackToSavepoint(sp1);
            await Assert.That(col.Count()).IsEqualTo(1);
            
            col.Insert(new UserWithIntId { Id = 3, Name = "U3" });
            trans.Commit();
            
            await Assert.That(col.Count()).IsEqualTo(2); // 1 and 3
            await Assert.That(col.FindById(1)).IsNotNull();
            await Assert.That(col.FindById(2)).IsNull();
            await Assert.That(col.FindById(3)).IsNotNull();
        }
        finally
        {
            if (File.Exists(dbFile)) File.Delete(dbFile);
        }
    }

    [Test]
    public async Task Transaction_Savepoints_EdgeCases_ShouldWork()
    {
        using var engine = new TinyDbEngine(Path.Combine(Path.GetTempPath(), $"trans_edge_{Guid.NewGuid():N}.db"));
        using var trans = engine.BeginTransaction();
        
        await Assert.That(() => trans.RollbackToSavepoint(Guid.NewGuid())).Throws<ArgumentException>();
        
        // ReleaseSavepoint ignores non-existent savepoints
        trans.ReleaseSavepoint(Guid.NewGuid());
        
        var sp = trans.CreateSavepoint("s1");
        trans.ReleaseSavepoint(sp);
        // Re-releasing or rolling back to released should fail
        await Assert.That(() => trans.RollbackToSavepoint(sp)).Throws<ArgumentException>();
    }

    [Test]
    public async Task Transaction_State_Validations_ShouldWork()
    {
        using var engine = new TinyDbEngine(Path.GetTempFileName());
        var trans = engine.BeginTransaction();
        trans.Commit();
        
        await Assert.That(() => trans.Commit()).Throws<InvalidOperationException>();
        await Assert.That(() => trans.Rollback()).Throws<InvalidOperationException>();
        await Assert.That(() => trans.CreateSavepoint("s")).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task TransactionStatistics_ShouldWork()
    {
        var stats = new TransactionStatistics
        {
            TransactionId = Guid.NewGuid(),
            State = TransactionState.Active,
            StartTime = DateTime.UtcNow,
            Duration = TimeSpan.FromSeconds(1),
            OperationCount = 10,
            SavepointCount = 2
        };
        
        await Assert.That(stats.OperationCount).IsEqualTo(10);
        await Assert.That(stats.State).IsEqualTo(TransactionState.Active);
    }
}
