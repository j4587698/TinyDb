using System;
using System.IO;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class TransactionManagerAdvancedTests
{
    private string _dbFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _dbFile = Path.Combine(Path.GetTempPath(), $"tm_adv_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbFile);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_dbFile)) File.Delete(_dbFile);
    }

    [Test]
    public async Task Transaction_Rollback_Complex_ShouldWork()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        col.Insert(new UserWithIntId { Id = 1, Name = "Original" });
        
        using (var trans = _engine.BeginTransaction())
        {
            col.Update(new UserWithIntId { Id = 1, Name = "Modified" });
            col.Insert(new UserWithIntId { Id = 2, Name = "New" });
            col.Delete(1);
            
            trans.Rollback();
        }
        
        var user = col.FindById(1);
        await Assert.That(user).IsNotNull();
        await Assert.That(user!.Name).IsEqualTo("Original");
        await Assert.That(col.Count()).IsEqualTo(1);
    }

    [Test]
    public async Task TransactionManager_Dispose_ShouldRollbackActive()
    {
        var dbFile2 = Path.GetTempFileName();
        try 
        {
            using (var engine2 = new TinyDbEngine(dbFile2))
            {
                var col = engine2.GetCollection<UserWithIntId>();
                var trans = engine2.BeginTransaction();
                col.Insert(new UserWithIntId { Id = 1, Name = "U1" });
                // engine2.Dispose() called here by using
            }
            
            using (var engine3 = new TinyDbEngine(dbFile2))
            {
                var col = engine3.GetCollection<UserWithIntId>();
                await Assert.That(col.Count()).IsEqualTo(0);
            }
        }
        finally
        {
            if (File.Exists(dbFile2)) File.Delete(dbFile2);
        }
    }
}
