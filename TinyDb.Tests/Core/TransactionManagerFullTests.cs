using System;
using System.Collections.Generic;
using System.IO;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class TransactionManagerFullTests
{
    private string _dbFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _dbFile = Path.Combine(Path.GetTempPath(), $"tm_full_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbFile);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_dbFile)) File.Delete(_dbFile);
    }

    [Test]
    public async Task BeginTransaction_Should_RespectMaxTransactions()
    {
        var options = new TinyDbOptions { MaxTransactions = 2 };
        using var engine = new TinyDbEngine(Path.GetTempFileName(), options);
        
        var t1 = engine.BeginTransaction();
        var t2 = engine.BeginTransaction();
        
        await Assert.That(() => engine.BeginTransaction()).Throws<InvalidOperationException>();
        
        t1.Commit();
        var t3 = engine.BeginTransaction(); // Should work now
        await Assert.That(t3).IsNotNull();
    }

    [Test]
    public async Task TransactionManager_GetStatistics_ShouldWork()
    {
        var tm = new TransactionManager(_engine);
        using var t1 = tm.BeginTransaction();
        
        var stats = tm.GetStatistics();
        await Assert.That(stats.ActiveTransactionCount).IsEqualTo(1);
        await Assert.That(stats.ToString()).Contains("1/100 active");
    }

    [Test]
    public async Task Transaction_Dispose_ShouldAutoRollback()
    {
        var col = _engine.GetCollection<UserWithIntId>();
        using (var trans = _engine.BeginTransaction())
        {
            col.Insert(new UserWithIntId { Id = 1, Name = "U1" });
            // Dispose without commit
        }
        
        await Assert.That(col.Count()).IsEqualTo(0);
    }
}
