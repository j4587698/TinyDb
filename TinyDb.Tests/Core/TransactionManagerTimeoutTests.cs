using System;
using System.IO;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

/// <summary>
/// 事务管理器超时和清理逻辑测试
/// </summary>
public class TransactionManagerTimeoutTests
{
    private string _databasePath = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"txn_timeout_{Guid.NewGuid():N}.db");
        var options = new TinyDbOptions
        {
            DatabaseName = "TransactionTimeoutTestDb",
            PageSize = 4096,
            CacheSize = 100,
            MaxTransactions = 10,
            TransactionTimeout = TimeSpan.FromMilliseconds(100)
        };

        _engine = new TinyDbEngine(_databasePath, options);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_databasePath))
        {
            try { File.Delete(_databasePath); } catch { }
        }
    }

    [Test]
    public async Task TransactionManager_WithVeryShortTimeout_ShouldInitialize()
    {
        using var manager = new TransactionManager(_engine, 5, TimeSpan.FromMilliseconds(50));
        await Assert.That(manager.TransactionTimeout).IsEqualTo(TimeSpan.FromMilliseconds(50));
        await Assert.That(manager.MaxTransactions).IsEqualTo(5);
    }

    [Test]
    public async Task TransactionManager_DefaultTimeout_ShouldBe5Minutes()
    {
        using var manager = new TransactionManager(_engine);
        await Assert.That(manager.TransactionTimeout).IsEqualTo(TimeSpan.FromMinutes(5));
    }

    [Test]
    public async Task TransactionManager_NullEngine_ShouldThrow()
    {
        await Assert.That(() => new TransactionManager(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task TransactionManager_ToString_ShouldReturnInfo()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        var str = manager.ToString();
        await Assert.That(str).Contains("TransactionManager");
        await Assert.That(str).Contains("0/10");
    }

    [Test]
    public async Task GetStatistics_WithNoTransactions_ShouldReturnEmptyStats()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        var stats = manager.GetStatistics();
        
        await Assert.That(stats.ActiveTransactionCount).IsEqualTo(0);
        await Assert.That(stats.AverageOperationCount).IsEqualTo(0);
        await Assert.That(stats.TotalOperations).IsEqualTo(0);
        await Assert.That(stats.AverageTransactionAge).IsEqualTo(0);
    }

    [Test]
    public async Task GetStatistics_WithTransactions_ShouldReturnCorrectStats()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        using var txn1 = manager.BeginTransaction();
        using var txn2 = manager.BeginTransaction();
        
        var stats = manager.GetStatistics();
        
        await Assert.That(stats.ActiveTransactionCount).IsEqualTo(2);
        await Assert.That(stats.States).ContainsKey(TransactionState.Active);
        await Assert.That(stats.ToString()).Contains("TransactionManager");
    }

    [Test]
    public async Task CommitTransaction_WithNullTransaction_ShouldThrow()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        
        // 使用反射调用内部方法
        var method = typeof(TransactionManager).GetMethod("CommitTransaction", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        await Assert.That(() => method!.Invoke(manager, new object?[] { null }))
            .Throws<System.Reflection.TargetInvocationException>();
    }

    [Test]
    public async Task RollbackTransaction_WithNullTransaction_ShouldThrow()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        
        var method = typeof(TransactionManager).GetMethod("RollbackTransaction", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        await Assert.That(() => method!.Invoke(manager, new object?[] { null }))
            .Throws<System.Reflection.TargetInvocationException>();
    }

    [Test]
    public async Task CreateSavepoint_WithNullTransaction_ShouldThrow()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        
        var method = typeof(TransactionManager).GetMethod("CreateSavepoint", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        await Assert.That(() => method!.Invoke(manager, new object?[] { null, "test" }))
            .Throws<System.Reflection.TargetInvocationException>();
    }

    [Test]
    public async Task CreateSavepoint_WithEmptyName_ShouldThrow()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        using var txn = manager.BeginTransaction();
        
        await Assert.That(() => txn.CreateSavepoint("")).Throws<ArgumentException>();
        await Assert.That(() => txn.CreateSavepoint(null!)).Throws<ArgumentException>();
    }

    [Test]
    public async Task RollbackToSavepoint_WithInvalidSavepointId_ShouldThrow()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        using var txn = manager.BeginTransaction();
        
        await Assert.That(() => txn.RollbackToSavepoint(Guid.NewGuid()))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task RecordOperation_WithNullTransaction_ShouldThrow()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        
        var method = typeof(TransactionManager).GetMethod("RecordOperation", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        var op = new TransactionOperation(TransactionOperationType.Insert, "test", new BsonInt32(1));
        await Assert.That(() => method!.Invoke(manager, new object?[] { null, op }))
            .Throws<System.Reflection.TargetInvocationException>();
    }

    [Test]
    public async Task RecordOperation_WithNullOperation_ShouldThrow()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        using var txn = manager.BeginTransaction();
        
        var method = typeof(TransactionManager).GetMethod("RecordOperation", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        await Assert.That(() => method!.Invoke(manager, new object?[] { txn, null }))
            .Throws<System.Reflection.TargetInvocationException>();
    }

    [Test]
    public async Task Dispose_WithActiveTransactions_ShouldCleanup()
    {
        var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        var txn1 = manager.BeginTransaction();
        var txn2 = manager.BeginTransaction();
        
        await Assert.That(manager.ActiveTransactionCount).IsEqualTo(2);
        
        manager.Dispose();
        
        await Assert.That(manager.ActiveTransactionCount).IsEqualTo(0);
    }

    [Test]
    public async Task Dispose_CalledTwice_ShouldNotThrow()
    {
        var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        manager.Dispose();
        manager.Dispose(); // 不应该抛出异常
        await Assert.That(true).IsTrue();
    }

    [Test]
    public async Task BeginTransaction_AfterDispose_ShouldThrow()
    {
        var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        manager.Dispose();
        
        await Assert.That(() => manager.BeginTransaction())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task GetStatistics_AfterDispose_ShouldThrow()
    {
        var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        manager.Dispose();
        
        await Assert.That(() => manager.GetStatistics())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task TransactionManagerStatistics_ToString_ShouldReturnInfo()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        using var txn = manager.BeginTransaction();
        
        var stats = manager.GetStatistics();
        var str = stats.ToString();
        
        await Assert.That(str).Contains("TransactionManager");
        await Assert.That(str).Contains("1/10");
    }

    [Test]
    public async Task RollbackToSavepoint_WithNullTransaction_ShouldThrow()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        
        var method = typeof(TransactionManager).GetMethod("RollbackToSavepoint", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        await Assert.That(() => method!.Invoke(manager, new object?[] { null, Guid.NewGuid() }))
            .Throws<System.Reflection.TargetInvocationException>();
    }

    [Test]
    public async Task ReleaseSavepoint_WithNullTransaction_ShouldThrow()
    {
        using var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(5));
        
        var method = typeof(TransactionManager).GetMethod("ReleaseSavepoint", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        
        await Assert.That(() => method!.Invoke(manager, new object?[] { null, Guid.NewGuid() }))
            .Throws<System.Reflection.TargetInvocationException>();
    }
}
