using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using SimpleDb.Bson;
using SimpleDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace SimpleDb.Tests.Core;

/// <summary>
/// 事务管理器测试
/// </summary>
public class TransactionManagerTests
{
    private string _databasePath = null!;
    private SimpleDbEngine _engine = null!;
    private TransactionManager _manager = null!;

    [Before(Test)]
    public void Setup()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"txn_{Guid.NewGuid():N}.db");
        var options = new SimpleDbOptions
        {
            DatabaseName = "TransactionTestDb",
            PageSize = 4096,
            CacheSize = 100,
            MaxTransactions = 10,
            TransactionTimeout = TimeSpan.FromSeconds(30)
        };

        _engine = new SimpleDbEngine(_databasePath, options);
        _manager = new TransactionManager(_engine, options.MaxTransactions, options.TransactionTimeout);
    }

    [After(Test)]
    public void Cleanup()
    {
        _manager?.Dispose();
        _engine?.Dispose();
        if (File.Exists(_databasePath))
        {
            File.Delete(_databasePath);
        }
    }

    [Test]
    public async Task TransactionManager_Constructor_ShouldInitializeCorrectly()
    {
        await Assert.That(_manager.ActiveTransactionCount).IsEqualTo(0);
        await Assert.That(_manager.MaxTransactions).IsEqualTo(10);
        await Assert.That(_manager.TransactionTimeout).IsEqualTo(TimeSpan.FromSeconds(30));
    }

    [Test]
    public async Task BeginTransaction_ShouldCreateNewTransaction()
    {
        using var transaction = _manager.BeginTransaction();

        await Assert.That(transaction).IsNotNull();
        await Assert.That(transaction.State).IsEqualTo(TransactionState.Active);
        await Assert.That(_manager.ActiveTransactionCount).IsEqualTo(1);
    }

    [Test]
    public async Task BeginTransaction_ShouldRespectMaxTransactionLimit()
    {
        var transactions = new List<ITransaction>();

        for (int i = 0; i < 10; i++)
        {
            transactions.Add(_manager.BeginTransaction());
        }

        await Assert.That(_manager.ActiveTransactionCount).IsEqualTo(10);
        await Assert.That(() => _manager.BeginTransaction()).Throws<InvalidOperationException>();

        foreach (var transaction in transactions)
        {
            transaction.Dispose();
        }
    }

    [Test]
    public async Task CommitTransaction_ShouldCommitSuccessfully()
    {
        using var transaction = _manager.BeginTransaction();

        transaction.Commit();

        await Assert.That(transaction.State).IsEqualTo(TransactionState.Committed);
        await Assert.That(_manager.ActiveTransactionCount).IsEqualTo(0);
    }

    [Test]
    public async Task RollbackTransaction_ShouldRollbackSuccessfully()
    {
        using var transaction = _manager.BeginTransaction();

        transaction.Rollback();

        await Assert.That(transaction.State).IsEqualTo(TransactionState.RolledBack);
        await Assert.That(_manager.ActiveTransactionCount).IsEqualTo(0);
    }

    [Test]
    public async Task TransactionSavepoint_ShouldWorkCorrectly()
    {
        using var transaction = _manager.BeginTransaction();

        var savepointId = transaction.CreateSavepoint("test_savepoint");
        await Assert.That(savepointId).IsNotEqualTo(Guid.Empty);

        transaction.RollbackToSavepoint(savepointId);
        await Assert.That(transaction.State).IsEqualTo(TransactionState.Active);

        transaction.ReleaseSavepoint(savepointId);
        await Assert.That(transaction.State).IsEqualTo(TransactionState.Active);
    }

    [Test]
    public async Task TransactionDispose_ShouldAutoRollback()
    {
        var transaction = _manager.BeginTransaction();
        await Assert.That(_manager.ActiveTransactionCount).IsEqualTo(1);

        transaction.Dispose();

        await Assert.That(_manager.ActiveTransactionCount).IsEqualTo(0);
    }

    [Test]
    public async Task GetStatistics_ShouldReturnCorrectInformation()
    {
        using var transaction1 = _manager.BeginTransaction();
        using var transaction2 = _manager.BeginTransaction();

        var stats = _manager.GetStatistics();

        await Assert.That(stats.ActiveTransactionCount).IsEqualTo(2);
        await Assert.That(stats.MaxTransactions).IsEqualTo(10);
        await Assert.That(stats.TransactionTimeout).IsEqualTo(TimeSpan.FromSeconds(30));
    }

    [Test]
    public async Task TransactionStateValidation_ShouldWorkCorrectly()
    {
        await VerifyTransactionState(TransactionState.Active, true);
        await VerifyTransactionState(TransactionState.Committed, false);
        await VerifyTransactionState(TransactionState.RolledBack, false);
        await VerifyTransactionState(TransactionState.Failed, false);
    }

    [Test]
    public async Task MultipleConcurrentTransactions_ShouldWorkCorrectly()
    {
        var transactions = new List<ITransaction>();

        for (int i = 0; i < 5; i++)
        {
            transactions.Add(_manager.BeginTransaction());
        }

        await Assert.That(_manager.ActiveTransactionCount).IsEqualTo(5);

        for (int i = 0; i < 2; i++)
        {
            transactions[i].Commit();
        }

        await Assert.That(_manager.ActiveTransactionCount).IsEqualTo(3);

        for (int i = 2; i < 5; i++)
        {
            transactions[i].Rollback();
        }

        await Assert.That(_manager.ActiveTransactionCount).IsEqualTo(0);
    }

    private async Task VerifyTransactionState(TransactionState state, bool canOperate)
    {
        using var transaction = _manager.BeginTransaction();

        if (state == TransactionState.Committed)
        {
            transaction.Commit();
        }
        else if (state == TransactionState.RolledBack || state == TransactionState.Failed)
        {
            transaction.Rollback();
        }

        if (canOperate)
        {
            var savepoint = transaction.CreateSavepoint("test");
            await Assert.That(savepoint).IsNotEqualTo(Guid.Empty);
        }
        else
        {
            await Assert.That(() => transaction.CreateSavepoint("test")).Throws<InvalidOperationException>();
        }
    }
}
