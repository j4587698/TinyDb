using System;
using System.IO;
using System.Threading.Tasks;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class TransactionManagerInactiveTransactionCoverageTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly TinyDbEngine _engine;
    private readonly TransactionManager _manager;

    public TransactionManagerInactiveTransactionCoverageTests()
    {
        _testDirectory = Path.Combine(
            Path.GetTempPath(),
            "TinyDbTransactionManagerInactiveTransactionCoverageTests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(_testDirectory);

        var dbPath = Path.Combine(_testDirectory, "test.db");
        _engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        _manager = new TransactionManager(_engine, maxTransactions: 10, transactionTimeout: TimeSpan.FromMinutes(1));
    }

    public void Dispose()
    {
        _manager.Dispose();
        _engine.Dispose();
        if (Directory.Exists(_testDirectory))
        {
            try { Directory.Delete(_testDirectory, true); } catch { }
        }
    }

    [Test]
    public async Task InactiveTransaction_ManagerOperations_ShouldThrow()
    {
        var tx = (Transaction)_manager.BeginTransaction();

        // Commit via manager to bypass Transaction state guards and exercise manager's active-transaction checks.
        _manager.CommitTransaction(tx);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            _manager.CommitTransaction(tx);
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            _manager.RollbackTransaction(tx);
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            _manager.CreateSavepoint(tx, "sp");
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            _manager.ReleaseSavepoint(tx, Guid.NewGuid());
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            _manager.RollbackToSavepoint(tx, Guid.NewGuid());
            return Task.CompletedTask;
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            _manager.RecordOperation(tx, new TransactionOperation(TransactionOperationType.Insert, "c"));
            return Task.CompletedTask;
        });
    }
}
