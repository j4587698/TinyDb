using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TransactionValidationEdgeCasesTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public TransactionValidationEdgeCasesTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"trans_val_edge_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Commit_Null_Transaction_Should_Throw()
    {
        var managerField = typeof(TinyDbEngine).GetField("_transactionManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var manager = (TransactionManager)managerField!.GetValue(_engine)!;

        await Assert.That(() => manager.CommitTransaction(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Rollback_Null_Transaction_Should_Throw()
    {
        var managerField = typeof(TinyDbEngine).GetField("_transactionManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var manager = (TransactionManager)managerField!.GetValue(_engine)!;

        await Assert.That(() => manager.RollbackTransaction(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task CreateSavepoint_Null_Transaction_Should_Throw()
    {
        var managerField = typeof(TinyDbEngine).GetField("_transactionManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var manager = (TransactionManager)managerField!.GetValue(_engine)!;

        await Assert.That(() => manager.CreateSavepoint(null!, "sp")).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task CreateSavepoint_Invalid_Name_Should_Throw()
    {
        using var trans = (Transaction)_engine.BeginTransaction();
        var managerField = typeof(TinyDbEngine).GetField("_transactionManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var manager = (TransactionManager)managerField!.GetValue(_engine)!;

        await Assert.That(() => manager.CreateSavepoint(trans, null!)).Throws<ArgumentException>();
        await Assert.That(() => manager.CreateSavepoint(trans, "")).Throws<ArgumentException>();
    }

    [Test]
    public async Task RollbackToSavepoint_Null_Transaction_Should_Throw()
    {
        var managerField = typeof(TinyDbEngine).GetField("_transactionManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var manager = (TransactionManager)managerField!.GetValue(_engine)!;

        await Assert.That(() => manager.RollbackToSavepoint(null!, Guid.NewGuid())).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task RollbackToSavepoint_Invalid_Id_Should_Throw()
    {
        using var trans = (Transaction)_engine.BeginTransaction();
        var managerField = typeof(TinyDbEngine).GetField("_transactionManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var manager = (TransactionManager)managerField!.GetValue(_engine)!;

        await Assert.That(() => manager.RollbackToSavepoint(trans, Guid.NewGuid())).Throws<ArgumentException>();
    }

    [Test]
    public async Task RecordOperation_Null_Args_Should_Throw()
    {
        using var trans = (Transaction)_engine.BeginTransaction();
        var managerField = typeof(TinyDbEngine).GetField("_transactionManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var manager = (TransactionManager)managerField!.GetValue(_engine)!;
        var op = new TransactionOperation(TransactionOperationType.Insert, "col");

        await Assert.That(() => manager.RecordOperation(null!, op)).Throws<ArgumentNullException>();
        await Assert.That(() => manager.RecordOperation(trans, null!)).Throws<ArgumentNullException>();
    }
}
