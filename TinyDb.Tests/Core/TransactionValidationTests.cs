using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TransactionValidationTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public TransactionValidationTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"trans_val_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task ValidateOperations_Should_Detect_Duplicate_Ids()
    {
        using var trans = (Transaction)_engine.BeginTransaction();
        var managerField = typeof(TinyDbEngine).GetField("_transactionManager", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        var manager = (TransactionManager)managerField!.GetValue(_engine)!;

        // Manually record two inserts with same ID
        var op1 = new TransactionOperation(TransactionOperationType.Insert, "Users", new TinyDb.Bson.BsonInt32(1));
        var op2 = new TransactionOperation(TransactionOperationType.Insert, "Users", new TinyDb.Bson.BsonInt32(1));
        
        manager.RecordOperation(trans, op1);
        manager.RecordOperation(trans, op2);

        await Assert.That(() => trans.Commit())
            .Throws<InvalidOperationException>()
            .WithMessage("Failed to commit transaction: Duplicate document IDs detected in transaction");
    }
}
