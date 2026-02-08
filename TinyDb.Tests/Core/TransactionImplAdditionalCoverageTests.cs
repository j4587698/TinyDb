using System;
using System.IO;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public sealed class TransactionImplAdditionalCoverageTests : IDisposable
{
    private readonly string _dbPath;

    public TransactionImplAdditionalCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"txn_impl_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
        try { if (File.Exists(_dbPath + "-wal")) File.Delete(_dbPath + "-wal"); } catch { }
        try { if (File.Exists(_dbPath + "-shm")) File.Delete(_dbPath + "-shm"); } catch { }
    }

    [Test]
    public async Task RecordUpdate_And_RecordDelete_ShouldUseFallbackIds()
    {
        using var engine = new TinyDbEngine(_dbPath);
        using var _ = engine.BeginTransaction();

        var tx = engine.GetCurrentTransaction();
        await Assert.That(tx).IsNotNull();

        var original = new BsonDocument().Set("_id", 123);
        var newWithId = new BsonDocument().Set("_id", 456);
        tx!.RecordUpdate("col", original, newWithId);
        tx.RecordUpdate("col", original, new BsonDocument());

        var deleteWithId = new BsonDocument().Set("_id", 789);
        tx.RecordDelete("col", deleteWithId);
        tx.RecordDelete("col", new BsonDocument());

        await Assert.That(tx.Operations.Count).IsEqualTo(4);
        await Assert.That(tx.Operations[0].DocumentId).IsEqualTo(newWithId["_id"]);
        await Assert.That(tx.Operations[1].DocumentId).IsEqualTo(original["_id"]);
        await Assert.That(tx.Operations[2].DocumentId).IsEqualTo(deleteWithId["_id"]);
        await Assert.That(tx.Operations[3].DocumentId).IsTypeOf<BsonObjectId>();
    }

    [Test]
    public async Task Dispose_WhenOperationsListIsCorrupted_ShouldNotThrow()
    {
        using var engine = new TinyDbEngine(_dbPath);
        engine.BeginTransaction();

        var tx = engine.GetCurrentTransaction();
        await Assert.That(tx).IsNotNull();

        var operationsField = typeof(Transaction).GetField("_operations", BindingFlags.Instance | BindingFlags.NonPublic);
        await Assert.That(operationsField).IsNotNull();

        operationsField!.SetValue(tx, null);

        await Assert.That(() => tx!.Dispose()).ThrowsNothing();
    }
}

