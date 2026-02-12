using System;
using System.IO;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TransactionAtomicityAndReinsertTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly ITinyCollection<BsonDocument> _collection;
    private readonly string _collectionName;

    public TransactionAtomicityAndReinsertTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"txn_atomic_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath, new TinyDbOptions { EnableJournaling = false });
        _collectionName = $"col_{Guid.NewGuid():N}";
        _collection = _engine.GetBsonCollection(_collectionName);
        _engine.EnsureIndex(_collectionName, "Code", "idx_code_unique", unique: true);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task Commit_WhenUniqueIndexViolationOccursMidApply_ShouldLeaveDatabaseUnchanged()
    {
        using var tx = _engine.BeginTransaction();

        _collection.Insert(new BsonDocument().Set("_id", 1).Set("Code", "dup"));
        _collection.Insert(new BsonDocument().Set("_id", 2).Set("Code", "dup"));

        var threw = false;
        try
        {
            tx.Commit();
        }
        catch (InvalidOperationException)
        {
            threw = true;
        }

        await Assert.That(threw).IsTrue();

        await Assert.That(_collection.Count()).IsEqualTo(0);

        _collection.Insert(new BsonDocument().Set("_id", 3).Set("Code", "dup"));
        await Assert.That(_collection.Count()).IsEqualTo(1);
    }

    [Test]
    public async Task Insert_Delete_InsertSameDocument_ShouldSucceed()
    {
        _collection.Insert(new BsonDocument().Set("_id", 1).Set("Code", "re"));
        await Assert.That(_collection.Count()).IsEqualTo(1);

        var deleted = _collection.Delete(1);
        await Assert.That(deleted).IsEqualTo(1);
        await Assert.That(_collection.Count()).IsEqualTo(0);

        _collection.Insert(new BsonDocument().Set("_id", 1).Set("Code", "re"));
        await Assert.That(_collection.Count()).IsEqualTo(1);
    }
}
