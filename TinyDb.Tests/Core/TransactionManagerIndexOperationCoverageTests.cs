using System;
using System.IO;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class TransactionManagerIndexOperationCoverageTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly TinyDbEngine _engine;

    public TransactionManagerIndexOperationCoverageTests()
    {
        _testDirectory = Path.Combine(
            Path.GetTempPath(),
            "TinyDbTransactionManagerIndexOperationCoverageTests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(_testDirectory);

        var dbPath = Path.Combine(_testDirectory, "test.db");
        _engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_testDirectory))
        {
            try { Directory.Delete(_testDirectory, true); } catch { }
        }
    }

    [Test]
    public async Task Commit_ShouldApply_CreateIndex_And_DropIndex_Operations()
    {
        var collectionName = $"products_{Guid.NewGuid():N}";
        _ = _engine.GetCollection<TestProduct>(collectionName);

        var tx = (Transaction)_engine.BeginTransaction();
        tx.RecordCreateIndex(collectionName, "idx_price", new[] { "Price" }, unique: false);
        tx.RecordDropIndex(collectionName, "idx_price");

        tx.Commit();

        var exists = _engine.GetIndexManager(collectionName).IndexExists("idx_price");
        await Assert.That(exists).IsFalse();
    }

    [Test]
    public async Task Commit_WhenCreateIndexAlreadyExists_ShouldThrow()
    {
        var collectionName = $"products_{Guid.NewGuid():N}";
        _ = _engine.GetCollection<TestProduct>(collectionName);

        // Create existing index with different uniqueness to force CreateIndex to throw inside the transaction.
        _engine.GetIndexManager(collectionName).CreateIndex("dup_idx", new[] { "Price" }, unique: true);

        var tx = (Transaction)_engine.BeginTransaction();
        tx.RecordCreateIndex(collectionName, "dup_idx", new[] { "Price" }, unique: false);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
        {
            tx.Commit();
            return Task.CompletedTask;
        });
    }
}
