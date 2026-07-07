using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public sealed class TransactionRollbackCoverageTests
{
    [Test]
    public async Task RecordDropIndex_WhenIndexStatisticsThrows_ShouldThrow()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"tx_dropidx_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            const string collectionName = "c";
            const string indexName = "idx";

            engine.EnsureIndex(collectionName, "x", indexName, unique: true);

            var index = engine.GetIndexManager(collectionName).GetIndex(indexName);
            await Assert.That(index).IsNotNull();
            index!.Dispose();

            using var tx = (Transaction)engine.BeginTransaction();

            await Assert.That(() => tx.RecordDropIndex(collectionName, indexName))
                .Throws<InvalidOperationException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task Commit_WhenUnsupportedOperationIsRecorded_ShouldThrow()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"tx_comp_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions
            {
                EnableJournaling = false,
                BackgroundFlushInterval = Timeout.InfiniteTimeSpan
            });

            const string collectionName = "c";

            var tx = (Transaction)engine.BeginTransaction();
            tx.AddOperation(new TransactionOperation((TransactionOperationType)123, collectionName));

            await Assert.That(() => tx.Commit()).Throws<InvalidOperationException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task Commit_WhenApplyFailsAfterAppliedOperation_ShouldRollbackAppliedOperation()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"tx_comp_applied_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions
            {
                EnableJournaling = false,
                BackgroundFlushInterval = Timeout.InfiniteTimeSpan
            });

            const string appliedCollection = "c1";
            const string blockingCollection = "c2";
            const string indexName = "idx";

            engine.EnsureIndex(blockingCollection, "x", indexName, unique: true);

            var tx = (Transaction)engine.BeginTransaction();
            tx.AddOperation(new TransactionOperation(
                TransactionOperationType.Insert,
                appliedCollection,
                documentId: new BsonInt32(1),
                newDocument: new BsonDocument().Set("_id", 1).Set("x", 1)));
            tx.AddOperation(new TransactionOperation(
                TransactionOperationType.CreateIndex,
                blockingCollection,
                indexName: indexName,
                indexFields: new[] { "x" },
                indexUnique: false));

            await Assert.That(() => tx.Commit()).Throws<InvalidOperationException>();
            await Assert.That(engine.FindById(appliedCollection, new BsonInt32(1))).IsNull();
            await Assert.That(engine.IsCorrupted).IsFalse();

            tx.Dispose();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task MarkCorrupted_ShouldBlockOperationsAndDisposeWithoutFlush()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"tx_poison_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions
            {
                BackgroundFlushInterval = Timeout.InfiniteTimeSpan
            });

            engine.MarkCorrupted(new IOException("poison"));

            await Assert.That(engine.IsCorrupted).IsTrue();
            await Assert.That(() => engine.Flush()).Throws<InvalidOperationException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    private static void CleanupDb(string dbPath)
    {
        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }

        var walFile = Path.Combine(Path.GetDirectoryName(dbPath)!, $"{Path.GetFileNameWithoutExtension(dbPath)}-wal.db");
        try { if (File.Exists(walFile)) File.Delete(walFile); } catch { }
    }
}
