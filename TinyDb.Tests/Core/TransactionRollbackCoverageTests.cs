using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public sealed class TransactionRollbackCoverageTests
{
    [Test]
    public async Task RecordDropIndex_WhenIndexStatisticsThrows_ShouldBeSwallowed()
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

            using var tx = engine.BeginTransaction();
            var recordDropIndex = tx.GetType().GetMethod("RecordDropIndex", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(recordDropIndex).IsNotNull();

            await Assert.That(() => recordDropIndex!.Invoke(tx, new object[] { collectionName, indexName }))
                .ThrowsNothing();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task Commit_WhenRollbackCompensationThrows_ShouldBeSwallowed()
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
            tx.Operations.Add(new TransactionOperation((TransactionOperationType)123, collectionName));

            await Assert.That(() => tx.Commit()).Throws<InvalidOperationException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task Commit_WhenRollbackOfAppliedOperationThrows_ShouldBeSwallowed()
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
            var indexManager = engine.GetIndexManager(blockingCollection);

            var rwLockField = typeof(IndexManager).GetField("_rwLock", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(rwLockField).IsNotNull();

            var rwLock = (ReaderWriterLockSlim)rwLockField!.GetValue(indexManager)!;

            var tx = (Transaction)engine.BeginTransaction();
            tx.Operations.Add(new TransactionOperation(
                TransactionOperationType.Insert,
                appliedCollection,
                documentId: new BsonInt32(1),
                newDocument: new BsonDocument().Set("_id", 1).Set("x", 1)));
            tx.Operations.Add(new TransactionOperation(
                TransactionOperationType.CreateIndex,
                blockingCollection,
                indexName: indexName,
                indexFields: new[] { "x" },
                indexUnique: false));

            var commitTcs = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var commitThread = new Thread(() =>
            {
                try
                {
                    tx.Commit();
                    commitTcs.SetResult(null);
                }
                catch (Exception ex)
                {
                    commitTcs.SetException(ex);
                }
            })
            {
                IsBackground = true
            };

            bool commitReachedCreateIndex;
            rwLock.EnterWriteLock();
            try
            {
                commitThread.Start();

                commitReachedCreateIndex = SpinWait.SpinUntil(
                    () => rwLock.WaitingWriteCount > 0,
                    TimeSpan.FromSeconds(30));

                if (commitReachedCreateIndex)
                {
                    tx.Operations[0] = new TransactionOperation((TransactionOperationType)123, appliedCollection);
                }
            }
            finally
            {
                rwLock.ExitWriteLock();
            }

            await Assert.That(async () => await commitTcs.Task).Throws<InvalidOperationException>();
            await Assert.That(commitReachedCreateIndex).IsTrue();

            tx.Dispose();
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
