using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Metadata;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Security;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using System.Globalization;
using System.Threading;
using ExpressionType = System.Linq.Expressions.ExpressionType;

namespace TinyDb.Tests.Regression;

[NotInParallel]
public sealed class ReviewReportRegressionTests : IDisposable
{
    private readonly string _directory;

    public ReviewReportRegressionTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), "TinyDbRegression", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_directory);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_directory)) Directory.Delete(_directory, true);
        }
        catch
        {
        }
    }

    [Test]
    public async Task PredicatePushdown_ShouldMatchInMemoryNumericAndBooleanSemantics()
    {
        using var engine = CreateEngine("pushdown.db");
        var collection = engine.GetCollection<QueryObjectValueDocument>();

        collection.Insert(new QueryObjectValueDocument { Id = 1, Value = 26 });
        collection.Insert(new QueryObjectValueDocument { Id = 2, Value = true });

        await Assert.That(collection.Find(x => x.Value == (object)25.5).Count()).IsEqualTo(0);
        await Assert.That(collection.Find(x => x.Value == (object)1).Count()).IsEqualTo(0);
    }

    [Test]
    public async Task BsonSpanReader_ShouldRejectOverflowingLengths()
    {
        var hugeStringLength = new byte[] { 0x7F, 0xFF, 0xFF, 0x7F };
        await Assert.That(() => new BsonSpanReader(hugeStringLength).ReadString()).Throws<InvalidOperationException>();

        var negativeBinaryLength = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x00 };
        await Assert.That(() => new BsonSpanReader(negativeBinaryLength).ReadBinary()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task AutoIncrementInt64Ids_ShouldPersistHighWatermarkAcrossReopen()
    {
        var path = Path.Combine(_directory, "identity-persist.db");

        using (var engine = new TinyDbEngine(path))
        {
            var collection = engine.GetCollection<AutoIdentityDocument>();
            var first = new AutoIdentityDocument { Name = "first" };
            var second = new AutoIdentityDocument { Name = "second" };

            collection.Insert(first);
            collection.Insert(second);
            collection.Delete(second.Id);

            await Assert.That(first.Id).IsEqualTo(1);
            await Assert.That(second.Id).IsEqualTo(2);
        }

        using (var reopened = new TinyDbEngine(path))
        {
            var collection = reopened.GetCollection<AutoIdentityDocument>();
            var third = new AutoIdentityDocument { Name = "third" };

            collection.Insert(third);

            await Assert.That(third.Id).IsEqualTo(3);
        }
    }

    [Test]
    public async Task BsonComparer_ShouldUseOrdinalStringsAndPreciseNumericComparison()
    {
        var upper = new BsonString("Z");
        var lower = new BsonString("a");

        await Assert.That(Math.Sign(upper.CompareTo(lower)))
            .IsEqualTo(Math.Sign(new IndexKey(upper).CompareTo(new IndexKey(lower))));

        var preciseLong = new BsonInt64(9_007_199_254_740_993L);
        var roundedDouble = new BsonDouble(9_007_199_254_740_992d);

        await Assert.That(preciseLong.CompareTo(roundedDouble)).IsGreaterThan(0);
        await Assert.That(roundedDouble.CompareTo(preciseLong)).IsLessThan(0);
    }

    [Test]
    public async Task BsonAndQueryComparers_ShouldUseSameStableNumericOrdering()
    {
        await Assert.That(new IndexKey(new BsonInt32(1)).CompareTo(new IndexKey(new BsonInt64(1))))
            .IsEqualTo(0);
        await Assert.That(new IndexKey(new BsonInt32(1)).Equals(new IndexKey(new BsonInt64(1))))
            .IsTrue();
        await Assert.That(new IndexKey(new BsonInt32(1)).GetHashCode())
            .IsEqualTo(new IndexKey(new BsonInt64(1)).GetHashCode());
        await Assert.That(QueryValueComparer.Compare(new BsonInt32(1), new BsonInt64(1)))
            .IsEqualTo(0);
        await Assert.That(QueryValueComparer.GetHashCode(1))
            .IsEqualTo(QueryValueComparer.GetHashCode(1L));

        object?[] values =
        [
            double.PositiveInfinity,
            decimal.MaxValue,
            1L,
            1,
            double.NaN,
            "x",
            null
        ];

        var sorted = values.OrderBy(v => v, Comparer<object?>.Create(QueryValueComparer.Compare)).ToArray();
        await Assert.That(sorted.Length).IsEqualTo(values.Length);
    }

    [Test]
    public async Task DiskBTree_DeleteAndContains_ShouldUseComparerConsistentNumericValueEquality()
    {
        var path = Path.Combine(_directory, "btree-numeric-value-equality.db");
        using var disk = new DiskStream(path);
        using var pageManager = new PageManager(disk, 4096, 8);
        using var tree = DiskBTree.Create(pageManager, maxKeys: 3);

        var insertedKey = new IndexKey(new BsonInt32(5));
        var lookupKey = new IndexKey(new BsonInt64(5));

        tree.Insert(insertedKey, new BsonInt32(42));

        await Assert.That(tree.Contains(lookupKey, new BsonInt64(42))).IsTrue();
        await Assert.That(tree.Delete(lookupKey, new BsonInt64(42))).IsTrue();
        await Assert.That(tree.Contains(insertedKey, new BsonInt32(42))).IsFalse();
    }

    [Test]
    public async Task BsonReader_ShouldRejectJavaScriptWithScopeSizeMismatch()
    {
        using var stream = new MemoryStream();
        using (var writer = new BsonWriter(stream, leaveOpen: true))
        {
            writer.WriteJavaScriptWithScope("code", new BsonDocument().Set("v", 1));
        }

        var data = stream.ToArray();
        var totalSize = BitConverter.ToInt32(data, 0);
        BitConverter.GetBytes(totalSize + 1).CopyTo(data, 0);

        using var reader = new BsonReader(new MemoryStream(data));
        await Assert.That(() => reader.ReadValue(BsonType.JavaScriptWithScope))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task BsonScanner_ShouldReturnFalseForMalformedMatchedValue()
    {
        var malformed = new byte[]
        {
            12, 0, 0, 0,
            (byte)BsonType.String,
            (byte)'x', 0,
            0, 0, 0, 0,
            0
        };

        await Assert.That(BsonScanner.TryGetValue(malformed, "x", out _)).IsFalse();
    }

    [Test]
    public async Task ExpressionEvaluator_ShouldRejectExcessiveExpressionDepth()
    {
        QueryExpression expression = new ConstantExpression(true);
        for (var i = 0; i < 300; i++)
        {
            expression = new BinaryExpression(ExpressionType.AndAlso, new ConstantExpression(true), expression);
        }

        await Assert.That(() => ExpressionEvaluator.Evaluate(expression, new BsonDocument()))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task TinyDbOptions_ShouldAcceptEncryptionWithValidKey()
    {
        var options = new TinyDbOptions
        {
            EnableEncryption = true,
            EncryptionKey = new byte[32]
        };

        options.Validate();
        await Assert.That(options.EnableEncryption).IsTrue();
    }

    [Test]
    public async Task PasswordManagerCreateSecureDatabase_ShouldUseEncryptionMetadata()
    {
        var path = Path.Combine(_directory, "password-manager-encrypted.db");

        using (var engine = PasswordManager.CreateSecureDatabase(path, "pass1234"))
        {
            engine.GetCollection<AutoIdentityDocument>().Insert(new AutoIdentityDocument { Name = "secure" });
        }

        var bytes = File.ReadAllBytes(path);
        var markerOffset = IndexOf(bytes.AsSpan(0, DatabaseHeader.Size), new byte[] { (byte)'S', (byte)'E', (byte)'C', (byte)'1' });
        await Assert.That(markerOffset).IsEqualTo(-1);
        await Assert.That(DatabaseSecurity.HasSecurityMetadata(path)).IsFalse();
        await Assert.That(PasswordManager.VerifyPassword(path, "pass1234")).IsTrue();
        await Assert.That(() => new TinyDbEngine(path)).Throws<UnauthorizedAccessException>();
    }

    [Test]
    public async Task PageChecksumCorruption_ShouldFailOnRead()
    {
        var path = Path.Combine(_directory, "page-checksum.db");
        uint pageId;

        using (var disk = new DiskStream(path))
        using (var pageManager = new PageManager(disk, TinyDbOptions.DefaultPageSize))
        {
            var page = pageManager.NewPage(PageType.Data);
            pageId = page.PageID;
            page.WriteData(0, new byte[] { 1, 2, 3, 4 });
            pageManager.SavePage(page, forceFlush: true);
        }

        var bytes = File.ReadAllBytes(path);
        var corruptionOffset = 80;
        await Assert.That(bytes.Length).IsGreaterThan(corruptionOffset);
        bytes[corruptionOffset] ^= 0x5A;
        File.WriteAllBytes(path, bytes);

        using var reopenedDisk = new DiskStream(path);
        using var reopenedPageManager = new PageManager(reopenedDisk, TinyDbOptions.DefaultPageSize);
        await Assert.That(() => reopenedPageManager.GetPage(pageId)).Throws<InvalidDataException>();
    }

    [Test]
    public async Task DateTimeSerialization_ShouldPreserveTicksAndKind()
    {
        var local = DateTime.SpecifyKind(
            new DateTime(2024, 6, 1, 12, 30, 15, 123).AddTicks(4567),
            DateTimeKind.Local);
        var bson = new BsonDateTime(local);

        var deserialized = (BsonDateTime)BsonSerializer.Deserialize(BsonSerializer.Serialize(bson));

        await Assert.That(deserialized.Value.Kind).IsEqualTo(DateTimeKind.Local);
        await Assert.That(deserialized.Value.Ticks).IsEqualTo(local.Ticks);
    }

    [Test]
    public async Task QueryValueComparer_ShouldNotOrderIncompatibleTypesByString()
    {
        await Assert.That(QueryValueComparer.EvaluateComparison(DateTime.UtcNow, "9999-01-01", ExpressionType.GreaterThan)).IsFalse();
        await Assert.That(QueryValueComparer.EvaluateComparison(DateTime.UtcNow, "9999-01-01", ExpressionType.LessThan)).IsFalse();
        await Assert.That(QueryValueComparer.EvaluateComparison(DateTime.UtcNow, "9999-01-01", ExpressionType.Equal)).IsFalse();
        await Assert.That(QueryValueComparer.EvaluateComparison(DateTime.UtcNow, "9999-01-01", ExpressionType.NotEqual)).IsTrue();
    }

    [Test]
    public async Task DiskBTree_ShouldFindDuplicateKeysAcrossLeafBoundaries()
    {
        var path = Path.Combine(_directory, "btree-duplicates.db");
        using var disk = new DiskStream(path);
        using var pageManager = new PageManager(disk, 4096, 8);
        var tree = DiskBTree.Create(pageManager, maxKeys: 3);
        var key = new IndexKey(new BsonInt32(10));

        for (int i = 0; i < 40; i++)
        {
            tree.Insert(key, new BsonInt32(i));
        }

        tree.Insert(new IndexKey(new BsonInt32(11)), new BsonInt32(100));

        await Assert.That(tree.Find(key)).Count().IsEqualTo(40);
        await Assert.That(tree.FindExact(key)).IsNotNull();
        await Assert.That(tree.Contains(key)).IsTrue();
    }

    [Test]
    public async Task DiskBTree_WithTinyCache_ShouldKeepOperationPagesPinned()
    {
        var path = Path.Combine(_directory, "btree-tiny-cache.db");
        using var disk = new DiskStream(path);
        using var pageManager = new PageManager(disk, 4096, 1);
        using var tree = DiskBTree.Create(pageManager, maxKeys: 3);

        for (int i = 0; i < 120; i++)
        {
            tree.Insert(new IndexKey(new BsonInt32(i)), new BsonInt32(i));
        }

        await Assert.That(tree.EntryCount).IsEqualTo(120);

        for (int i = 0; i < 120; i++)
        {
            await Assert.That(tree.FindExact(new IndexKey(new BsonInt32(i)))).IsEqualTo(new BsonInt32(i));
        }

        await Assert.That(tree.FindRange(new IndexKey(new BsonInt32(20)), new IndexKey(new BsonInt32(40)), true, true))
            .Count()
            .IsEqualTo(21);
    }

    [Test]
    public async Task DiskBTree_DeleteMergeThenReuseFreePages_ShouldNotKeepFreedPagesLeased()
    {
        var path = Path.Combine(_directory, "btree-free-reuse.db");
        using var disk = new DiskStream(path);
        using var pageManager = new PageManager(disk, 4096, 1);
        using var tree = DiskBTree.Create(pageManager, maxKeys: 3);

        for (int i = 0; i < 200; i++)
        {
            tree.Insert(new IndexKey(new BsonInt32(i)), new BsonInt32(i));
        }

        for (int i = 0; i < 150; i++)
        {
            await Assert.That(tree.Delete(new IndexKey(new BsonInt32(i)), new BsonInt32(i))).IsTrue();
        }

        for (int i = 200; i < 350; i++)
        {
            tree.Insert(new IndexKey(new BsonInt32(i)), new BsonInt32(i));
        }

        await Assert.That(tree.Validate()).IsTrue();
        await Assert.That(tree.EntryCount).IsEqualTo(200);

        for (int i = 0; i < 150; i++)
        {
            await Assert.That(tree.FindExact(new IndexKey(new BsonInt32(i)))).IsNull();
        }

        for (int i = 150; i < 350; i++)
        {
            await Assert.That(tree.FindExact(new IndexKey(new BsonInt32(i)))).IsEqualTo(new BsonInt32(i));
        }
    }

    [Test]
    public async Task FreePage_WalAfterImage_ShouldMatchDiskFreeListLink()
    {
        var path = Path.Combine(_directory, "freepage-wal-link.db");

        using var disk = new DiskStream(path);
        using var pageManager = new PageManager(disk, 4096, 8);

        var page = pageManager.NewPage(PageType.Data);
        var existingFreeHead = pageManager.NewPage(PageType.Data);
        var competingPage = pageManager.NewPage(PageType.Data);

        pageManager.SavePage(page);
        pageManager.SavePage(existingFreeHead);
        pageManager.SavePage(competingPage);
        pageManager.FreePage(existingFreeHead.PageID);

        Task? competingFree = null;
        uint walNextPageId = uint.MaxValue;

        pageManager.RegisterWAL(
            (walPage, _) =>
            {
                if (walPage.PageID != page.PageID) return;

                walNextPageId = walPage.Header.NextPageID;
                if (competingFree != null) return;

                competingFree = Task.Run(() => pageManager.FreePage(competingPage.PageID));
                Thread.Sleep(100);
            },
            _ => { });

        pageManager.FreePage(page.PageID);

        if (competingFree != null)
        {
            await competingFree.WaitAsync(TimeSpan.FromSeconds(5));
        }

        var reloaded = pageManager.GetPage(page.PageID, useCache: false);
        await Assert.That(reloaded.Header.NextPageID).IsEqualTo(walNextPageId);
    }

    [Test]
    public async Task WriteAheadLog_ShouldUndoUncommittedTransactionPages()
    {
        var path = Path.Combine(_directory, "wal-transaction-undo.db");
        uint pageId;

        using (var disk = new DiskStream(path))
        using (var pageManager = new PageManager(disk, 4096, 8))
        using (var wal = new WriteAheadLog(path, 4096, enabled: true))
        {
            pageManager.RegisterWAL(
                (page, beforeImage) => wal.AppendPage(page, beforeImage),
                lsn => wal.FlushToLSN(lsn),
                () => wal.RequiresBeforeImage);

            var page = pageManager.NewPage(PageType.Data);
            pageId = page.PageID;
            page.WriteData(0, new byte[] { 1, 2, 3 });
            pageManager.SavePage(page, forceFlush: true);
            wal.Synchronize(() => pageManager.FlushDirtyPages());

            using (var tx = wal.BeginTransaction(Guid.NewGuid()))
            {
                page.WriteData(0, new byte[] { 9, 9, 9 });
                pageManager.SavePage(page, forceFlush: true);
            }
        }

        using var recoveryDisk = new DiskStream(path);
        using var recoveryPageManager = new PageManager(recoveryDisk, 4096, 8);
        using var recoveryWal = new WriteAheadLog(path, 4096, enabled: true);

        recoveryWal.Replay(
            (id, data) => recoveryPageManager.RestorePage(id, data),
            (id, data) => recoveryPageManager.RestorePage(id, data));

        var recoveredPage = recoveryPageManager.GetPage(pageId);
        await Assert.That(recoveredPage.ReadBytes(0, 3).SequenceEqual(new byte[] { 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task WriteAheadLogAsyncReplay_ShouldUndoUncommittedTransactionPages()
    {
        var path = Path.Combine(_directory, "wal-transaction-undo-async.db");
        uint pageId;

        using (var disk = new DiskStream(path))
        using (var pageManager = new PageManager(disk, 4096, 8))
        using (var wal = new WriteAheadLog(path, 4096, enabled: true))
        {
            pageManager.RegisterWAL(
                (page, beforeImage) => wal.AppendPage(page, beforeImage),
                lsn => wal.FlushToLSN(lsn),
                () => wal.RequiresBeforeImage);

            var page = pageManager.NewPage(PageType.Data);
            pageId = page.PageID;
            page.WriteData(0, new byte[] { 1, 2, 3 });
            pageManager.SavePage(page, forceFlush: true);
            wal.Synchronize(() => pageManager.FlushDirtyPages());

            using (var tx = wal.BeginTransaction(Guid.NewGuid()))
            {
                page.WriteData(0, new byte[] { 9, 9, 9 });
                pageManager.SavePage(page, forceFlush: true);
            }
        }

        using var recoveryDisk = new DiskStream(path);
        using var recoveryPageManager = new PageManager(recoveryDisk, 4096, 8);
        using var recoveryWal = new WriteAheadLog(path, 4096, enabled: true);

        await recoveryWal.ReplayAsync(
            (id, data) =>
            {
                recoveryPageManager.RestorePage(id, data);
                return Task.CompletedTask;
            },
            (id, data) =>
            {
                recoveryPageManager.RestorePage(id, data);
                return Task.CompletedTask;
            });

        var recoveredPage = recoveryPageManager.GetPage(pageId);
        await Assert.That(recoveredPage.ReadBytes(0, 3).SequenceEqual(new byte[] { 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task BatchInsert_WhenUniqueIndexFails_ShouldRollbackInsertedDocuments()
    {
        using var engine = CreateEngine("batch-rollback.db");
        var collection = engine.GetCollection<UniqueEmailDocument>();

        await Assert.That(() => collection.Insert(new[]
        {
            new UniqueEmailDocument { Id = 1, Email = "same@example.com" },
            new UniqueEmailDocument { Id = 2, Email = "same@example.com" }
        })).Throws<AggregateException>();

        await Assert.That(collection.Count()).IsEqualTo(0);
        await Assert.That(collection.FindById(1)).IsNull();
        await Assert.That(collection.FindById(2)).IsNull();
    }

    [Test]
    public async Task ConcurrentTransactions_ShouldDetectLostUpdate()
    {
        using var engine = CreateEngine("tx-conflict.db");
        var collection = engine.GetCollection<TransactionDocument>();
        collection.Insert(new TransactionDocument { Id = 1, Value = 0 });

        using var ready = new Barrier(2);

        var first = Task.Run(() => UpdateInTransaction(engine, 1, ready));
        var second = Task.Run(() => UpdateInTransaction(engine, 2, ready));

        var results = await Task.WhenAll(first, second);

        await Assert.That(results.Count(result => result)).IsEqualTo(1);
        await Assert.That(results.Count(result => !result)).IsEqualTo(1);
    }

    [Test]
    public async Task TransactionCommit_WithJournaling_ShouldNotReenterDurabilityFlush()
    {
        var path = Path.Combine(_directory, "tx-journaled-commit.db");
        var options = new TinyDbOptions
        {
            EnableJournaling = true,
            WriteConcern = WriteConcern.Synced
        };

        using (var engine = new TinyDbEngine(path, options))
        {
            var collection = engine.GetCollection<TransactionDocument>();
            var tx = engine.BeginTransaction();
            collection.Insert(new TransactionDocument { Id = 1, Value = 42 });

            await Task.Run(() => tx.Commit()).WaitAsync(TimeSpan.FromSeconds(5));
            tx.Dispose();
        }

        using (var reopened = new TinyDbEngine(path, options))
        {
            var collection = reopened.GetCollection<TransactionDocument>();
            var document = collection.FindById(1);
            await Assert.That(document).IsNotNull();
            await Assert.That(document!.Value).IsEqualTo(42);
        }
    }

    [Test]
    public async Task TransactionCommit_WithConcurrentImplicitWriteAndWal_ShouldComplete()
    {
        var path = Path.Combine(_directory, "tx-concurrent-wal.db");
        var options = new TinyDbOptions
        {
            EnableJournaling = true,
            WriteConcern = WriteConcern.Journaled
        };

        using var engine = new TinyDbEngine(path, options);
        var collection = engine.GetCollection<TransactionDocument>();
        collection.Insert(new TransactionDocument { Id = 1, Value = 0 });

        using var tx = engine.BeginTransaction();
        var document = collection.FindById(1)!;
        document.Value = 100;
        collection.Update(document);

        var commitTask = Task.Run(() => tx.Commit());
        Task insertTask;
        using (ExecutionContext.SuppressFlow())
        {
            insertTask = Task.Run(() =>
            {
                var implicitCollection = engine.GetCollection<TransactionDocument>();
                implicitCollection.Insert(new TransactionDocument { Id = 2, Value = 200 });
            });
        }

        await Task.WhenAll(commitTask, insertTask).WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(collection.FindById(1)!.Value).IsEqualTo(100);
        await Assert.That(collection.FindById(2)!.Value).IsEqualTo(200);
    }

    [Test]
    public async Task DropCollection_ShouldClearIdentitySequenceCache()
    {
        using var engine = CreateEngine("drop-identity-cache.db");
        var collection = engine.GetCollection<AutoIdentityDocument>();

        var first = new AutoIdentityDocument { Name = "first" };
        collection.Insert(first);
        await Assert.That(first.Id).IsEqualTo(1);

        await Assert.That(engine.DropCollection("AutoIdentityDocuments")).IsTrue();

        var recreated = engine.GetCollection<AutoIdentityDocument>();
        var second = new AutoIdentityDocument { Name = "second" };
        recreated.Insert(second);

        await Assert.That(second.Id).IsEqualTo(1);
    }

    [Test]
    public async Task IndexedNotEqual_ShouldReturnMatchingRows()
    {
        using var engine = CreateEngine("indexed-not-equal.db");
        var collection = engine.GetCollection<IndexedValueDocument>();

        collection.Insert(new IndexedValueDocument { Id = 1, Score = 5 });
        collection.Insert(new IndexedValueDocument { Id = 2, Score = 6 });
        collection.Insert(new IndexedValueDocument { Id = 3, Score = 7 });
        engine.EnsureIndex("IndexedValueDocuments", "Score", "idx_score");

        var ids = collection.Find(x => x.Score != 5).Select(x => x.Id).OrderBy(x => x).ToArray();

        await Assert.That(ids.SequenceEqual(new[] { 2, 3 })).IsTrue();
    }

    [Test]
    public async Task PageManager_ShouldCalculateOffsetsBeyondFourGb()
    {
        var path = Path.Combine(_directory, "large-offset.db");
        using var disk = new DiskStream(path);
        using var pageManager = new PageManager(disk, 8192);
        var pageId = 600_000u;
        var offset = pageManager.CalculatePageOffset(pageId);

        await Assert.That(offset).IsEqualTo(((long)pageId - 1) * 8192);
    }

    [Test]
    public async Task DefaultObjectId_ShouldSerializeAsAllZeroBytes()
    {
        var value = new BsonObjectId(default(ObjectId));

        var deserialized = (BsonObjectId)BsonSerializer.Deserialize(BsonSerializer.Serialize(value));

        await Assert.That(deserialized.Value).IsEqualTo(ObjectId.Empty);
        await Assert.That(deserialized.Value.ToByteArray().ToArray().All(b => b == 0)).IsTrue();
    }

    [Test]
    public async Task SparseUniqueIndex_ShouldIgnoreMissingFields()
    {
        using var manager = new IndexManager("sparse_unique");
        manager.CreateIndex("idx_email", new[] { "email" }, unique: true, sparse: true);

        manager.InsertDocument(new BsonDocument().Set("_id", 1), new BsonInt32(1));
        manager.InsertDocument(new BsonDocument().Set("_id", 2), new BsonInt32(2));
        manager.InsertDocument(new BsonDocument().Set("_id", 3).Set("email", "a@example.com"), new BsonInt32(3));

        var index = manager.GetIndex("idx_email")!;
        await Assert.That(index.GetStatistics().IsSparse).IsTrue();
        await Assert.That(index.EntryCount).IsEqualTo(1);
        await Assert.That(() => manager.InsertDocument(
            new BsonDocument().Set("_id", 4).Set("email", "a@example.com"),
            new BsonInt32(4))).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task CompositeIndex_ShouldMatchPredicatesByIndexFieldOrder()
    {
        using var engine = CreateEngine("composite-order.db");
        engine.GetIndexManager("CompositeOrderDocuments").CreateIndex("idx_b_a", new[] { "B", "A" });
        var optimizer = new QueryOptimizer(engine);

        var plan = optimizer.CreateExecutionPlan<CompositeOrderDocument>(
            "CompositeOrderDocuments",
            x => x.A == 1 && x.B == 2);

        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.UseIndex!.Name).IsEqualTo("idx_b_a");
        await Assert.That(plan.IndexScanKeys.Select(k => k.FieldName).SequenceEqual(new[] { "b", "a" })).IsTrue();
    }

    [Test]
    public async Task ReverseRangeScan_ShouldReadDuplicateKeysAcrossLeafBoundary()
    {
        var path = Path.Combine(_directory, "btree-reverse-duplicates.db");
        using var disk = new DiskStream(path);
        using var pageManager = new PageManager(disk, 4096, 8);
        using var tree = DiskBTree.Create(pageManager, maxKeys: 3);
        var duplicateKey = new IndexKey(new BsonInt32(10));

        tree.Insert(new IndexKey(new BsonInt32(9)), new BsonInt32(-1));
        for (int i = 0; i < 40; i++)
        {
            tree.Insert(duplicateKey, new BsonInt32(i));
        }
        tree.Insert(new IndexKey(new BsonInt32(11)), new BsonInt32(100));

        var values = tree.FindRangeReverse(duplicateKey, duplicateKey, true, true)
            .Cast<BsonInt32>()
            .Select(x => x.Value)
            .OrderBy(x => x)
            .ToArray();

        await Assert.That(values.SequenceEqual(Enumerable.Range(0, 40))).IsTrue();
    }

    [Test]
    public async Task Decimal128_ToString_ShouldHandleExternalLargeFiniteValues()
    {
        var large = new Decimal128(1UL, 0x3108000000000000UL);

        await Assert.That(() => large.ToString()).ThrowsNothing();
        await Assert.That(() => large.ToDecimal()).Throws<OverflowException>();
    }

    [Test]
    public async Task Decimal128_ShouldUseNumericEqualityForBooleanAndHashing()
    {
        var zero = new BsonDecimal128(0m);
        var oneWithOneScale = new Decimal128(1.0m);
        var oneWithTwoScale = new Decimal128(1.00m);
        var largeOne = new BsonDecimal128(new Decimal128(1UL, 0x3108000000000000UL));
        var largeTwo = new BsonDecimal128(new Decimal128(2UL, 0x3108000000000000UL));

        await Assert.That(zero.ToBoolean(null)).IsFalse();
        await Assert.That(oneWithOneScale.Equals(oneWithTwoScale)).IsTrue();
        await Assert.That(oneWithOneScale.GetHashCode()).IsEqualTo(oneWithTwoScale.GetHashCode());
        await Assert.That(largeOne.CompareTo(largeTwo)).IsLessThan(0);
    }

    [Test]
    public async Task Decimal128_GetHashCode_ShouldNotFormatLargeExponentValues()
    {
        var exponent = 5000;
        var biasedExponent = (ulong)(6176 + exponent);
        var sameValueWithTrailingZero = (ulong)(6176 + exponent - 1);
        var value = new Decimal128(1UL, biasedExponent << 49);
        var equivalent = new Decimal128(10UL, sameValueWithTrailingZero << 49);

        await Assert.That(value.Equals(equivalent)).IsTrue();
        await Assert.That(() => value.GetHashCode()).ThrowsNothing();
        await Assert.That(value.GetHashCode()).IsEqualTo(equivalent.GetHashCode());
    }

    [Test]
    public async Task BsonBinary_ShouldDefensivelyCopyByteArrays()
    {
        var source = new byte[] { 1, 2, 3 };
        var binary = new BsonBinary(source);
        var hash = binary.GetHashCode();

        source[0] = 9;
        var exposed = binary.Bytes;
        exposed[1] = 9;

        await Assert.That(binary.Bytes.SequenceEqual(new byte[] { 1, 2, 3 })).IsTrue();
        await Assert.That(binary.GetHashCode()).IsEqualTo(hash);
    }

    [Test]
    public async Task BsonDocumentHashCode_ShouldReduceSymmetricCollisions()
    {
        var first = new BsonDocument()
            .Set("a", new BsonInt32(1))
            .Set("b", new BsonInt32(2));
        var second = new BsonDocument()
            .Set("a", new BsonInt32(2))
            .Set("b", new BsonInt32(1));

        await Assert.That(first.Equals(second)).IsFalse();
        await Assert.That(first.GetHashCode()).IsNotEqualTo(second.GetHashCode());
    }

    [Test]
    public async Task PrimaryKeyLookup_ShouldUseBsonNumericValueEquality()
    {
        using var engine = CreateEngine("primary-key-numeric-equality.db");
        var collection = engine.GetCollection<IndexedValueDocument>();

        collection.Insert(new IndexedValueDocument { Id = 10, Score = 5 });

        var found = collection.FindById(new BsonInt64(10));
        await Assert.That(found).IsNotNull();
        await Assert.That(found!.Score).IsEqualTo(5);
    }

    [Test]
    public async Task TransactionFindAll_ShouldMergeNumericEquivalentIds()
    {
        const string collectionName = "tx_numeric_merge";
        using var engine = CreateEngine("tx-numeric-merge.db");
        var collection = engine.GetCollection<NumericIdOrderDocument>(collectionName);

        engine.InsertDocument(collectionName, new BsonDocument()
            .Set("_id", new BsonInt64(5))
            .Set("Score", 10));

        using var tx = engine.BeginTransaction();
        collection.Update(new NumericIdOrderDocument { Id = 5, Score = 20 });

        var rows = collection.FindAll().ToList();

        await Assert.That(rows.Count).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(5);
        await Assert.That(rows[0].Score).IsEqualTo(20);
    }

    [Test]
    public async Task OrderIndexTransactionOverlay_ShouldHideCommittedNumericEquivalentId()
    {
        const string collectionName = "tx_numeric_order";
        using var engine = CreateEngine("tx-numeric-order.db");
        var collection = engine.GetCollection<NumericIdOrderDocument>(collectionName);
        engine.EnsureIndex(collectionName, "Score", "idx_score");

        engine.InsertDocument(collectionName, new BsonDocument()
            .Set("_id", new BsonInt64(5))
            .Set("Score", 10));

        using var tx = engine.BeginTransaction();
        collection.Update(new NumericIdOrderDocument { Id = 5, Score = 20 });

        var rows = collection.Query().OrderBy(static x => x.Score).ToList();

        await Assert.That(rows.Count).IsEqualTo(1);
        await Assert.That(rows[0].Id).IsEqualTo(5);
        await Assert.That(rows[0].Score).IsEqualTo(20);
    }

    [Test]
    public async Task TransactionWriteConflict_ShouldUseBsonNumericValueEquality()
    {
        const string collectionName = "tx_numeric_write_conflict";
        using var engine = CreateEngine("tx-numeric-write-conflict.db");
        var collection = engine.GetBsonCollection(collectionName);

        collection.Insert(new BsonDocument()
            .Set("_id", new BsonInt64(1))
            .Set("Score", new BsonInt64(5))
            .Set("Values", new BsonArray(new BsonValue[] { new BsonInt64(10) })));

        using var tx = (Transaction)engine.BeginTransaction();
        tx.AddOperation(new TransactionOperation(
            TransactionOperationType.Update,
            collectionName,
            new BsonInt32(1),
            originalDocument: new BsonDocument()
                .Set("_id", new BsonInt32(1))
                .Set("_collection", collectionName)
                .Set("Score", new BsonInt32(5))
                .Set("Values", new BsonArray(new BsonValue[] { new BsonInt32(10) })),
            newDocument: new BsonDocument()
                .Set("_id", new BsonInt32(1))
                .Set("_collection", collectionName)
                .Set("Score", new BsonInt32(6))
                .Set("Values", new BsonArray(new BsonValue[] { new BsonInt32(11) }))));

        await Assert.That(() => tx.Commit()).ThrowsNothing();

        var updated = collection.FindById(new BsonInt64(1));
        await Assert.That(updated).IsNotNull();
        await Assert.That(updated!["Score"]).IsEqualTo(new BsonInt32(6));
    }

    [Test]
    public async Task QueryAggregationProjection_ShouldConvertSumAndAverageToLinqReturnTypes()
    {
        using var engine = CreateEngine("query-aggregate-return-types.db");
        var collection = engine.GetCollection<QueryAggregateDocument>();

        collection.Insert(new QueryAggregateDocument { Id = 1, Category = "A", Score = 1 });
        collection.Insert(new QueryAggregateDocument { Id = 2, Category = "A", Score = 2 });

        var sum = collection.Query()
            .GroupBy(x => x.Category)
            .Select(g => g.Sum(x => x.Score))
            .Single();
        var average = collection.Query()
            .GroupBy(x => x.Category)
            .Select(g => g.Average(x => x.Score))
            .Single();

        await Assert.That(sum).IsEqualTo(3);
        await Assert.That(average).IsEqualTo(1.5d);
    }

    [Test]
    public async Task ExpressionEvaluator_ShouldUseCSharpIntegerDivision()
    {
        var divide = new BinaryExpression(
            ExpressionType.Divide,
            new ConstantExpression(5),
            new ConstantExpression(2));

        var result = ExpressionEvaluator.EvaluateValue(divide, new BsonDocument());

        await Assert.That(result).IsEqualTo(2);
    }

    [Test]
    public async Task StringFunctions_ShouldUseInvariantCaseConversion()
    {
        var previousCulture = CultureInfo.CurrentCulture;
        var previousUiCulture = CultureInfo.CurrentUICulture;

        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("tr-TR");
            CultureInfo.CurrentUICulture = new CultureInfo("tr-TR");

            using var engine = CreateEngine("invariant-string-functions.db");
            var collection = engine.GetCollection<CultureStringDocument>();
            collection.Insert(new CultureStringDocument { Id = 1, Name = "INTEREST" });

            await Assert.That(collection.Find(x => x.Name.ToLower() == "interest").Count()).IsEqualTo(1);
        }
        finally
        {
            CultureInfo.CurrentCulture = previousCulture;
            CultureInfo.CurrentUICulture = previousUiCulture;
        }
    }

    [Test]
    public async Task MetadataCodeGeneration_ShouldRejectUnsafeTypeSyntaxAndEscapeLineSeparators()
    {
        var unsafeSchema = new MetadataDocument
        {
            TableName = "UnsafeTypes",
            TypeName = "UnsafeTypes",
            DisplayName = "UnsafeTypes",
            Columns = new BsonArray()
                .AddValue(new BsonDocument()
                    .Set("n", "_id")
                    .Set("pn", "Id")
                    .Set("t", "System.Int32")
                    .Set("pk", true)
                    .Set("r", true)
                    .Set("o", 1))
                .AddValue(new BsonDocument()
                    .Set("n", "name")
                    .Set("pn", "Name")
                    .Set("t", "System.String\npublic int Injected { get; set; }")
                    .Set("r", false)
                    .Set("o", 2))
        };

        await Assert.That(() => CSharpEntityGenerator.Generate(unsafeSchema))
            .Throws<InvalidOperationException>();

        var safeSchema = new MetadataDocument
        {
            TableName = "GeneratedTypes",
            TypeName = "GeneratedTypes",
            DisplayName = "Line\u2028Separator",
            Columns = new BsonArray()
                .AddValue(new BsonDocument()
                    .Set("n", "_id")
                    .Set("pn", "Id")
                    .Set("t", "System.Int32")
                    .Set("pk", true)
                    .Set("r", true)
                    .Set("o", 1))
        };

        var code = CSharpEntityGenerator.Generate(safeSchema);
        await Assert.That(code).Contains("\\u2028");
    }

    [Test]
    public async Task BsonSerialization_ShouldRejectExcessiveDepthAndConversionCycles()
    {
        BsonValue deepValue = new BsonString("leaf");
        for (var i = 0; i < 130; i++)
        {
            deepValue = new BsonDocument().Set("x", deepValue);
        }

        var deepDocument = (BsonDocument)deepValue;
        await Assert.That(() => BsonSerializer.CalculateDocumentSize(deepDocument))
            .Throws<InvalidDataException>();
        await Assert.That(() => BsonSerializer.SerializeDocument(deepDocument))
            .Throws<InvalidDataException>();

        var cycle = new List<object>();
        cycle.Add(cycle);

        await Assert.That(() => BsonConversion.ToBsonValue(cycle))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task LargeDocument_ShouldPopulateSecondaryIndexesFromOriginalDocument()
    {
        const string collectionName = "large_indexed_documents";
        using var engine = CreateEngine("large-secondary-index.db");
        var collection = engine.GetCollection<LargeIndexedDocument>(collectionName);

        engine.EnsureIndex(collectionName, "Category", "idx_large_category");

        collection.Insert(new LargeIndexedDocument
        {
            Id = 1,
            Category = "target",
            Payload = new string('x', 20_000)
        });

        var rows = collection.Find(static x => x.Category == "target").ToList();

        await Assert.That(rows.Count).IsEqualTo(1);
        await Assert.That(rows[0].Payload.Length).IsEqualTo(20_000);
    }

    [Test]
    public async Task TransactionOverlay_ShouldNotMergeStringAndNumericIdsWithSameText()
    {
        const string collectionName = "tx_mixed_id_overlay";
        using var engine = CreateEngine("tx-mixed-id-overlay.db");
        var collection = engine.GetBsonCollection(collectionName);

        collection.Insert(new BsonDocument()
            .Set("_id", new BsonString("123"))
            .Set("Name", "string-id"));
        collection.Insert(new BsonDocument()
            .Set("_id", new BsonInt32(123))
            .Set("Name", "numeric-id"));

        using var tx = engine.BeginTransaction();
        collection.Update(new BsonDocument()
            .Set("_id", new BsonInt32(123))
            .Set("Name", "numeric-updated"));

        var rows = collection.FindAll().ToList();

        await Assert.That(rows.Count).IsEqualTo(2);
        await Assert.That(rows.Any(static doc =>
            doc["_id"] is BsonString id &&
            id.Value == "123" &&
            doc["Name"].ToString() == "string-id")).IsTrue();
        await Assert.That(rows.Any(static doc =>
            doc["_id"] is BsonInt32 id &&
            id.Value == 123 &&
            doc["Name"].ToString() == "numeric-updated")).IsTrue();
    }

    [Test]
    public async Task QueryOptimizer_ShouldKeepCaseSensitiveFieldComparisonsSeparate()
    {
        const string collectionName = "case_sensitive_fields";
        using var engine = CreateEngine("case-sensitive-optimizer.db");
        engine.GetIndexManager(collectionName)
            .CreateIndex("idx_case_fields", new[] { "myField" });

        var optimizer = new QueryOptimizer(engine);
        var query = new BinaryExpression(
            ExpressionType.AndAlso,
            new BinaryExpression(
                ExpressionType.Equal,
                new MemberExpression("MyField"),
                new ConstantExpression(1)),
            new BinaryExpression(
                ExpressionType.Equal,
                new MemberExpression("myField"),
                new ConstantExpression(2)));

        var plan = optimizer.CreateExecutionPlan(collectionName, query);

        await Assert.That(plan.IndexScanKeys.Count).IsEqualTo(1);
        await Assert.That(plan.IndexScanKeys[0].FieldName).IsEqualTo("myField");
        await Assert.That(plan.IndexScanKeys[0].Value).IsEqualTo(new BsonInt32(2));
    }

    [Test]
    public async Task BsonDocumentCompareTo_ShouldBeAntisymmetricForDifferentKeySets()
    {
        var left = new BsonDocument().Set("A", 1);
        var right = new BsonDocument().Set("B", 2);

        var leftToRight = left.CompareTo(right);
        var rightToLeft = right.CompareTo(left);

        await Assert.That(Math.Sign(leftToRight)).IsEqualTo(-Math.Sign(rightToLeft));
        await Assert.That(Math.Sign(leftToRight)).IsLessThan(0);
        await Assert.That(Math.Sign(rightToLeft)).IsGreaterThan(0);
    }

    [Test]
    public async Task BsonSerializer_ShouldRoundTripDocumentsThroughNonSeekableStreams()
    {
        var original = new BsonDocument()
            .Set("name", "non-seek")
            .Set("nested", new BsonDocument().Set("value", 42))
            .Set("items", new BsonArray([new BsonInt64(1), new BsonDouble(2.5), BsonBoolean.True]))
            .Set("script", new BsonJavaScriptWithScope("return value;", new BsonDocument().Set("value", 7)));

        using var output = new MemoryStream();
        using (var nonSeekOutput = new NonSeekStream(output, leaveOpen: true))
        {
            BsonSerializer.SerializeDocument(original, nonSeekOutput);
        }

        using var input = new NonSeekStream(new MemoryStream(output.ToArray()));
        using var reader = new BsonReader(input);
        var roundTrip = reader.ReadDocument();

        await Assert.That(roundTrip["name"].ToString()).IsEqualTo("non-seek");
        await Assert.That(((BsonDocument)roundTrip["nested"])["value"].ToInt32()).IsEqualTo(42);
        await Assert.That(((BsonArray)roundTrip["items"]).Count).IsEqualTo(3);
        await Assert.That(((BsonJavaScriptWithScope)roundTrip["script"]).Scope["value"].ToInt32()).IsEqualTo(7);
    }

    [Test]
    public async Task BsonSerializer_ShouldPatchDocumentSizesForSeekableStreams()
    {
        var document = new BsonDocument()
            .Set("name", "seek")
            .Set("nested", new BsonDocument().Set("value", 42))
            .Set("items", new BsonArray([new BsonInt32(1), new BsonInt32(2)]));

        using var stream = new MemoryStream();
        BsonSerializer.SerializeDocument(document, stream);

        var expected = BsonSerializer.SerializeDocument(document);
        await Assert.That(stream.ToArray().SequenceEqual(expected)).IsTrue();
    }

    [Test]
    public async Task BsonSerializer_GetRecyclableStream_ShouldSupportExpansionAndReuseSemantics()
    {
        using var stream = BsonSerializer.GetRecyclableStream();
        var payload = Enumerable.Range(0, 10_000).Select(static i => (byte)(i % 251)).ToArray();

        stream.Write(payload);
        stream.Position = 0;

        var read = new byte[payload.Length];
        var bytesRead = stream.Read(read, 0, read.Length);

        await Assert.That(bytesRead).IsEqualTo(payload.Length);
        await Assert.That(read.SequenceEqual(payload)).IsTrue();
    }

    [Test]
    public async Task AotBsonMapper_ShouldConvertAnyOneDimensionalArrayFallback()
    {
        var longValues = (long[])AotBsonMapper.ConvertValue(
            new BsonArray([new BsonInt64(1), new BsonInt64(2)]),
            typeof(long[]))!;
        var doubleValues = (double[])AotBsonMapper.ConvertValue(
            new BsonArray([new BsonDouble(1.25), new BsonDouble(2.5)]),
            typeof(double[]))!;
        var boolValues = (bool[])AotBsonMapper.ConvertValue(
            new BsonArray([BsonBoolean.True, BsonBoolean.False]),
            typeof(bool[]))!;

        await Assert.That(longValues.SequenceEqual([1L, 2L])).IsTrue();
        await Assert.That(doubleValues.SequenceEqual([1.25, 2.5])).IsTrue();
        await Assert.That(boolValues.SequenceEqual([true, false])).IsTrue();
    }

    [Test]
    public async Task AotBsonMapper_ShouldRejectSelfReferencingCollections()
    {
        var items = new List<object?>();
        items.Add(items);
        var entity = new SelfReferencingCollectionDocument
        {
            Id = 1,
            Items = items
        };

        await Assert.That(() => AotBsonMapper.ToDocument(entity)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task AotBsonMapper_ShouldUseAsyncLocalSerializationContext()
    {
        var contextType = AotBsonMapper.SerializationContextType;

        await Assert.That(contextType.IsGenericType).IsTrue();
        await Assert.That(contextType.GetGenericTypeDefinition()).IsEqualTo(typeof(AsyncLocal<>));
    }

    [Test]
    public async Task MetadataManager_SaveMetadata_ShouldBeAtomicForConcurrentFirstWrites()
    {
        using var engine = CreateEngine("metadata-race.db");
        const string tableName = "metadata_race";

        var tasks = Enumerable.Range(0, 16)
            .Select(i => Task.Run(() => engine.MetadataManager.SaveMetadata(CreateRaceSchema(tableName, i))))
            .ToArray();

        await Task.WhenAll(tasks);

        await Assert.That(engine.MetadataManager.GetMetadata(tableName)).IsNotNull();
    }

    private TinyDbEngine CreateEngine(string fileName)
    {
        return new TinyDbEngine(Path.Combine(_directory, fileName));
    }

    private static MetadataDocument CreateRaceSchema(string tableName, int order)
    {
        return new MetadataDocument
        {
            TableName = tableName,
            TypeName = "MetadataRaceDocument",
            DisplayName = "MetadataRaceDocument",
            Columns = new BsonArray([
                new BsonDocument()
                    .Set("n", "_id")
                    .Set("pn", "Id")
                    .Set("t", typeof(int).FullName ?? "System.Int32")
                    .Set("pk", true)
                    .Set("r", true)
                    .Set("o", 0),
                new BsonDocument()
                    .Set("n", "name")
                    .Set("pn", "Name")
                    .Set("t", typeof(string).FullName ?? "System.String")
                    .Set("r", false)
                    .Set("o", order + 1)
            ])
        };
    }

    private static int IndexOf(ReadOnlySpan<byte> haystack, ReadOnlySpan<byte> needle)
    {
        for (int i = 0; i <= haystack.Length - needle.Length; i++)
        {
            if (haystack.Slice(i, needle.Length).SequenceEqual(needle))
            {
                return i;
            }
        }

        return -1;
    }

    private static bool UpdateInTransaction(TinyDbEngine engine, int value, Barrier ready)
    {
        var collection = engine.GetCollection<TransactionDocument>();

        using var tx = engine.BeginTransaction();
        var document = collection.FindById(1)!;
        document.Value = value;
        collection.Update(document);

        ready.SignalAndWait(TimeSpan.FromSeconds(10));

        try
        {
            tx.Commit();
            return true;
        }
        catch (InvalidOperationException)
        {
            return false;
        }
    }

    private sealed class NonSeekStream : Stream
    {
        private readonly Stream _inner;
        private readonly bool _leaveOpen;

        public NonSeekStream(Stream inner, bool leaveOpen = false)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _leaveOpen = leaveOpen;
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => _inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => _inner.Read(buffer);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);

        protected override void Dispose(bool disposing)
        {
            if (disposing && !_leaveOpen)
            {
                _inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}

[Entity("AutoIdentityDocuments")]
public sealed class AutoIdentityDocument
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Entity("QueryObjectValueDocuments")]
public sealed class QueryObjectValueDocument
{
    public int Id { get; set; }
    public object? Value { get; set; }
}

[Entity("UniqueEmailDocuments")]
public sealed class UniqueEmailDocument
{
    public int Id { get; set; }

    [Index(Unique = true)]
    public string Email { get; set; } = string.Empty;
}

[Entity("TransactionDocuments")]
public sealed class TransactionDocument
{
    public int Id { get; set; }
    public int Value { get; set; }
}

[Entity]
public sealed class NumericIdOrderDocument
{
    public int Id { get; set; }
    public int Score { get; set; }
}

[Entity("IndexedValueDocuments")]
public sealed class IndexedValueDocument
{
    public int Id { get; set; }
    public int Score { get; set; }
}

[Entity("CompositeOrderDocuments")]
public sealed class CompositeOrderDocument
{
    public int Id { get; set; }
    public int A { get; set; }
    public int B { get; set; }
}

[Entity("QueryAggregateDocuments")]
public sealed class QueryAggregateDocument
{
    public int Id { get; set; }
    public string Category { get; set; } = string.Empty;
    public int Score { get; set; }
}

[Entity("CultureStringDocuments")]
public sealed class CultureStringDocument
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

[Entity("LargeIndexedDocuments")]
public sealed class LargeIndexedDocument
{
    public int Id { get; set; }
    public string Category { get; set; } = string.Empty;
    public string Payload { get; set; } = string.Empty;
}

[Entity("SelfReferencingCollectionDocuments")]
public sealed class SelfReferencingCollectionDocument
{
    public int Id { get; set; }
    public List<object?> Items { get; set; } = new();
}
