using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using ExpressionType = System.Linq.Expressions.ExpressionType;

namespace TinyDb.Tests.Regression;

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
        await Assert.That(() => new BsonSpanReader(hugeStringLength).ReadString()).Throws<EndOfStreamException>();

        var negativeBinaryLength = new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0x00 };
        await Assert.That(() => new BsonSpanReader(negativeBinaryLength).ReadBinary()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task DateTimeSerialization_ShouldNormalizeLocalValuesToUtc()
    {
        var local = DateTime.SpecifyKind(new DateTime(2024, 6, 1, 12, 30, 15, 123), DateTimeKind.Local);
        var bson = new BsonDateTime(local);

        var deserialized = (BsonDateTime)BsonSerializer.Deserialize(BsonSerializer.Serialize(bson));

        await Assert.That(deserialized.Value.Kind).IsEqualTo(DateTimeKind.Utc);
        await Assert.That(Math.Abs((deserialized.Value - local.ToUniversalTime()).TotalMilliseconds)).IsLessThan(1.0);
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

    private TinyDbEngine CreateEngine(string fileName)
    {
        return new TinyDbEngine(Path.Combine(_directory, fileName));
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
