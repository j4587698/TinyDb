using System;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public sealed class TinyDbEngineAdditionalCoverageTests
{
    private sealed class NullToStringBsonValue : BsonValue
    {
        public override BsonType BsonType => BsonType.Int32;
        public override object? RawValue => null;
        public override int CompareTo(BsonValue? other) => 0;
        public override bool Equals(BsonValue? other) => ReferenceEquals(this, other);
        public override int GetHashCode() => 0;
        public override string ToString() => null!;
        public override TypeCode GetTypeCode() => TypeCode.Empty;
        public override bool ToBoolean(IFormatProvider? provider) => false;
        public override byte ToByte(IFormatProvider? provider) => 0;
        public override char ToChar(IFormatProvider? provider) => '\0';
        public override DateTime ToDateTime(IFormatProvider? provider) => default;
        public override decimal ToDecimal(IFormatProvider? provider) => 0m;
        public override double ToDouble(IFormatProvider? provider) => 0.0;
        public override short ToInt16(IFormatProvider? provider) => 0;
        public override int ToInt32(IFormatProvider? provider) => 0;
        public override long ToInt64(IFormatProvider? provider) => 0L;
        public override sbyte ToSByte(IFormatProvider? provider) => 0;
        public override float ToSingle(IFormatProvider? provider) => 0.0f;
        public override string ToString(IFormatProvider? provider) => null!;
        public override object ToType(Type conversionType, IFormatProvider? provider) => throw new NotSupportedException();
        public override ushort ToUInt16(IFormatProvider? provider) => 0;
        public override uint ToUInt32(IFormatProvider? provider) => 0u;
        public override ulong ToUInt64(IFormatProvider? provider) => 0ul;
    }

    [Test]
    public async Task CheckpointAsync_ShouldNotThrow()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"engine_checkpoint_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions
            {
                EnableJournaling = true,
                BackgroundFlushInterval = Timeout.InfiniteTimeSpan
            });

            var col = engine.GetCollection<BsonDocument>("c");
            col.Insert(new BsonDocument().Set("x", 1));

            await engine.CheckpointAsync();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task FindAll_ShouldSkipNonDataAndEmptyPages()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"engine_findall_sync_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = engine.GetCollection<BsonDocument>("c");
            col.Insert(new BsonDocument().Set("x", 1));

            var collectionStatesField = typeof(TinyDbEngine).GetField("_collectionStates", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(collectionStatesField).IsNotNull();

            var collectionStates = collectionStatesField!.GetValue(engine)!;
            var tryGetValue = collectionStates.GetType().GetMethod("TryGetValue");
            await Assert.That(tryGetValue).IsNotNull();

            var args = new object?[] { "c", null };
            var found = (bool)tryGetValue!.Invoke(collectionStates, args)!;
            await Assert.That(found).IsTrue();
            var stateObj = args[1];

            var ownedPagesProp = stateObj!.GetType().GetProperty("OwnedPages", BindingFlags.Public | BindingFlags.Instance);
            await Assert.That(ownedPagesProp).IsNotNull();

            var ownedPages = (ConcurrentDictionary<uint, byte>)ownedPagesProp!.GetValue(stateObj)!;

            var pageManagerField = typeof(TinyDbEngine).GetField("_pageManager", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(pageManagerField).IsNotNull();

            var pm = (PageManager)pageManagerField!.GetValue(engine)!;

            var indexPage = pm.NewPage(PageType.Index);
            pm.SavePage(indexPage, forceFlush: true);
            ownedPages.TryAdd(indexPage.PageID, 0);

            var emptyDataPage = pm.NewPage(PageType.Data);
            pm.SavePage(emptyDataPage, forceFlush: true);
            ownedPages.TryAdd(emptyDataPage.PageID, 0);

            var docs = new List<BsonDocument>(engine.FindAll("c"));
            await Assert.That(docs.Count).IsGreaterThan(0);
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task PrepareDocumentForInsert_ShouldCoverHasCorrectColBranches()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"engine_prepare_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });

            var method = typeof(TinyDbEngine).GetMethod("PrepareDocumentForInsert", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(method).IsNotNull();

            foreach (var doc in new[]
            {
                new BsonDocument().Set("_id", 1).Set("_collection", "c"),
                new BsonDocument().Set("_id", 2),
                new BsonDocument().Set("_id", 3).Set("_collection", "other"),
                new BsonDocument().Set("_id", 4).Set("_collection", (BsonValue)null!)
            })
            {
                var args = new object?[] { "c", doc, null };
                _ = (BsonDocument)method!.Invoke(engine, args)!;
                await Assert.That(args[2]).IsNotNull();
            }
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task FindAll_WhenTransactionOperationsHaveNullKeys_ShouldCoverMergeBranches()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"engine_merge_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            using var tx = (Transaction)engine.BeginTransaction();

            tx.Operations.Add(new TransactionOperation(
                TransactionOperationType.CreateIndex,
                "c",
                indexName: "i",
                indexFields: new[] { "x" },
                indexUnique: true));
            tx.Operations.Add(new TransactionOperation(
                TransactionOperationType.Delete,
                "c",
                documentId: new NullToStringBsonValue()));
            tx.Operations.Add(new TransactionOperation(
                TransactionOperationType.Delete,
                "c",
                documentId: new BsonInt32(1)));

            _ = new List<BsonDocument>(engine.FindAll("c"));
            await Assert.That(tx.Operations.Count).IsEqualTo(3);
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task BeginTransaction_WhenNotInitialized_ShouldThrowInvalidOperationException()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"engine_initflag_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });

            var isInitializedField = typeof(TinyDbEngine).GetField("_isInitialized", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(isInitializedField).IsNotNull();

            var original = (bool)isInitializedField!.GetValue(engine)!;
            isInitializedField.SetValue(engine, false);
            try
            {
                await Assert.That(() => engine.BeginTransaction()).Throws<InvalidOperationException>();
            }
            finally
            {
                isInitializedField.SetValue(engine, original);
            }
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task GetCollectionNames_WhenDisposed_ShouldThrowObjectDisposedException()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"engine_disposed_{Guid.NewGuid():N}.db");

        try
        {
            var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            engine.Dispose();

            await Assert.That(() => engine.GetCollectionNames()).Throws<ObjectDisposedException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task GetCollectionMetadata_Internal_ShouldReturnDocument()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"engine_meta_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });

            var method = typeof(TinyDbEngine).GetMethod("GetCollectionMetadata", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(method).IsNotNull();

            var metadata = (BsonDocument)method!.Invoke(engine, new object[] { "missing" })!;
            await Assert.That(metadata).IsNotNull();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task FindAllAsync_ShouldSkipNonDataAndEmptyPages_AndSwallowPageErrors()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"engine_findall_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = engine.GetCollection<BsonDocument>("c");

            var inserted = new BsonDocument().Set("x", 1);
            col.Insert(inserted);

            var collectionStatesField = typeof(TinyDbEngine).GetField("_collectionStates", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(collectionStatesField).IsNotNull();

            var collectionStates = collectionStatesField!.GetValue(engine)!;
            var tryGetValue = collectionStates.GetType().GetMethod("TryGetValue");
            await Assert.That(tryGetValue).IsNotNull();

            var args = new object?[] { "c", null };
            var found = (bool)tryGetValue!.Invoke(collectionStates, args)!;
            await Assert.That(found).IsTrue();
            var stateObj = args[1];

            var ownedPagesProp = stateObj!.GetType().GetProperty("OwnedPages", BindingFlags.Public | BindingFlags.Instance);
            await Assert.That(ownedPagesProp).IsNotNull();

            var ownedPages = (ConcurrentDictionary<uint, byte>)ownedPagesProp!.GetValue(stateObj)!;

            ownedPages.TryAdd(0, 0);
            ownedPages.TryAdd(1, 0);

            var pageManagerField = typeof(TinyDbEngine).GetField("_pageManager", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(pageManagerField).IsNotNull();

            var pm = (PageManager)pageManagerField!.GetValue(engine)!;
            var emptyDataPage = pm.NewPage(PageType.Data);
            pm.SavePage(emptyDataPage, forceFlush: true);
            ownedPages.TryAdd(emptyDataPage.PageID, 0);

            var docs = await col.FindAllAsync();
            await Assert.That(docs.Count).IsGreaterThan(0);
            await Assert.That(docs.Exists(d => d.TryGetValue("x", out var v) && v.ToInt32(null) == 1)).IsTrue();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task FindAllAsync_WhenLargeDocumentStored_ShouldResolveLargeDocumentAsync()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"engine_large_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = engine.GetCollection<BsonDocument>("c");

            col.Insert(new BsonDocument().Set("x", 1).Set("_isLargeDocument", false));

            var big = new string('a', 20_000);
            col.Insert(new BsonDocument().Set("name", "big").Set("payload", big));

            var docs = await col.FindAllAsync();
            await Assert.That(docs.Exists(d => d.TryGetValue("name", out var n) && n.ToString() == "big")).IsTrue();
            await Assert.That(docs.Exists(d => d.TryGetValue("payload", out var p) && p.ToString() == big)).IsTrue();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task InitializeDatabase_WhenWalHasNewerPages_ShouldReplayAndRestore()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"engine_replay_{Guid.NewGuid():N}.db");

        try
        {
            using (var engine = new TinyDbEngine(dbPath, new TinyDbOptions
            {
                EnableJournaling = false,
                BackgroundFlushInterval = Timeout.InfiniteTimeSpan
            }))
            {
                var col = engine.GetCollection<BsonDocument>("c");
                col.Insert(new BsonDocument().Set("x", 1));
            }

            var pageSize = (int)TinyDbOptions.DefaultPageSize;
            var fileSize = new FileInfo(dbPath).Length;
            var totalPages = (uint)(fileSize / pageSize);
            await Assert.That(totalPages).IsGreaterThanOrEqualTo(2u);

            var page1Bytes = ReadPageBytes(dbPath, pageSize, 1);
            var page2Bytes = ReadPageBytes(dbPath, pageSize, 2);

            var newPageId = totalPages + 1;
            var wal = new WriteAheadLog(dbPath, pageSize, true);
            try
            {
                wal.AppendPage(new Page(1, page1Bytes));
                wal.AppendPage(new Page(2, page2Bytes));
                wal.AppendPage(new Page(newPageId, pageSize, PageType.Data));
            }
            finally
            {
                wal.Dispose();
            }

            using var replayed = new TinyDbEngine(dbPath, new TinyDbOptions
            {
                EnableJournaling = true,
                BackgroundFlushInterval = Timeout.InfiniteTimeSpan
            });
            await Assert.That(replayed.IsInitialized).IsTrue();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    private static byte[] ReadPageBytes(string dbPath, int pageSize, uint pageId)
    {
        var buffer = new byte[pageSize];
        using var fs = new FileStream(dbPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        fs.Seek((long)(pageId - 1) * pageSize, SeekOrigin.Begin);
        var read = fs.Read(buffer, 0, buffer.Length);
        if (read != buffer.Length) throw new InvalidOperationException("Failed to read full page.");
        return buffer;
    }

    private static void CleanupDb(string dbPath)
    {
        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }

        var walFile = Path.Combine(Path.GetDirectoryName(dbPath)!, $"{Path.GetFileNameWithoutExtension(dbPath)}-wal.db");
        try { if (File.Exists(walFile)) File.Delete(walFile); } catch { }
    }
}
