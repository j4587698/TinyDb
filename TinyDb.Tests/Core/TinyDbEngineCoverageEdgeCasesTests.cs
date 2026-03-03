using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Core;
using TinyDb.Storage;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
[SkipInAot]
public sealed class TinyDbEngineCoverageEdgeCasesTests
{
    [Test]
    public async Task InsertDocuments_WhenCurrentWritablePageLoadFails_ShouldWrap()
    {
        var dbPath = CreateDbPath("engine_insert_wrap_sync");
        var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        try
        {
            var colName = "sync_wrap_col";
            engine.GetBsonCollection(colName).Insert(new BsonDocument().Set("_id", 1).Set("v", 1));

            var states = UnsafeAccessors.TinyDbEngineAccessor.CollectionStates(engine);
            var state = states[colName];
            if (state.PageState.PageId == 0)
            {
                state.PageState.PageId = state.OwnedPages.Keys.First();
            }

            UnsafeAccessors.TinyDbEngineAccessor.PageManager(engine).Dispose();

            await Assert.That(() => engine.InsertDocuments(
                    colName,
                    new[] { new BsonDocument().Set("_id", 2).Set("v", 2) }))
                .Throws<InvalidOperationException>();
        }
        finally
        {
            DisposeEngineSafely(engine);
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task InsertDocumentsAsync_WhenCurrentWritablePageLoadFails_ShouldWrap()
    {
        var dbPath = CreateDbPath("engine_insert_wrap_async");
        var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        try
        {
            var colName = "async_wrap_col";
            engine.GetBsonCollection(colName).Insert(new BsonDocument().Set("_id", 1).Set("v", 1));

            var states = UnsafeAccessors.TinyDbEngineAccessor.CollectionStates(engine);
            var state = states[colName];
            if (state.PageState.PageId == 0)
            {
                state.PageState.PageId = state.OwnedPages.Keys.First();
            }

            UnsafeAccessors.TinyDbEngineAccessor.PageManager(engine).Dispose();

            await Assert.That(async () =>
                    await engine.InsertDocumentsAsync(
                        colName,
                        new[] { new BsonDocument().Set("_id", 2).Set("v", 2) }))
                .Throws<InvalidOperationException>();
        }
        finally
        {
            DisposeEngineSafely(engine);
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task BuildDocumentLocationCache_WhenPageLoadFails_ShouldWrap()
    {
        var dbPath = CreateDbPath("engine_build_loc_cache_wrap");
        var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        try
        {
            var colName = "rebuild_col";
            engine.GetBsonCollection(colName).Insert(new BsonDocument().Set("_id", 1).Set("v", 1));

            var states = UnsafeAccessors.TinyDbEngineAccessor.CollectionStates(engine);
            var state = states[colName];

            UnsafeAccessors.TinyDbEngineAccessor.PageManager(engine).Dispose();

            var method = typeof(TinyDbEngine).GetMethod("BuildDocumentLocationCache", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new MissingMethodException(typeof(TinyDbEngine).FullName, "BuildDocumentLocationCache");

            await Assert.That(() => InvokeAndUnwrap(method, engine, colName, state))
                .Throws<InvalidOperationException>();
        }
        finally
        {
            DisposeEngineSafely(engine);
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task StreamRawScanResultPages_ShouldCoverEmptyAndRetryFailureBranches()
    {
        var dbPath = CreateDbPath("engine_raw_scan_state_machine");
        var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        try
        {
            var states = UnsafeAccessors.TinyDbEngineAccessor.CollectionStates(engine);
            var pageManager = UnsafeAccessors.TinyDbEngineAccessor.PageManager(engine);

            var emptyCol = "raw_empty_col";
            engine.GetBsonCollection(emptyCol).Insert(new BsonDocument().Set("_id", 1).Set("v", 1));
            var emptyState = states[emptyCol];
            var emptyPageId = emptyState.OwnedPages.Keys.First();
            var emptyPage = pageManager.GetPage(emptyPageId);
            emptyPage.ClearData();
            emptyPage.UpdatePageType(PageType.Empty);
            emptyPage.Header.ItemCount = 0;
            emptyPage.UpdateChecksum();
            pageManager.SavePage(emptyPage, true);

            var emptyResult = engine.FindAllRawWithPredicateInfo(emptyCol).ToList();
            await Assert.That(emptyResult.Count).IsEqualTo(0);

            var disposedCol = "raw_disposed_col";
            engine.GetBsonCollection(disposedCol).Insert(new BsonDocument().Set("_id", 2).Set("v", 2));
            var disposedState = states[disposedCol];
            var disposedPageId = disposedState.OwnedPages.Keys.First();
            var poisonedPage = pageManager.GetPage(disposedPageId);
            poisonedPage.Dispose();

            await Assert.That(() => engine.FindAllRawWithPredicateInfo(disposedCol).ToList())
                .Throws<InvalidOperationException>();
        }
        finally
        {
            DisposeEngineSafely(engine);
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task ReadRawDocumentSnapshot_ShouldEnumerateIteratorToCompletion()
    {
        var dbPath = CreateDbPath("engine_raw_snapshot_iter");
        using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        try
        {
            var colName = "raw_snapshot_col";
            engine.GetBsonCollection(colName).Insert(new BsonDocument().Set("_id", 1).Set("v", 1));

            var getState = typeof(TinyDbEngine).GetMethod("GetCollectionState", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new MissingMethodException(typeof(TinyDbEngine).FullName, "GetCollectionState");
            var state = getState.Invoke(engine, new object[] { colName })!;

            var readRawSnapshot = typeof(TinyDbEngine).GetMethod("ReadRawDocumentSnapshot", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new MissingMethodException(typeof(TinyDbEngine).FullName, "ReadRawDocumentSnapshot");
            var enumerable = (IEnumerable<ReadOnlyMemory<byte>>)readRawSnapshot.Invoke(engine, new object?[] { colName, state, null })!;

            var all = enumerable.ToList();
            await Assert.That(all.Count).IsGreaterThan(0);
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task ResolveLargeDocumentAsync_ShouldCoverLargeAndPassthroughBranches()
    {
        var dbPath = CreateDbPath("engine_resolve_large_async");
        using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        try
        {
            var storageField = typeof(TinyDbEngine).GetField("_largeDocumentStorage", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new MissingFieldException(typeof(TinyDbEngine).FullName, "_largeDocumentStorage");

            var storage = (LargeDocumentStorage)storageField.GetValue(engine)!;
            var original = new BsonDocument().Set("_id", 10).Set("name", "large");
            var indexPageId = storage.StoreLargeDocument(original, "resolve_async_col");

            var stub = new BsonDocument()
                .Set("_id", 10)
                .Set("_isLargeDocument", true)
                .Set("_largeDocumentIndex", (long)indexPageId);

            var resolved = await engine.ResolveLargeDocumentAsync(stub);
            await Assert.That(resolved["name"].ToString()).IsEqualTo("large");

            var plain = new BsonDocument().Set("_id", 11).Set("v", 123);
            var passthrough = await engine.ResolveLargeDocumentAsync(plain);
            await Assert.That(passthrough["v"].ToInt32()).IsEqualTo(123);
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task Log_ShouldRouteToConfiguredLogger()
    {
        var dbPath = CreateDbPath("engine_log_wrapper");
        try
        {
            TinyDbLogLevel? level = null;
            string? message = null;
            Exception? error = null;

            using var engine = new TinyDbEngine(
                dbPath,
                new TinyDbOptions
                {
                    EnableJournaling = false,
                    Logger = (l, m, ex) =>
                    {
                        level = l;
                        message = m;
                        error = ex;
                    }
                });

            var expected = new InvalidOperationException("engine-log");
            engine.Log(TinyDbLogLevel.Information, "engine-message", expected);

            await Assert.That(level).IsEqualTo(TinyDbLogLevel.Information);
            await Assert.That(message).IsEqualTo("engine-message");
            await Assert.That(object.ReferenceEquals(error, expected)).IsTrue();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task Dispose_WhenCleanupFails_ShouldThrowAggregateWithCleanupBranch()
    {
        var dbPath = CreateDbPath("engine_dispose_cleanup_branch");
        var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        try
        {
            var collectionsField = typeof(TinyDbEngine).GetField("_collections", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new MissingFieldException(typeof(TinyDbEngine).FullName, "_collections");

            var collections = (ConcurrentDictionary<string, IDocumentCollection>)collectionsField.GetValue(engine)!;
            collections["throwing_collection"] = new ThrowingDocumentCollection();

            await Assert.That(() => engine.Dispose()).Throws<AggregateException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task Dispose_WhenOnlyFlushFails_ShouldRethrowFlushException()
    {
        var dbPath = CreateDbPath("engine_dispose_flush_branch");
        var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        try
        {
            var diskStreamField = typeof(TinyDbEngine).GetField("_diskStream", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new MissingFieldException(typeof(TinyDbEngine).FullName, "_diskStream");
            var innerDiskStream = (IDiskStream)diskStreamField.GetValue(engine)!;
            diskStreamField.SetValue(engine, new FlushFailingDiskStream(innerDiskStream));

            await Assert.That(() => engine.Dispose()).Throws<InvalidOperationException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    private static object? InvokeAndUnwrap(MethodInfo method, params object?[] args)
    {
        try
        {
            return method.Invoke(args[0], args.Skip(1).ToArray());
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw ex.InnerException;
        }
    }

    private static string CreateDbPath(string tag)
    {
        return Path.Combine(Path.GetTempPath(), $"{tag}_{Guid.NewGuid():N}.db");
    }

    private static void CleanupDb(string dbPath)
    {
        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }

        var directory = Path.GetDirectoryName(dbPath) ?? string.Empty;
        var name = Path.GetFileNameWithoutExtension(dbPath);
        var ext = Path.GetExtension(dbPath).TrimStart('.');
        var walPath = Path.Combine(directory, $"{name}-wal.{ext}");
        try { if (File.Exists(walPath)) File.Delete(walPath); } catch { }
    }

    private static void DisposeEngineSafely(TinyDbEngine engine)
    {
        try
        {
            engine.Dispose();
        }
        catch
        {
            // Ignore cleanup failures in tests that intentionally corrupt engine internals.
        }
    }

    private sealed class ThrowingDocumentCollection : IDocumentCollection
    {
        public string Name => "throwing_collection";
        public Type DocumentType => typeof(BsonDocument);
        public int DeleteAll() => 0;

        public void Dispose()
        {
            throw new InvalidOperationException("Simulated collection dispose failure.");
        }
    }

    private sealed class FlushFailingDiskStream : IDiskStream
    {
        private readonly IDiskStream _inner;

        public FlushFailingDiskStream(IDiskStream inner)
        {
            _inner = inner;
        }

        public string FilePath => _inner.FilePath;
        public long Size => _inner.Size;
        public bool IsReadable => _inner.IsReadable;
        public bool IsWritable => _inner.IsWritable;

        public byte[] ReadPage(long pageOffset, int pageSize) => _inner.ReadPage(pageOffset, pageSize);
        public void WritePage(long pageOffset, byte[] pageData) => _inner.WritePage(pageOffset, pageData);
        public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default) => _inner.ReadPageAsync(pageOffset, pageSize, cancellationToken);
        public Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default) => _inner.WritePageAsync(pageOffset, pageData, cancellationToken);
        public void Flush() => throw new InvalidOperationException("Simulated flush failure from wrapper stream.");
        public Task FlushAsync(CancellationToken cancellationToken = default) => _inner.FlushAsync(cancellationToken);
        public void SetLength(long length) => _inner.SetLength(length);
        public DiskStreamStatistics GetStatistics() => _inner.GetStatistics();
        public void Dispose() => _inner.Dispose();
    }
}
