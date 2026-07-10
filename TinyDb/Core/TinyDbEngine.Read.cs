using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Security;
using TinyDb.Collections;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Attributes;
using TinyDb.Utils;

namespace TinyDb.Core;

public sealed partial class TinyDbEngine
{
    internal IEnumerable<BsonDocument> FindAll(string col)
    {
        return FindAllWithCommitGate(col);
    }

    private IEnumerable<BsonDocument> FindAllWithCommitGate(string col)
    {
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });
        var st = GetCollectionState(col);
        var ds = ReadAllDocumentsSnapshotFromPageSnapshots(col, st);
        var tx = GetCurrentTransaction();
        // 即使 ds 为空，也需要合并事务挂起操作
        var documents = tx != null ? MergeTransactionOperations(col, ds, tx) : ds;
        foreach (var document in documents)
        {
            yield return document;
        }
    }

    internal IEnumerable<BsonValue> FindAllIds(string col)
    {
        return FindAllIdsWithCommitGate(col);
    }

    private IEnumerable<BsonValue> FindAllIdsWithCommitGate(string col)
    {
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });
        var st = GetCollectionState(col);
        var tx = GetCurrentTransaction();
        IEnumerable<BsonValue> ids;
        if (tx != null)
        {
            ids = ReadIdsFromDocuments(MergeTransactionOperations(col, ReadAllDocumentsSnapshotFromPageSnapshots(col, st), tx));
        }
        else
        {
            ids = ReadIdsFromRawDocuments(col, st);
        }

        foreach (var id in ids)
        {
            yield return id;
        }
    }

    private IEnumerable<BsonValue> ReadIdsFromRawDocuments(string col, CollectionState st)
    {
        foreach (var result in StreamRawScanResultPages(col, st, null))
        {
            var document = result.Slice.Span;
            if (BsonScanner.TryGetValue(document, "_collection", out var collectionValue) &&
                collectionValue?.ToString() != col)
            {
                continue;
            }

            if (BsonScanner.TryGetValue(document, "_id", out var id) && id != null && !id.IsNull)
            {
                yield return id;
            }
        }
    }

    private static IEnumerable<BsonValue> ReadIdsFromDocuments(IEnumerable<BsonDocument> documents)
    {
        foreach (var document in documents)
        {
            if (document.TryGetValue("_id", out var id) && id != null && !id.IsNull)
            {
                yield return id;
            }
        }
    }

    internal async Task<List<BsonDocument>> FindAllAsync(string col, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var collectionCommitGate = await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false);

        var st = GetCollectionState(col);
        var ds = await ReadAllDocumentsSnapshotAsync(col, st, cancellationToken).ConfigureAwait(false);
        var tx = GetCurrentTransaction();
        var result = tx != null ? MergeTransactionOperations(col, ds, tx).ToList() : ds;
        return result;
    }

    /// <summary>
    /// 获取集合中所有文档的原始数据快照，支持原地谓词下推。
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="predicates">扫描谓词（可选）</param>
    /// <returns>文档原始数据的枚举</returns>
    internal IEnumerable<ReadOnlyMemory<byte>> FindAllRaw(string col, ScanPredicate[]? predicates = null)
    {
        return FindAllRawWithCommitGate(col, predicates);
    }

    private IEnumerable<ReadOnlyMemory<byte>> FindAllRawWithCommitGate(string col, ScanPredicate[]? predicates)
    {
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });
        var st = GetCollectionState(col);
        foreach (var slice in ReadRawDocumentSnapshot(col, st, predicates))
        {
            yield return slice;
        }
    }

    internal IEnumerable<RawScanResult> FindAllRawWithPredicateInfo(string col, ScanPredicate[]? predicates = null)
    {
        return FindAllRawWithPredicateInfoWithCommitGate(col, predicates);
    }

    private IEnumerable<RawScanResult> FindAllRawWithPredicateInfoWithCommitGate(string col, ScanPredicate[]? predicates)
    {
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });
        var st = GetCollectionState(col);
        foreach (var result in ReadRawScanResultSnapshot(col, st, predicates))
        {
            yield return result;
        }
    }

    internal async IAsyncEnumerable<ReadOnlyMemory<byte>> FindAllRawAsync(
        string col,
        ScanPredicate[]? predicates = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var result in FindAllRawWithPredicateInfoAsync(col, predicates, cancellationToken).ConfigureAwait(false))
        {
            yield return result.Slice;
        }
    }

    internal async IAsyncEnumerable<RawScanResult> FindAllRawWithPredicateInfoAsync(
        string col,
        ScanPredicate[]? predicates = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var collectionCommitGate = await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false);
        await foreach (var result in FindAllRawWithPredicateInfoNoGateAsync(col, predicates, cancellationToken).ConfigureAwait(false))
        {
            yield return result;
        }
    }

    private async IAsyncEnumerable<RawScanResult> FindAllRawWithPredicateInfoNoGateAsync(
        string col,
        ScanPredicate[]? predicates = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var st = GetCollectionState(col);
        await foreach (var result in StreamRawScanResultPagesAsync(col, st, predicates, cancellationToken).ConfigureAwait(false))
        {
            yield return result;
        }
    }

    internal BsonDocument? FindById(string col, BsonValue id)
    {
        var tx = GetCurrentTransaction();
        if (tx != null && TryGetTransactionDocument(tx, col, id, out var transactionDocument))
        {
            return transactionDocument;
        }

        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });
        return FindCommittedById(col, id);
    }

    internal List<BsonDocument?> FindByIds(string col, IReadOnlyList<BsonValue> ids)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));
        if (ids.Count == 0) return new List<BsonDocument?>();

        var tx = GetCurrentTransaction();
        if (tx == null)
        {
            using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });
            return FindCommittedByIds(col, ids);
        }

        var results = new BsonDocument?[ids.Count];
        var committedIds = new List<BsonValue>(ids.Count);
        var committedOrdinals = new List<int>(ids.Count);

        var operations = tx.GetOperationsSnapshot();
        for (int i = 0; i < ids.Count; i++)
        {
            if (TryGetTransactionDocument(operations, col, ids[i], out var transactionDocument))
            {
                results[i] = transactionDocument;
                continue;
            }

            committedOrdinals.Add(i);
            committedIds.Add(ids[i]);
        }

        if (committedIds.Count > 0)
        {
            List<BsonDocument?> committedDocuments;
            using (EnterCollectionWriteGates(new[] { col }))
            {
                committedDocuments = FindCommittedByIds(col, committedIds);
            }

            for (int i = 0; i < committedDocuments.Count; i++)
            {
                results[committedOrdinals[i]] = committedDocuments[i];
            }
        }

        return results.ToList();
    }

    internal BsonDocument? FindCommittedById(string col, BsonValue id)
    {
        var st = GetCollectionState(col);
        DocumentLocation? indexedLocation = null;
        if (st.Index.TryGet(id, out var loc))
        {
            indexedLocation = loc;
        }

        if (indexedLocation is { } location)
        {
            var p = _pageManager.GetPage(location.PageId);
            BsonDocument? document = null;

            using (st.EnterPageMutationLock(location.PageId))
            {
                if (p.PageType == PageType.Data && location.EntryIndex < p.Header.ItemCount)
                {
                    var entry = _dataPageAccess.ReadDocumentAt(p, location.EntryIndex);
                    document = entry?.Document;
                }
            }

            if (document != null &&
                document.TryGetValue("_id", out var documentId) &&
                BsonValuesEqual(documentId, id) &&
                (!document.TryGetValue("_collection", out var collectionValue) || collectionValue.ToString() == col))
            {
                return ResolveLargeDocument(document);
            }

            RecordFindByIdStaleIndexHit(col, id);
        }

        return FindByIdFullScan(col, id, st);
    }

    internal List<BsonDocument?> FindCommittedByIds(string col, IReadOnlyList<BsonValue> ids)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var st = GetCollectionState(col);
        var results = new BsonDocument?[ids.Count];
        var indexHits = new bool[ids.Count];
        var pageLookups = new Dictionary<uint, List<(BsonValue Id, int Ordinal, DocumentLocation Location)>>();

        for (int i = 0; i < ids.Count; i++)
        {
            if (st.Index.TryGet(ids[i], out var location))
            {
                indexHits[i] = true;
                if (!pageLookups.TryGetValue(location.PageId, out var lookups))
                {
                    lookups = new List<(BsonValue Id, int Ordinal, DocumentLocation Location)>();
                    pageLookups.Add(location.PageId, lookups);
                }

                lookups.Add((ids[i], i, location));
            }
        }

        foreach (var (pageId, lookups) in pageLookups)
        {
            var page = _pageManager.GetPage(pageId);

            using (st.EnterPageMutationLock(pageId))
            {
                ReadCommittedPageLookups(col, page, lookups, results);
            }
        }

        for (int i = 0; i < results.Length; i++)
        {
            if (results[i] == null && indexHits[i])
            {
                RecordFindByIdStaleIndexHit(col, ids[i]);
            }

            results[i] ??= FindByIdFullScan(col, ids[i], st);
            if (results[i] != null)
            {
                results[i] = ResolveLargeDocument(results[i]!);
            }
        }

        return results.ToList();
    }

    internal async Task<BsonDocument?> FindByIdAsync(string col, BsonValue id, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var tx = GetCurrentTransaction();
        if (tx != null && TryGetTransactionDocument(tx, col, id, out var transactionDocument))
        {
            return transactionDocument;
        }

        using var collectionCommitGate = await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false);
        var st = GetCollectionState(col);
        DocumentLocation? indexedLocation = null;
        if (st.Index.TryGet(id, out var loc))
        {
            indexedLocation = loc;
        }

        if (indexedLocation is { } location)
        {
            var indexedDocument = await TryReadCommittedByLocationAsync(col, id, st, location, cancellationToken).ConfigureAwait(false);
            if (indexedDocument != null) return indexedDocument;
            RecordFindByIdStaleIndexHit(col, id);
        }

        return await FindByIdFullScanAsync(col, id, cancellationToken).ConfigureAwait(false);
    }

    internal async Task<List<BsonDocument?>> FindByIdsAsync(
        string col,
        IReadOnlyList<BsonValue> ids,
        CancellationToken cancellationToken = default)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));
        cancellationToken.ThrowIfCancellationRequested();
        if (ids.Count == 0) return new List<BsonDocument?>();

        var tx = GetCurrentTransaction();
        if (tx == null)
        {
            using var collectionCommitGate = await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false);
            return await FindCommittedByIdsAsync(col, ids, cancellationToken).ConfigureAwait(false);
        }

        var results = new BsonDocument?[ids.Count];
        var committedIds = new List<BsonValue>(ids.Count);
        var committedOrdinals = new List<int>(ids.Count);

        var operations = tx.GetOperationsSnapshot();
        for (int i = 0; i < ids.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (TryGetTransactionDocument(operations, col, ids[i], out var transactionDocument))
            {
                results[i] = transactionDocument;
                continue;
            }

            committedOrdinals.Add(i);
            committedIds.Add(ids[i]);
        }

        if (committedIds.Count > 0)
        {
            List<BsonDocument?> committedDocuments;
            using (await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false))
            {
                committedDocuments = await FindCommittedByIdsAsync(col, committedIds, cancellationToken).ConfigureAwait(false);
            }

            for (int i = 0; i < committedDocuments.Count; i++)
            {
                results[committedOrdinals[i]] = committedDocuments[i];
            }
        }

        return results.ToList();
    }

    internal async Task<List<BsonDocument?>> FindCommittedByIdsAsync(
        string col,
        IReadOnlyList<BsonValue> ids,
        CancellationToken cancellationToken = default)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));
        cancellationToken.ThrowIfCancellationRequested();

        var st = GetCollectionState(col);
        var results = new BsonDocument?[ids.Count];
        var indexHits = new bool[ids.Count];
        var pageLookups = new Dictionary<uint, List<(BsonValue Id, int Ordinal, DocumentLocation Location)>>();

        for (int i = 0; i < ids.Count; i++)
        {
            if (st.Index.TryGet(ids[i], out var location))
            {
                indexHits[i] = true;
                if (!pageLookups.TryGetValue(location.PageId, out var lookups))
                {
                    lookups = new List<(BsonValue Id, int Ordinal, DocumentLocation Location)>();
                    pageLookups.Add(location.PageId, lookups);
                }

                lookups.Add((ids[i], i, location));
            }
        }

        foreach (var (pageId, lookups) in pageLookups)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var page = await _pageManager.GetPageAsync(pageId, cancellationToken: cancellationToken).ConfigureAwait(false);

            using (st.EnterPageMutationLock(pageId))
            {
                ReadCommittedPageLookups(col, page, lookups, results);
            }
        }

        for (int i = 0; i < results.Length; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (results[i] == null && indexHits[i])
            {
                RecordFindByIdStaleIndexHit(col, ids[i]);
            }

            results[i] ??= await FindByIdFullScanAsync(col, ids[i], cancellationToken).ConfigureAwait(false);
            if (results[i] != null)
            {
                results[i] = await ResolveLargeDocumentAsync(results[i]!, cancellationToken).ConfigureAwait(false);
            }
        }

        return results.ToList();
    }

    private async Task<BsonDocument?> FindByIdFullScanAsync(string col, BsonValue id, CancellationToken cancellationToken = default)
    {
        RecordFindByIdFullScan();

        var idPredicate = new[]
        {
            new ScanPredicate(
                Encoding.UTF8.GetBytes("_id"),
                Encoding.UTF8.GetBytes("id"),
                Encoding.UTF8.GetBytes("Id"),
                id,
                ExpressionType.Equal)
        };

        await foreach (var result in FindAllRawWithPredicateInfoNoGateAsync(col, idPredicate, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var doc = DeserializeDocumentOrThrow(result.Slice);
            if (doc.TryGetValue("_collection", out var c) && c.ToString() != col)
            {
                continue;
            }

            doc = await ResolveLargeDocumentAsync(doc, cancellationToken).ConfigureAwait(false);
            if (doc.TryGetValue("_id", out var documentId) && BsonValuesEqual(documentId, id))
            {
                RecordFindByIdFullScanHit(col, id);
                return doc;
            }
        }

        return null;
    }

    private IEnumerable<BsonDocument> ReadAllDocumentsSnapshot(string col, CollectionState st, CancellationToken cancellationToken = default)
    {
        return ReadAllDocumentsSnapshotFromPageSnapshots(col, st, cancellationToken);
    }

    private IEnumerable<BsonDocument> ReadAllDocumentsSnapshotFromPageSnapshots(string col, CollectionState st, CancellationToken cancellationToken = default)
    {
        foreach (var result in StreamRawScanResultPages(col, st, null))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var doc = DeserializeDocumentOrThrow(result.Slice);
            if (doc.TryGetValue("_collection", out var c) && c.ToString() != col) continue;
            yield return ResolveLargeDocument(doc);
        }
    }

    private async Task<List<BsonDocument>> ReadAllDocumentsSnapshotAsync(string col, CollectionState st, CancellationToken cancellationToken = default)
    {
        var ds = new List<BsonDocument>();

        await foreach (var result in StreamRawScanResultPagesAsync(col, st, null, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var doc = DeserializeDocumentOrThrow(result.Slice);
            if (doc.TryGetValue("_collection", out var c) && c.ToString() != col)
            {
                continue;
            }

            ds.Add(await ResolveLargeDocumentAsync(doc, cancellationToken).ConfigureAwait(false));
        }

        return ds;
    }

    internal IEnumerable<ReadOnlyMemory<byte>> ReadRawDocumentSnapshot(string col, CollectionState st, ScanPredicate[]? predicates)
    {
        foreach (var result in StreamRawScanResultPages(col, st, predicates))
        {
            yield return result.Slice;
        }
    }

    private IEnumerable<RawScanResult> ReadRawScanResultSnapshot(string col, CollectionState st, ScanPredicate[]? predicates)
    {
        foreach (var result in StreamRawScanResultPages(col, st, predicates))
        {
            yield return result;
        }
    }

    private IEnumerable<RawScanResult> StreamRawScanResultPages(string col, CollectionState st, ScanPredicate[]? predicates)
    {
        var pages = new List<uint>(st.OwnedPages.Count);
        foreach (var page in st.OwnedPages)
        {
            pages.Add(page.Key);
        }

        pages.Sort();

        foreach (var pageId in pages)
        {
            byte[]? pageSnapshot = null;
            int itemCount = 0;
            int endOffset = 0;
            Exception? lastError = null;

            for (int attempt = 0; attempt < 2; attempt++)
            {
                try
                {
                    var p = _pageManager.GetPage(pageId);

                    using (st.EnterPageMutationLock(pageId))
                    {
                        if (p.PageType != PageType.Data || p.Header.ItemCount == 0)
                        {
                            pageSnapshot = null;
                            itemCount = 0;
                            endOffset = 0;
                            break;
                        }

                        // 为当前页创建稳定快照：每页只复制一次，避免逐文档 ToArray 带来的分配放大。
                        p.Pin();
                        try
                        {
                            itemCount = p.Header.ItemCount;
                            pageSnapshot = p.Snapshot(includeUnusedTail: false);
                            endOffset = pageSnapshot.Length;
                        }
                        finally
                        {
                            p.Unpin();
                        }
                    }

                    lastError = null;
                    break;
                }
                catch (ObjectDisposedException ex) when (attempt == 0)
                {
                    // 页面可能在极窄窗口内被缓存淘汰，重试一次即可。
                    lastError = ex;
                }
                catch (Exception ex)
                {
                    lastError = ex;
                    break;
                }
            }

            if (lastError != null)
            {
                throw new InvalidOperationException(
                    $"Failed to read page {pageId} in collection '{col}'.", lastError);
            }

            if (pageSnapshot == null || itemCount == 0) continue;

            foreach (var result in _dataPageAccess.ScanRawDocumentsFromPageSnapshotWithPredicateInfo(pageSnapshot, itemCount, endOffset, predicates))
            {
                yield return result;
            }
        }
    }

    private async IAsyncEnumerable<RawScanResult> StreamRawScanResultPagesAsync(
        string col,
        CollectionState st,
        ScanPredicate[]? predicates,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var pages = new List<uint>(st.OwnedPages.Count);
        foreach (var page in st.OwnedPages)
        {
            pages.Add(page.Key);
        }

        pages.Sort();

        foreach (var pageId in pages)
        {
            cancellationToken.ThrowIfCancellationRequested();

            byte[]? pageSnapshot = null;
            int itemCount = 0;
            int endOffset = 0;
            Exception? lastError = null;

            for (int attempt = 0; attempt < 2; attempt++)
            {
                try
                {
                    var p = await _pageManager.GetPageAsync(pageId, cancellationToken: cancellationToken).ConfigureAwait(false);

                    using (st.EnterPageMutationLock(pageId))
                    {
                        if (p.PageType != PageType.Data || p.Header.ItemCount == 0)
                        {
                            pageSnapshot = null;
                            itemCount = 0;
                            endOffset = 0;
                            break;
                        }

                        p.Pin();
                        try
                        {
                            itemCount = p.Header.ItemCount;
                            pageSnapshot = p.Snapshot(includeUnusedTail: false);
                            endOffset = pageSnapshot.Length;
                        }
                        finally
                        {
                            p.Unpin();
                        }
                    }

                    lastError = null;
                    break;
                }
                catch (ObjectDisposedException ex) when (attempt == 0)
                {
                    lastError = ex;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    lastError = ex;
                    break;
                }
            }

            if (lastError != null)
            {
                throw new InvalidOperationException(
                    $"Failed to read page {pageId} in collection '{col}'.", lastError);
            }

            if (pageSnapshot == null || itemCount == 0) continue;

            foreach (var result in _dataPageAccess.ScanRawDocumentsFromPageSnapshotWithPredicateInfo(pageSnapshot, itemCount, endOffset, predicates))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return result;
            }
        }
    }

    private async Task<BsonDocument?> TryReadCommittedByLocationAsync(
        string col,
        BsonValue id,
        CollectionState st,
        DocumentLocation location,
        CancellationToken cancellationToken)
    {
        var page = await _pageManager.GetPageAsync(location.PageId, cancellationToken: cancellationToken).ConfigureAwait(false);
        BsonDocument? document = null;

        using (st.EnterPageMutationLock(location.PageId))
        {
            if (page.PageType != PageType.Data || location.EntryIndex >= page.Header.ItemCount)
            {
                return null;
            }

            var entry = _dataPageAccess.ReadDocumentAt(page, location.EntryIndex);
            if (entry == null)
            {
                return null;
            }

            document = entry.Value.Document;
        }

        if (!document.TryGetValue("_id", out var documentId) || !BsonValuesEqual(documentId, id))
        {
            return null;
        }

        if (document.TryGetValue("_collection", out var collectionValue) && collectionValue.ToString() != col)
        {
            return null;
        }

        return await ResolveLargeDocumentAsync(document, cancellationToken).ConfigureAwait(false);
    }

    private static BsonDocument DeserializeDocumentOrThrow(ReadOnlyMemory<byte> slice)
    {
        try
        {
            return BsonSerializer.DeserializeDocument(slice);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Failed to deserialize BSON document from storage slice.", ex);
        }
    }

    internal BsonDocument ResolveLargeDocument(BsonDocument doc)
    {
        if (doc.TryGetValue("_isLargeDocument", out var v) && v.ToBoolean(null))
        {
            uint lId = (uint)doc["_largeDocumentIndex"].ToInt64(null);
            var data = _largeDocumentStorage.ReadLargeDocument(lId);
            return BsonSerializer.DeserializeDocument(data);
        }
        return doc;
    }

    internal async Task<BsonDocument> ResolveLargeDocumentAsync(BsonDocument doc, CancellationToken cancellationToken = default)
    {
        if (doc.TryGetValue("_isLargeDocument", out var v) && v.ToBoolean(null))
        {
            uint lId = (uint)doc["_largeDocumentIndex"].ToInt64(null);
            var data = await _largeDocumentStorage.ReadLargeDocumentAsync(lId, cancellationToken).ConfigureAwait(false);
            return BsonSerializer.DeserializeDocument(data);
        }
        return doc;
    }

    private static bool IsCommittedDocumentMatch(string collectionName, BsonValue id, BsonDocument document)
    {
        return document.TryGetValue("_id", out var documentId) &&
               BsonValuesEqual(documentId, id) &&
               (!document.TryGetValue("_collection", out var collectionValue) ||
                collectionValue.ToString() == collectionName);
    }

    private void ReadCommittedPageLookups(
        string collectionName,
        Page page,
        List<(BsonValue Id, int Ordinal, DocumentLocation Location)> lookups,
        BsonDocument?[] results)
    {
        if (page.PageType != PageType.Data || page.Header.ItemCount == 0)
        {
            return;
        }

        if (ShouldReadCommittedLookupsSparse(lookups.Count, page.Header.ItemCount))
        {
            foreach (var lookup in lookups)
            {
                if (lookup.Location.EntryIndex >= page.Header.ItemCount)
                {
                    continue;
                }

                var entry = _dataPageAccess.ReadDocumentAt(page, lookup.Location.EntryIndex);
                if (entry == null)
                {
                    continue;
                }

                var document = entry.Value.Document;
                if (IsCommittedDocumentMatch(collectionName, lookup.Id, document))
                {
                    results[lookup.Ordinal] = document;
                }
            }

            return;
        }

        var entries = _dataPageAccess.ReadDocumentsFromPageForRead(page);
        foreach (var lookup in lookups)
        {
            if (lookup.Location.EntryIndex >= entries.Count)
            {
                continue;
            }

            var document = entries[lookup.Location.EntryIndex].Document;
            if (IsCommittedDocumentMatch(collectionName, lookup.Id, document))
            {
                results[lookup.Ordinal] = document;
            }
        }
    }

    private static bool ShouldReadCommittedLookupsSparse(int lookupCount, int itemCount)
    {
        return lookupCount * 4 <= itemCount;
    }

    private BsonDocument? FindByIdFullScan(string col, BsonValue id, CollectionState st)
    {
        RecordFindByIdFullScan();

        var idPredicate = new[]
        {
            new ScanPredicate(
                Encoding.UTF8.GetBytes("_id"),
                Encoding.UTF8.GetBytes("id"),
                Encoding.UTF8.GetBytes("Id"),
                id,
                ExpressionType.Equal)
        };

        foreach (var result in ReadRawScanResultSnapshot(col, st, idPredicate))
        {
            var doc = DeserializeDocumentOrThrow(result.Slice);
            if (doc.TryGetValue("_collection", out var c) && c.ToString() != col)
            {
                continue;
            }

            doc = ResolveLargeDocument(doc);
            if (doc.TryGetValue("_id", out var documentId) && BsonValuesEqual(documentId, id))
            {
                RecordFindByIdFullScanHit(col, id);
                return doc;
            }
        }

        return null;
    }

    private void RecordFindByIdFullScan()
    {
        Interlocked.Increment(ref _findByIdFullScanCount);
    }

    private void RecordFindByIdFullScanHit(string col, BsonValue id)
    {
        var count = Interlocked.Increment(ref _findByIdFullScanHitCount);
        if (count == 1 || IsPowerOfTwo(count))
        {
            Log(
                TinyDbLogLevel.Warning,
                $"Primary key index miss in collection '{col}' for id '{id}'. Full-scan fallback found the document. HitCount={count}.");
        }
    }

    private void RecordFindByIdStaleIndexHit(string col, BsonValue id)
    {
        var count = Interlocked.Increment(ref _findByIdStaleIndexHitCount);
        if (count == 1 || IsPowerOfTwo(count))
        {
            Log(
                TinyDbLogLevel.Warning,
                $"Primary key index stale hit in collection '{col}' for id '{id}'. Falling back to full scan. StaleHitCount={count}.");
        }
    }

    private static bool IsPowerOfTwo(long value) => (value & (value - 1)) == 0;

    private bool TryResolveDocumentLocation(string col, CollectionState st, BsonValue id, out Page p, out List<PageDocumentEntry> e, out ushort i)
    {
        p = null!; e = null!; i = 0;
        if (!st.Index.TryGet(id, out var loc)) return false;
        p = _pageManager.GetPagePinned(loc.PageId);
        try
        {
            e = _dataPageAccess.ReadDocumentsFromPage(p);
            if (loc.EntryIndex < e.Count && IsCommittedDocumentMatch(col, id, e[loc.EntryIndex].Document))
            {
                i = loc.EntryIndex;
                return true;
            }
        }
        catch
        {
            p.Unpin();
            throw;
        }

        p.Unpin();
        p = null!;
        e = null!;
        return false;
    }

    internal void BuildDocumentLocationCache(string col, CollectionState st)
    {
        st.Index.Clear();
        st.OwnedPages.Clear();
        uint total = _pageManager.TotalPages;
        for (uint pId = 1; pId <= total; pId++)
        {
            Page p;
            try
            {
                p = _pageManager.GetPage(pId);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to rebuild collection '{col}' from page {pId}.", ex);
            }
            if (p.PageType != PageType.Data || p.Header.ItemCount == 0) continue;

            ushort idx = 0;
            bool pageOwned = false;

            foreach (var doc in _dataPageAccess.ScanDocumentsFromPage(p))
            {
                if (idx == 0)
                {
                    if (doc.TryGetValue("_collection", out var c) && c.ToString() == col)
                    {
                        st.OwnedPages.TryAdd(pId, 0);
                        pageOwned = true;
                    }
                    else
                    {
                        break;
                    }
                }

                if (pageOwned)
                {
                    if (doc.TryGetValue("_id", out var id))
                    {
                        st.Index.Set(id, new DocumentLocation(p.PageID, idx));
                    }
                }
                idx++;
            }
        }
    }
}
