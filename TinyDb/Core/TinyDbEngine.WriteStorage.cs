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
    private sealed class PreparedInsertPayload : IDisposable
    {
        private PooledBufferWriter? _buffer;

        private PreparedInsertPayload(BsonDocument document, BsonValue id, PooledBufferWriter buffer)
        {
            Document = document;
            Id = id;
            _buffer = buffer;
        }

        public BsonDocument Document { get; private set; }
        public BsonValue Id { get; }
        public int SerializedLength => _buffer?.WrittenCount ?? 0;
        public ReadOnlySpan<byte> SerializedSpan => _buffer != null ? _buffer.WrittenSpan : ReadOnlySpan<byte>.Empty;

        public static PreparedInsertPayload Create(BsonDocument document, BsonValue id)
        {
            var buffer = new PooledBufferWriter();
            try
            {
                BsonSerializer.SerializeDocumentToBuffer(document, buffer);
                return new PreparedInsertPayload(document, id, buffer);
            }
            catch
            {
                buffer.Dispose();
                throw;
            }
        }

        public void ReplaceDocument(BsonDocument document)
        {
            var nextBuffer = new PooledBufferWriter();
            try
            {
                BsonSerializer.SerializeDocumentToBuffer(document, nextBuffer);
            }
            catch
            {
                nextBuffer.Dispose();
                throw;
            }

            _buffer?.Dispose();
            _buffer = nextBuffer;
            Document = document;
        }

        public void Dispose()
        {
            _buffer?.Dispose();
            _buffer = null;
        }
    }

    private sealed class WritableDataPageSelectionException : InvalidOperationException
    {
        public WritableDataPageSelectionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }

    private BsonValue InsertPreparedDocument(string col, BsonDocument doc, BsonValue id, CollectionState st, IndexManager idx, bool u)
    {
        using var prepared = PreparedInsertPayload.Create(doc, id);
        return InsertPreparedDocument(col, prepared, st, idx, u);
    }

    private BsonValue InsertPreparedDocument(string col, PreparedInsertPayload prepared, CollectionState st, IndexManager idx, bool u)
    {
        var doc = prepared.Document;
        var indexDocument = doc;
        uint largeDocumentPageId = 0;
        bool appendedDocument = false;

        ThrowIfPrimaryKeyExists(st, prepared.Id);

        try
        {
            if (LargeDocumentStorage.RequiresLargeDocumentStorage(prepared.SerializedLength, _dataPageAccess.GetMaxDocumentSize()))
            {
                var originalLength = prepared.SerializedLength;
                largeDocumentPageId = StoreLargeDocumentOrThrow(prepared.SerializedSpan, col);
                doc = CreateLargeDocumentIndexDocument(prepared.Id, col, largeDocumentPageId, originalLength);
                prepared.ReplaceDocument(doc);
            }

            AppendDocumentBytesToWritableDataPage(st, prepared.Id, prepared.SerializedSpan);
            appendedDocument = true;
            if (u)
            {
                try
                {
                    idx.InsertDocument(indexDocument, prepared.Id);
                }
                catch
                {
                    DeleteDocumentCore(col, prepared.Id, st, idx);
                    throw;
                }
            }

            return prepared.Id;
        }
        catch (Exception ex)
        {
            if (!appendedDocument && largeDocumentPageId != 0)
            {
                CleanupLargeDocumentAfterFailedInsert(largeDocumentPageId, ex);
            }

            throw;
        }
    }

    private void ThrowIfPrimaryKeyExists(CollectionState st, BsonValue id)
    {
        if (st.Index.TryGet(id, out _))
        {
            throw new InvalidOperationException($"Duplicate document id '{id}'.");
        }
    }

    private (Page Page, bool IsNew) SelectWritableDataPage(CollectionState st, int requiredSize)
    {
        try
        {
            lock (st.PageState.SyncRoot)
            {
                var selected = _dataPageAccess.GetWritableDataPageLocked(st.PageState, requiredSize);
                st.OwnedPages.TryAdd(selected.Page.PageID, 0);
                if (selected.IsNew)
                {
                    IncrementUsedPagesAndWriteHeader();
                }

                return selected;
            }
        }
        catch (Exception ex) when (ex is not WritableDataPageSelectionException)
        {
            throw new WritableDataPageSelectionException("Failed to select a writable data page.", ex);
        }
    }

    private void MarkWritablePageUnavailable(CollectionState st, uint pageId)
    {
        Interlocked.CompareExchange(ref st.PageState.PageId, 0, pageId);
    }

    private void IncrementUsedPagesAndWriteHeader()
    {
        lock (_lock)
        {
            _header.UsedPages++;
            WriteHeader();
        }
    }

    private DocumentLocation AppendDocumentBytesToWritableDataPage(
        CollectionState st,
        BsonValue id,
        ReadOnlySpan<byte> bytes)
    {
        var requiredSize = DataPageAccess.GetEntrySize(bytes.Length);

        while (true)
        {
            var (page, _) = SelectWritableDataPage(st, requiredSize);
            try
            {
                using (st.EnterPageMutationLock(page.PageID))
                {
                    if (page.Header.PageType != PageType.Data || page.Header.FreeBytes < requiredSize)
                    {
                        MarkWritablePageUnavailable(st, page.PageID);
                        continue;
                    }

                    var entryIndex = page.Header.ItemCount;
                    var beforeImage = _dataPageAccess.CaptureBeforeImageForWal(page);
                    _dataPageAccess.AppendDocumentToPage(page, bytes);
                    var location = new DocumentLocation(page.PageID, entryIndex);
                    st.Index.Set(id, location);
                    _dataPageAccess.PersistPageDeferred(page, beforeImage);
                    return location;
                }
            }
            finally
            {
                page.Unpin();
            }
        }
    }

    private bool TryUpdatePreparedDocument(string col, BsonDocument doc, BsonValue id, CollectionState st, IndexManager idxMgr)
    {
        PageDocumentEntry old;
        var updatedDocument = doc;
        var newIndexDocument = doc;
        uint newLargeDocumentPageId = 0;
        bool newLargeDocumentReferenceAttempted = false;
        byte[]? relocationBytes = null;

        if (!st.Index.TryGet(id, out var initialLocation)) return false;

        try
        {
            using (st.EnterPageMutationLock(initialLocation.PageId))
            {
                if (!TryResolveDocumentLocation(col, st, id, out var p, out var e, out var i)) return false;
                try
                {
                    if (i >= e.Count) return false;
                    old = e[i];

                    var bs = BsonSerializer.SerializeDocument(updatedDocument);
                    if (LargeDocumentStorage.RequiresLargeDocumentStorage(bs.Length, _dataPageAccess.GetMaxDocumentSize()))
                    {
                        var largeDocumentPageId = StoreLargeDocumentOrThrow(bs, col);
                        newLargeDocumentPageId = largeDocumentPageId;
                        updatedDocument = CreateLargeDocumentIndexDocument(id, col, largeDocumentPageId, bs.Length);
                        bs = BsonSerializer.SerializeDocument(updatedDocument);
                    }

                    e[i] = new PageDocumentEntry(updatedDocument, bs);

                    if (!_dataPageAccess.CanFitInPage(p, e))
                    {
                        e[i] = old;
                        relocationBytes = bs;
                    }
                    else
                    {
                        newLargeDocumentReferenceAttempted = true;
                        _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                    }
                }
                finally
                {
                    p.Unpin();
                }
            }

            if (relocationBytes != null)
            {
                newLargeDocumentReferenceAttempted = true;
                MoveUpdatedDocumentToWritablePage(col, st, id, updatedDocument, relocationBytes);
            }
        }
        catch (Exception ex)
        {
            if (newLargeDocumentPageId != 0 && !newLargeDocumentReferenceAttempted)
            {
                CleanupLargeDocumentAfterFailedUpdate(newLargeDocumentPageId, ex);
            }

            throw;
        }

        try
        {
            var oldIndexDocument = old.IsLargeDocument ? ResolveLargeDocument(old.Document) : old.Document;
            idxMgr.UpdateDocument(oldIndexDocument, newIndexDocument, id);
        }
        catch
        {
            RestoreDocumentDataWithoutIndexUpdate(col, st, id, old);
            if (newLargeDocumentPageId != 0)
            {
                DeleteLargeDocumentOrThrow(newLargeDocumentPageId);
            }
            throw;
        }

        if (old.IsLargeDocument) DeleteLargeDocumentOrThrow(old.LargeDocumentIndexPageId);
        return true;
    }

    private void MoveUpdatedDocumentToWritablePage(
        string col,
        CollectionState st,
        BsonValue id,
        BsonDocument updatedDocument,
        byte[] updatedBytes)
    {
        var requiredSize = DataPageAccess.GetEntrySize(updatedBytes.Length);

        while (true)
        {
            if (!st.Index.TryGet(id, out var oldLocation))
            {
                throw new InvalidOperationException($"Document '{id}' disappeared during update relocation.");
            }

            var (targetPage, _) = SelectWritableDataPage(st, requiredSize);
            try
            {
                using (st.EnterPageMutationLocks(new[] { oldLocation.PageId, targetPage.PageID }))
                {
                    if (!st.Index.TryGet(id, out var currentLocation) ||
                        currentLocation.PageId != oldLocation.PageId)
                    {
                        continue;
                    }

                    if (targetPage.Header.PageType != PageType.Data || targetPage.Header.FreeBytes < requiredSize)
                    {
                        MarkWritablePageUnavailable(st, targetPage.PageID);
                        continue;
                    }

                    var oldPage = _pageManager.GetPagePinned(currentLocation.PageId);
                    try
                    {
                        var oldEntries = _dataPageAccess.ReadDocumentsFromPage(oldPage);
                        if (currentLocation.EntryIndex >= oldEntries.Count)
                        {
                            throw new InvalidOperationException($"Primary index for document '{id}' points outside page {oldPage.PageID}.");
                        }

                        var currentOldEntry = oldEntries[currentLocation.EntryIndex];
                        if (!BsonValuesEqual(currentOldEntry.Id, id))
                        {
                            throw new InvalidOperationException($"Primary index for document '{id}' points to a different document.");
                        }

                        oldEntries[currentLocation.EntryIndex] = new PageDocumentEntry(updatedDocument, updatedBytes);
                        if (_dataPageAccess.CanFitInPage(oldPage, oldEntries))
                        {
                            _dataPageAccess.RewritePageWithDocuments(col, st, oldPage, oldEntries, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                            return;
                        }

                        oldEntries[currentLocation.EntryIndex] = currentOldEntry;
                        oldEntries.RemoveAt(currentLocation.EntryIndex);
                        st.Index.Remove(id);

                        if (oldEntries.Count == 0 && oldPage.PageID != targetPage.PageID)
                        {
                            FreeDataPage(st, oldPage);
                        }
                        else
                        {
                            _dataPageAccess.RewritePageWithDocuments(col, st, oldPage, oldEntries, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                        }

                        var entryIndex = targetPage.Header.ItemCount;
                        var beforeImage = _dataPageAccess.CaptureBeforeImageForWal(targetPage);
                        _dataPageAccess.AppendDocumentToPage(targetPage, updatedBytes);
                        st.Index.Set(id, new DocumentLocation(targetPage.PageID, entryIndex));
                        _dataPageAccess.PersistPageDeferred(targetPage, beforeImage);
                        return;
                    }
                    finally
                    {
                        oldPage.Unpin();
                    }
                }
            }
            finally
            {
                targetPage.Unpin();
            }
        }
    }

    private void FreeDataPage(CollectionState st, Page page)
    {
        st.OwnedPages.TryRemove(page.PageID, out _);
        Interlocked.CompareExchange(ref st.PageState.PageId, 0, page.PageID);

        _pageManager.FreePage(page.PageID);
        DecrementUsedPagesAndWriteHeader();
    }

    private void RestoreDocumentDataWithoutIndexUpdate(string col, CollectionState st, BsonValue id, PageDocumentEntry oldEntry)
    {
        if (st.Index.TryGet(id, out var currentLocation))
        {
            using (st.EnterPageMutationLock(currentLocation.PageId))
            {
                if (TryResolveDocumentLocation(col, st, id, out var currentPage, out var currentEntries, out var currentIndex) &&
                    currentIndex < currentEntries.Count)
                {
                    try
                    {
                        currentEntries.RemoveAt(currentIndex);
                        st.Index.Remove(id);

                        if (currentEntries.Count == 0)
                        {
                            FreeDataPage(st, currentPage);
                        }
                        else
                        {
                            _dataPageAccess.RewritePageWithDocuments(col, st, currentPage, currentEntries, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                        }
                    }
                    finally
                    {
                        currentPage.Unpin();
                    }
                }
            }
        }

        AppendDocumentBytesToWritableDataPage(st, id, oldEntry.RawMemory.Span);
    }

    private static BsonDocument PrepareDocumentForUpdate(string col, BsonDocument doc, out BsonValue id)
    {
        if (!doc.TryGetValue("_id", out var existingId) || existingId == null || existingId.IsNull)
        {
            id = BsonNull.Value;
            return doc;
        }

        id = existingId;

        // Ensure _collection field matches the target collection name.
        // This is critical for AOT-generated documents where _collection might differ from the runtime collection.
        if (!doc.TryGetValue("_collection", out var docCol) || docCol.ToString() != col)
        {
            doc = doc.Set("_collection", col);
        }

        return doc;
    }

    private void RollbackInsertedDocuments(
        string collectionName,
        IReadOnlyList<PreparedInsertPayload> insertedPayloads,
        CollectionState st,
        IndexManager idxMgr,
        List<Exception> exceptions)
    {
        HashSet<BsonValue>? rolledBackIds = null;

        for (var i = insertedPayloads.Count - 1; i >= 0; i--)
        {
            var id = insertedPayloads[i].Id;
            if (id == null || id.IsNull)
            {
                continue;
            }

            rolledBackIds ??= new HashSet<BsonValue>(BsonValueComparer.EqualityComparer);
            if (!rolledBackIds.Add(id))
            {
                continue;
            }

            try
            {
                DeleteDocumentCore(collectionName, id, st, idxMgr);
            }
            catch (Exception ex)
            {
                exceptions.Add(new InvalidOperationException($"Failed to rollback inserted document '{id}' in collection '{collectionName}'.", ex));
            }
        }
    }

    private int DeleteDocumentCore(string col, BsonValue id, CollectionState st, IndexManager idxMgr)
    {
        if (!st.Index.TryGet(id, out var loc)) return 0;

        using var durabilityScope = BeginImplicitWalTransaction();
        try
        {
            using (st.EnterPageMutationLock(loc.PageId))
            {
                if (!TryResolveDocumentLocation(col, st, id, out var p, out var e, out var i)) return 0;
                try
                {
                    if (i >= e.Count) return 0;
                    var entry = e[i];
                    e.RemoveAt(i);
                    st.Index.Remove(id);
                    if (e.Count == 0)
                    {
                        FreeDataPage(st, p);
                    }
                    else
                    {
                        _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                    }

                    var indexDocument = entry.IsLargeDocument ? ResolveLargeDocument(entry.Document) : entry.Document;
                    if (entry.IsLargeDocument) DeleteLargeDocumentOrThrow(entry.LargeDocumentIndexPageId);
                    idxMgr.DeleteDocument(indexDocument, id);
                    durabilityScope?.Commit();
                    return 1;
                }
                finally
                {
                    p.Unpin();
                }
            }
        }
        catch (Exception ex)
        {
            RollbackImplicitWalTransaction(durabilityScope, ex);
            throw;
        }
    }

    private async Task<int> DeleteDocumentCoreAsync(
        string col,
        BsonValue id,
        CollectionState st,
        IndexManager idxMgr,
        CancellationToken cancellationToken)
    {
        if (!st.Index.TryGet(id, out var loc)) return 0;

        var pendingWalTransaction = PrepareImplicitWalTransactionAsync(cancellationToken);
        await pendingWalTransaction.BeginTask.ConfigureAwait(false);
        using var durabilityScope = EnterImplicitWalTransactionContext(pendingWalTransaction);
        try
        {
            using (st.EnterPageMutationLock(loc.PageId))
            {
                if (!TryResolveDocumentLocation(col, st, id, out var p, out var e, out var i)) return 0;
                try
                {
                    if (i >= e.Count) return 0;
                    var entry = e[i];
                    e.RemoveAt(i);
                    st.Index.Remove(id);
                    if (e.Count == 0)
                    {
                        FreeDataPage(st, p);
                    }
                    else
                    {
                        _dataPageAccess.RewritePageWithDocuments(col, st, p, e, (key, pId, idx) => st.Index.Set(key, new DocumentLocation(pId, idx)));
                    }

                    var indexDocument = entry.IsLargeDocument ? ResolveLargeDocument(entry.Document) : entry.Document;
                    if (entry.IsLargeDocument) DeleteLargeDocumentOrThrow(entry.LargeDocumentIndexPageId);
                    idxMgr.DeleteDocument(indexDocument, id);
                    if (durabilityScope != null) await durabilityScope.CommitAsync(cancellationToken).ConfigureAwait(false);
                    return 1;
                }
                finally
                {
                    p.Unpin();
                }
            }
        }
        catch (Exception ex)
        {
            RollbackImplicitWalTransaction(durabilityScope, ex);
            throw;
        }
    }

    internal BsonDocument PrepareDocumentForInsert(string col, BsonDocument doc, out BsonValue id)
    {
        bool hasId = doc.TryGetValue("_id", out var exId) && exId != null && !exId.IsNull;
        // 检查 _collection 是否已经存在且值正确（避免不必要的重建）
        bool hasCorrectCol = doc.TryGetValue("_collection", out var existingCol) && existingCol?.ToString() == col;
        bool hasIdFirst = doc.TryGetFirstKey(out var firstKey) &&
                          string.Equals(firstKey, "_id", StringComparison.Ordinal);

        // 快速路径：如果已有 _id 且 _collection 值正确，直接返回原文档
        if (hasId && hasCorrectCol && hasIdFirst)
        {
            id = exId!;
            return doc;
        }

        // 优化：直接创建新的 Builder，一次性添加所有字段，避免 ToBuilder() 的额外转换
        var builder = new BsonDocumentBuilder(doc.Count + 2);

        // 复制原文档的所有字段
        // 添加 _id（如果缺失）
        if (!hasId)
        {
            id = ObjectId.NewObjectId();
        }
        else
        {
            id = exId!;
        }

        // 强制设置 _collection 为实际使用的集合名称（覆盖 AOT 生成器设置的值）
        builder.Set("_id", id);
        foreach (var kvp in doc.Entries)
        {
            if (string.Equals(kvp.Key, "_id", StringComparison.Ordinal) ||
                string.Equals(kvp.Key, "_collection", StringComparison.Ordinal))
            {
                continue;
            }

            builder.Set(kvp.Key, kvp.Value);
        }

        builder.Set("_collection", col);

        return builder.Build();
    }

    private PreparedInsertPayload PrepareSerializedInsertPayload(string col, BsonDocument doc, out BsonValue id)
    {
        var prepared = PrepareDocumentForInsert(col, doc, out id);
        return PreparedInsertPayload.Create(prepared, id);
    }

    private List<PreparedInsertPayload> PrepareSerializedInsertPayloads(
        string col,
        IReadOnlyList<BsonDocument> docs,
        List<Exception> exceptions,
        CancellationToken cancellationToken = default)
    {
        var prepared = new List<PreparedInsertPayload>(docs.Count);
        for (int docIndex = 0; docIndex < docs.Count; docIndex++)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                exceptions.Add(new OperationCanceledException(cancellationToken));
                break;
            }

            var d = docs[docIndex];
            if (d == null) continue;

            PreparedInsertPayload? payload = null;
            try
            {
                payload = PrepareSerializedInsertPayload(col, d, out _);
                _metadataManager.ValidateDocumentForWrite(col, payload.Document, _options.SchemaValidationMode);
                prepared.Add(payload);
                payload = null;
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }
            finally
            {
                payload?.Dispose();
            }
        }

        return prepared;
    }
}
