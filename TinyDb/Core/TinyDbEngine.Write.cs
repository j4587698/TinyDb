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

    internal BsonValue InsertDocument(string col, BsonDocument doc)
    {
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });
        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        BsonValue res;
        using (var pr = PrepareSerializedInsertPayload(col, doc, out _))
        {
            _metadataManager.ValidateDocumentForWrite(col, pr.Document, _options.SchemaValidationMode);
            using var documentLock = st.EnterDocumentLock(pr.Id);
            using var durabilityScope = BeginImplicitWalTransaction();
            try
            {
                res = InsertPreparedDocument(col, pr, st, idx, true);
                durabilityScope?.Commit();
            }
            catch (Exception ex)
            {
                RollbackImplicitWalTransaction(durabilityScope, ex);
                throw;
            }
        }
        EnsureWriteDurability();
        return res;
    }

    /// <summary>
    /// 异步插入文档
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="doc">要插入的文档</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入文档的ID</returns>
    internal async Task<BsonValue> InsertDocumentAsync(string col, BsonDocument doc, CancellationToken cancellationToken = default)
    {
        using var collectionCommitGate = await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false);
        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        BsonValue res;
        using (var pr = PrepareSerializedInsertPayload(col, doc, out _))
        {
            _metadataManager.ValidateDocumentForWrite(col, pr.Document, _options.SchemaValidationMode);
            using var documentLock = await st.EnterDocumentLockAsync(pr.Id, cancellationToken).ConfigureAwait(false);
            var pendingWalTransaction = PrepareImplicitWalTransactionAsync(cancellationToken);
            await pendingWalTransaction.BeginTask.ConfigureAwait(false);
            using var durabilityScope = EnterImplicitWalTransactionContext(pendingWalTransaction);
            try
            {
                res = await InsertPreparedDocumentAsync(col, pr, st, idx, true, cancellationToken).ConfigureAwait(false);
                if (durabilityScope != null) await durabilityScope.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                RollbackImplicitWalTransaction(durabilityScope, ex);
                throw;
            }
        }
        await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        return res;
    }

    internal int UpdateDocument(string col, BsonDocument doc)
    {
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });
        doc = PrepareDocumentForUpdate(col, doc, out var id);
        if (id == null || id.IsNull) return 0;

        _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        bool updated;

        using (st.EnterDocumentLock(id))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            try
            {
                updated = TryUpdatePreparedDocument(col, doc, id, st, idxMgr);
                if (updated) durabilityScope?.Commit();
            }
            catch (Exception ex)
            {
                RollbackImplicitWalTransaction(durabilityScope, ex);
                throw;
            }
        }

        if (!updated) return 0;

        EnsureWriteDurability();
        return 1;
    }

    /// <summary>
    /// 异步更新文档
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="doc">要更新的文档</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>更新的文档数量</returns>
    internal async Task<int> UpdateDocumentAsync(string col, BsonDocument doc, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var collectionCommitGate = await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false);
        doc = PrepareDocumentForUpdate(col, doc, out var id);
        if (id == null || id.IsNull) return 0;

        _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        bool updated;

        using (await EnterDocumentLockWithPrefetchedPageAsync(st, id, cancellationToken).ConfigureAwait(false))
        {
            var pendingWalTransaction = PrepareImplicitWalTransactionAsync(cancellationToken);
            await pendingWalTransaction.BeginTask.ConfigureAwait(false);
            using var durabilityScope = EnterImplicitWalTransactionContext(pendingWalTransaction);
            try
            {
                updated = await TryUpdatePreparedDocumentAsync(col, doc, id, st, idxMgr, cancellationToken).ConfigureAwait(false);
                if (updated && durabilityScope != null) await durabilityScope.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                RollbackImplicitWalTransaction(durabilityScope, ex);
                throw;
            }
        }

        if (!updated) return 0;

        await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        return 1;
    }

    internal int UpdateDocuments(string col, IReadOnlyList<BsonDocument> docs)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return 0;
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });

        var prepared = new List<(BsonDocument Doc, BsonValue Id)>(docs.Count);
        foreach (var d in docs)
        {
            if (d == null) continue;

            var doc = PrepareDocumentForUpdate(col, d, out var id);
            if (id == null || id.IsNull) continue;

            _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);
            prepared.Add((doc, id));
        }

        if (prepared.Count == 0) return 0;

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        var updatedCount = 0;

        using (st.EnterDocumentLocks(prepared, static item => item.Id))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            try
            {
                foreach (var (doc, id) in prepared)
                {
                    if (TryUpdatePreparedDocument(col, doc, id, st, idxMgr))
                    {
                        updatedCount++;
                    }
                }
                if (updatedCount > 0) durabilityScope?.Commit();
            }
            catch (Exception ex)
            {
                RollbackImplicitWalTransaction(durabilityScope, ex);
                throw;
            }
        }

        if (updatedCount > 0)
        {
            EnsureWriteDurability();
        }

        return updatedCount;
    }

    internal async Task<int> UpdateDocumentsAsync(string col, IReadOnlyList<BsonDocument> docs, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return 0;
        using var collectionCommitGate = await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false);

        var prepared = new List<(BsonDocument Doc, BsonValue Id)>(docs.Count);
        foreach (var d in docs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (d == null) continue;

            var doc = PrepareDocumentForUpdate(col, d, out var id);
            if (id == null || id.IsNull) continue;

            _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);
            prepared.Add((doc, id));
        }

        if (prepared.Count == 0) return 0;

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        var updatedCount = 0;

        using (await EnterDocumentLocksWithPrefetchedPagesAsync(st, prepared, static item => item.Id, cancellationToken).ConfigureAwait(false))
        {
            var pendingWalTransaction = PrepareImplicitWalTransactionAsync(cancellationToken);
            await pendingWalTransaction.BeginTask.ConfigureAwait(false);
            using var durabilityScope = EnterImplicitWalTransactionContext(pendingWalTransaction);
            try
            {
                foreach (var (doc, id) in prepared)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (await TryUpdatePreparedDocumentAsync(col, doc, id, st, idxMgr, cancellationToken).ConfigureAwait(false))
                    {
                        updatedCount++;
                    }
                }
                if (updatedCount > 0 && durabilityScope != null) await durabilityScope.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                RollbackImplicitWalTransaction(durabilityScope, ex);
                throw;
            }
        }

        if (updatedCount > 0)
        {
            await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        }

        return updatedCount;
    }

    internal (UpdateType UpdateType, int Count) UpsertDocument(string col, BsonDocument doc)
    {
        var result = UpsertDocuments(col, new[] { doc });
        return result.InsertedCount > 0
            ? (UpdateType.Insert, result.InsertedCount)
            : (UpdateType.Update, result.UpdatedCount);
    }

    internal async Task<(UpdateType UpdateType, int Count)> UpsertDocumentAsync(string col, BsonDocument doc, CancellationToken cancellationToken = default)
    {
        var result = await UpsertDocumentsAsync(col, new[] { doc }, cancellationToken).ConfigureAwait(false);
        return result.InsertedCount > 0
            ? (UpdateType.Insert, result.InsertedCount)
            : (UpdateType.Update, result.UpdatedCount);
    }

    internal (int InsertedCount, int UpdatedCount) UpsertDocuments(string col, IReadOnlyList<BsonDocument> docs)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return (0, 0);
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });

        var prepared = new List<(BsonDocument Doc, BsonValue Id)>(docs.Count);
        foreach (var d in docs)
        {
            if (d == null) continue;

            var doc = PrepareDocumentForInsert(col, d, out var id);
            _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);
            prepared.Add((doc, id));
        }

        if (prepared.Count == 0) return (0, 0);

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        var insertedCount = 0;
        var updatedCount = 0;

        using (st.EnterDocumentLocks(prepared, static item => item.Id))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            try
            {
                foreach (var (doc, id) in prepared)
                {
                    if (TryUpdatePreparedDocument(col, doc, id, st, idxMgr))
                    {
                        updatedCount++;
                    }
                    else
                    {
                        InsertPreparedDocument(col, doc, id, st, idxMgr, true);
                        insertedCount++;
                    }
                }
                if (insertedCount + updatedCount > 0) durabilityScope?.Commit();
            }
            catch (Exception ex)
            {
                RollbackImplicitWalTransaction(durabilityScope, ex);
                throw;
            }
        }

        if (insertedCount + updatedCount > 0)
        {
            EnsureWriteDurability();
        }

        return (insertedCount, updatedCount);
    }

    internal async Task<(int InsertedCount, int UpdatedCount)> UpsertDocumentsAsync(string col, IReadOnlyList<BsonDocument> docs, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return (0, 0);
        using var collectionCommitGate = await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false);

        var prepared = new List<(BsonDocument Doc, BsonValue Id)>(docs.Count);
        foreach (var d in docs)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (d == null) continue;

            var doc = PrepareDocumentForInsert(col, d, out var id);
            _metadataManager.ValidateDocumentForWrite(col, doc, _options.SchemaValidationMode);
            prepared.Add((doc, id));
        }

        if (prepared.Count == 0) return (0, 0);

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        var insertedCount = 0;
        var updatedCount = 0;

        using (await EnterDocumentLocksWithPrefetchedPagesAsync(st, prepared, static item => item.Id, cancellationToken).ConfigureAwait(false))
        {
            var pendingWalTransaction = PrepareImplicitWalTransactionAsync(cancellationToken);
            await pendingWalTransaction.BeginTask.ConfigureAwait(false);
            using var durabilityScope = EnterImplicitWalTransactionContext(pendingWalTransaction);
            try
            {
                foreach (var (doc, id) in prepared)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (await TryUpdatePreparedDocumentAsync(col, doc, id, st, idxMgr, cancellationToken).ConfigureAwait(false))
                    {
                        updatedCount++;
                    }
                    else
                    {
                        await InsertPreparedDocumentAsync(col, doc, id, st, idxMgr, true, cancellationToken).ConfigureAwait(false);
                        insertedCount++;
                    }
                }
                if (insertedCount + updatedCount > 0 && durabilityScope != null)
                {
                    await durabilityScope.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                RollbackImplicitWalTransaction(durabilityScope, ex);
                throw;
            }
        }

        if (insertedCount + updatedCount > 0)
        {
            await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        }

        return (insertedCount, updatedCount);
    }

    internal int DeleteDocument(string col, BsonValue id)
    {
        if (id == null || id.IsNull) return 0;
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        int deleted;
        using (st.EnterDocumentLock(id))
        {
            deleted = DeleteDocumentCore(col, id, st, idxMgr);
        }

        if (deleted > 0)
        {
            EnsureWriteDurability();
        }

        return deleted;
    }

    /// <summary>
    /// 异步删除文档
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="id">要删除的文档ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除的文档数量</returns>
    internal async Task<int> DeleteDocumentAsync(string col, BsonValue id, CancellationToken cancellationToken = default)
    {
        if (id == null || id.IsNull) return 0;
        using var collectionCommitGate = await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false);

        var st = GetCollectionState(col);
        var idxMgr = GetIndexManager(col);
        using (await EnterDocumentLockWithPrefetchedPageAsync(st, id, cancellationToken).ConfigureAwait(false))
        {
            var deleted = await DeleteDocumentCoreAsync(col, id, st, idxMgr, cancellationToken).ConfigureAwait(false);
            if (deleted == 0) return 0;
        }
        await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
        return 1;
    }

    internal int InsertDocuments(string col, IReadOnlyList<BsonDocument> docs)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return 0;
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });

        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        var exceptions = new List<Exception>();
        var preparedPayloads = PrepareSerializedInsertPayloads(col, docs, exceptions);
        if (preparedPayloads.Count == 0)
        {
            if (exceptions.Count > 0)
            {
                throw new AggregateException("One or more errors occurred during batch insert", exceptions);
            }

            return 0;
        }

        var insertedPayloads = new List<PreparedInsertPayload>(preparedPayloads.Count);
        int insertedCount = 0;

        try
        {
            using (st.EnterDocumentLocks(preparedPayloads, static payload => payload.Id))
            {
                using var durabilityScope = BeginImplicitWalTransaction();
                try
                {
                    foreach (var payload in preparedPayloads)
                    {
                        try
                        {
                            InsertPreparedDocument(col, payload, st, idx, true);
                            insertedPayloads.Add(payload);
                            insertedCount++;
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                            break;
                        }
                    }

                    if (exceptions.Count > 0)
                    {
                        if (exceptions.FirstOrDefault(static ex => ex is WritableDataPageSelectionException) is { } pageSelectionException)
                        {
                            throw pageSelectionException;
                        }

                        throw new AggregateException("One or more errors occurred during batch insert", exceptions);
                    }

                    durabilityScope?.Commit();
                }
                catch (Exception ex)
                {
                    if (durabilityScope != null)
                    {
                        RollbackImplicitWalTransaction(durabilityScope, ex);
                    }
                    else
                    {
                        var exceptionCountBeforeRollback = exceptions.Count;
                        RollbackInsertedDocuments(col, insertedPayloads, st, idx, exceptions);
                        if (ex is WritableDataPageSelectionException && exceptions.Count == exceptionCountBeforeRollback)
                        {
                            throw;
                        }

                        throw new AggregateException("One or more errors occurred during batch insert", exceptions);
                    }
                    throw;
                }
            }

            EnsureWriteDurability();
            return insertedCount;
        }
        finally
        {
            foreach (var payload in preparedPayloads)
            {
                payload.Dispose();
            }
        }
    }

    /// <summary>
    /// 异步批量插入文档
    /// </summary>
    /// <param name="col">集合名称</param>
    /// <param name="docs">要插入的文档数组</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入的文档数量</returns>
    internal async Task<int> InsertDocumentsAsync(string col, IReadOnlyList<BsonDocument> docs, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();
        if (docs == null) throw new ArgumentNullException(nameof(docs));
        if (docs.Count == 0) return 0;
        cancellationToken.ThrowIfCancellationRequested();
        using var collectionCommitGate = await EnterCollectionWriteGatesAsync(new[] { col }, cancellationToken).ConfigureAwait(false);

        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        var exceptions = new List<Exception>();
        var preparedPayloads = PrepareSerializedInsertPayloads(col, docs, exceptions, cancellationToken);
        if (preparedPayloads.Count == 0)
        {
            if (exceptions.Count > 0)
            {
                var cancellationException = exceptions.Count == 1
                    ? exceptions[0] as OperationCanceledException
                    : null;
                if (cancellationException != null)
                {
                    throw cancellationException;
                }

                throw new AggregateException("One or more errors occurred during batch insert", exceptions);
            }

            return 0;
        }

        var insertedPayloads = new List<PreparedInsertPayload>(preparedPayloads.Count);
        int insertedCount = 0;

        try
        {
            using (await st.EnterDocumentLocksAsync(
                       preparedPayloads,
                       static payload => payload.Id,
                       cancellationToken).ConfigureAwait(false))
            {
                var pendingWalTransaction = PrepareImplicitWalTransactionAsync(cancellationToken);
                await pendingWalTransaction.BeginTask.ConfigureAwait(false);
                using var durabilityScope = EnterImplicitWalTransactionContext(pendingWalTransaction);
                try
                {
                    foreach (var payload in preparedPayloads)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            exceptions.Add(new OperationCanceledException(cancellationToken));
                            break;
                        }

                        try
                        {
                            await InsertPreparedDocumentAsync(col, payload, st, idx, true, cancellationToken).ConfigureAwait(false);
                            insertedPayloads.Add(payload);
                            insertedCount++;
                        }
                        catch (Exception ex)
                        {
                            exceptions.Add(ex);
                            break;
                        }
                    }

                    if (exceptions.Count > 0)
                    {
                        var cancellationException = exceptions.Count == 1
                            ? exceptions[0] as OperationCanceledException
                            : null;
                        if (cancellationException != null)
                        {
                            throw cancellationException;
                        }

                        if (exceptions.FirstOrDefault(static ex => ex is WritableDataPageSelectionException) is { } pageSelectionException)
                        {
                            throw pageSelectionException;
                        }

                        throw new AggregateException("One or more errors occurred during batch insert", exceptions);
                    }

                    if (durabilityScope != null) await durabilityScope.CommitAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    if (durabilityScope != null)
                    {
                        RollbackImplicitWalTransaction(durabilityScope, ex);
                    }
                    else
                    {
                        var exceptionCountBeforeRollback = exceptions.Count;
                        await RollbackInsertedDocumentsAsync(col, insertedPayloads, st, idx, exceptions).ConfigureAwait(false);
                        if (ex is WritableDataPageSelectionException && exceptions.Count == exceptionCountBeforeRollback)
                        {
                            throw;
                        }

                        throw new AggregateException("One or more errors occurred during batch insert", exceptions);
                    }
                    throw;
                }
            }

            await EnsureWriteDurabilityAsync(cancellationToken).ConfigureAwait(false);
            return insertedCount;
        }
        finally
        {
            foreach (var payload in preparedPayloads)
            {
                payload.Dispose();
            }
        }
    }

    internal BsonValue InsertDocumentInternal(string col, BsonDocument d)
    {
        using var collectionCommitGate = EnterCollectionWriteGates(new[] { col });
        var st = GetCollectionState(col);
        var idx = GetIndexManager(col);
        using var pr = PrepareSerializedInsertPayload(col, d, out _);
        BsonValue result;
        using (st.EnterDocumentLock(pr.Id))
        {
            using var durabilityScope = BeginImplicitWalTransaction();
            try
            {
                result = InsertPreparedDocument(col, pr, st, idx, true);
                durabilityScope?.Commit();
            }
            catch (Exception ex)
            {
                RollbackImplicitWalTransaction(durabilityScope, ex);
                throw;
            }
        }

        return result;
    }



}
