using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;
using TinyDb.Query;
using TinyDb.Index;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace TinyDb.Collections;

public sealed partial class DocumentCollection<T> where T : class
{
    #region 异步方法实现

    /// <summary>
    /// 异步插入单个文档
    /// </summary>
    /// <param name="entity">要插入的实体</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入文档的ID</returns>
    public async Task<BsonValue> InsertAsync(T entity, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var document = PrepareDocumentForInsert(entity);

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，记录操作而不是直接写入（事务不支持异步）
            return ((Transaction)currentTransaction).RecordInsert(_name, document);
        }
        else
        {
            // 不在事务中，异步插入到数据库
            return await _engine.InsertDocumentAsync(_name, document, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 异步插入多个文档
    /// </summary>
    /// <param name="entities">要插入的实体集合</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入的文档数量</returns>
    public async Task<int> InsertAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            return InsertInTransaction(entities, (Transaction)currentTransaction, cancellationToken);
        }

        int totalInserted = 0;
        const int BatchSize = 1000;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            if (entity == null) continue;
            cancellationToken.ThrowIfCancellationRequested();

            var document = PrepareDocumentForInsert(entity);

            docBatch.Add(document);

            if (docBatch.Count >= BatchSize)
            {
                totalInserted += await InsertDocumentBatchAsync(docBatch, cancellationToken).ConfigureAwait(false);
                docBatch.Clear();
            }
        }

        if (docBatch.Count > 0)
        {
            totalInserted += await InsertDocumentBatchAsync(docBatch, cancellationToken).ConfigureAwait(false);
        }

        return totalInserted;
    }

    private async Task<int> InsertDocumentBatchAsync(List<BsonDocument> documents, CancellationToken cancellationToken)
    {
        if (documents.Count == 0) return 0;

        return await _engine.InsertDocumentsAsync(_name, documents, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 异步更新文档
    /// </summary>
    /// <param name="entity">要更新的实体</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>更新的文档数量</returns>
    public async Task<int> UpdateAsync(T entity, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var document = PrepareDocumentForUpdate(entity, out var id);

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var originalDocument = await _engine.FindByIdAsync(_name, id, cancellationToken).ConfigureAwait(false);
            if (originalDocument == null)
            {
                return 0;
            }
            else
            {
                ((Transaction)currentTransaction).RecordUpdate(_name, originalDocument, document);
                return 1;
            }
        }
        else
        {
            return await _engine.UpdateDocumentAsync(_name, document, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 异步更新多个文档
    /// </summary>
    /// <param name="entities">要更新的实体集合</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>更新的文档数量</returns>
    public async Task<int> UpdateAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            return await UpdateInTransactionAsync(entities, (Transaction)currentTransaction, cancellationToken).ConfigureAwait(false);
        }

        int updatedCount = 0;
        const int BatchSize = 1000;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entity != null)
            {
                var document = PrepareDocumentForUpdate(entity, out _);
                docBatch.Add(document);

                if (docBatch.Count >= BatchSize)
                {
                    updatedCount += await _engine.UpdateDocumentsAsync(_name, docBatch, cancellationToken).ConfigureAwait(false);
                    docBatch.Clear();
                }
            }
        }

        if (docBatch.Count > 0)
        {
            updatedCount += await _engine.UpdateDocumentsAsync(_name, docBatch, cancellationToken).ConfigureAwait(false);
        }

        return updatedCount;
    }

    private async Task<int> UpdateInTransactionAsync(
        IEnumerable<T> entities,
        Transaction transaction,
        CancellationToken cancellationToken)
    {
        var prepared = new List<(BsonDocument Document, BsonValue Id)>();
        var ids = new List<BsonValue>();

        foreach (var entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entity == null) continue;

            var document = PrepareDocumentForUpdate(entity, out var id);
            prepared.Add((document, id));
            ids.Add(id);
        }

        if (prepared.Count == 0)
        {
            return 0;
        }

        var originalDocuments = await _engine.FindByIdsAsync(_name, ids, cancellationToken).ConfigureAwait(false);
        return RecordPreparedUpdatesInTransaction(prepared, originalDocuments, transaction);
    }

    /// <summary>
    /// 异步删除文档
    /// </summary>
    /// <param name="id">要删除的文档ID</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除的文档数量</returns>
    public async Task<int> DeleteAsync(BsonValue id, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (id == null || id.IsNull) return 0;

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var documentToDelete = await _engine.FindByIdAsync(_name, id, cancellationToken).ConfigureAwait(false);
            if (documentToDelete == null)
            {
                return 0;
            }
            else
            {
                ((Transaction)currentTransaction).RecordDelete(_name, documentToDelete);
                return 1;
            }
        }
        else
        {
            return await _engine.DeleteDocumentAsync(_name, id, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// 异步删除多个文档
    /// </summary>
    /// <param name="ids">要删除的文档ID集合</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除的文档数量</returns>
    public async Task<int> DeleteAsync(IEnumerable<BsonValue> ids, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var deletedCount = 0;
        foreach (var id in ids)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (id != null && !id.IsNull)
            {
                deletedCount += await DeleteAsync(id, cancellationToken).ConfigureAwait(false);
            }
        }

        return deletedCount;
    }

    /// <summary>
    /// 异步删除所有文档
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除的文档数量</returns>
    public async Task<T?> FindByIdAsync(BsonValue id, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (id == null || id.IsNull) return null;

        var document = await _engine.FindByIdAsync(_name, id, cancellationToken).ConfigureAwait(false);
        if (document == null) return default;

        if (typeof(T) == typeof(BsonDocument))
        {
            var patched = _engine.MetadataManager.ApplySchemaDefaults(_name, document);
            return (T)(object)patched;
        }

        return AotBsonMapper.FromDocument<T>(document);
    }

    public async Task<List<T>> FindAllAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var documents = await _engine.FindAllAsync(_name, cancellationToken).ConfigureAwait(false);
        var results = new List<T>(documents.Count);

        if (typeof(T) == typeof(BsonDocument))
        {
            foreach (var document in documents)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var patched = _engine.MetadataManager.ApplySchemaDefaults(_name, document);
                results.Add((T)(object)patched);
            }

            return results;
        }

        foreach (var document in documents)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var entity = AotBsonMapper.FromDocument<T>(document);
            if (entity != null)
            {
                results.Add(entity);
            }
        }

        return results;
    }

    public async Task<List<T>> FindAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        return await FindAsync(predicate, 0, int.MaxValue, cancellationToken).ConfigureAwait(false);
    }

    public async Task<List<T>> FindAsync(Expression<Func<T, bool>> predicate, int skip, int limit, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        ValidatePaginationArguments(skip, limit);
        cancellationToken.ThrowIfCancellationRequested();
        if (limit == 0) return new List<T>();

        var shape = new QueryShape<T>
        {
            Predicate = predicate,
            PushedWhereCount = 1,
            Skip = skip > 0 ? skip : null,
            Take = limit < int.MaxValue ? limit : null
        };

        var results = limit == int.MaxValue
            ? new List<T>()
            : new List<T>(Math.Max(0, limit));

        await foreach (var item in _queryExecutor.ExecuteShapedAsync(_name, shape, out _, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            results.Add(item);
        }

        return results;
    }

    public async Task<T?> FindOneAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        cancellationToken.ThrowIfCancellationRequested();
        await foreach (var item in _queryExecutor.ExecuteAsync(_name, predicate, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            return item;
        }

        return default;
    }

    public Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        // 关键修复：如果在事务中，必须使用 FindAllAsync().Count 以包含挂起的操作
        if (_engine.GetCurrentTransaction() is Transaction transaction)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(_engine.GetTransactionalDocumentCount(_name, transaction));
        }

        return Task.FromResult((long)_engine.GetCachedDocumentCount(_name));
    }

    public async Task<long> CountAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        cancellationToken.ThrowIfCancellationRequested();
        long count = 0;
        await foreach (var _ in _queryExecutor.ExecuteAsync(_name, predicate, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            count++;
        }

        return count;
    }

    public async Task<bool> ExistsAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));
        cancellationToken.ThrowIfCancellationRequested();
        await foreach (var _ in _queryExecutor.ExecuteAsync(_name, predicate, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            return true;
        }

        return false;
    }

    public async Task<int> DeleteAllAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        var deletedCount = 0;
        foreach (var id in _engine.FindAllIds(_name))
        {
            cancellationToken.ThrowIfCancellationRequested();
            deletedCount += await DeleteAsync(id, cancellationToken).ConfigureAwait(false);
        }

        return deletedCount;
    }

    /// <summary>
    /// 异步根据条件删除文档
    /// </summary>
    /// <param name="predicate">查询条件</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>删除的文档数量</returns>
    public async Task<int> DeleteManyAsync(Expression<Func<T, bool>> predicate, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (predicate == null) throw new ArgumentNullException(nameof(predicate));

        var deletedCount = 0;
        await foreach (var entity in _queryExecutor.ExecuteFullTableScanAsync<T>(_name, predicate, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var id = GetEntityId(entity);
            if (id != null && !id.IsNull)
            {
                deletedCount += await DeleteAsync(id, cancellationToken).ConfigureAwait(false);
            }
        }

        return deletedCount;
    }

    /// <summary>
    /// 异步插入或更新文档（如果存在ID则更新，否则插入）
    /// </summary>
    /// <param name="entity">要插入或更新的实体</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>操作类型和影响的文档数量</returns>
    public async Task<(UpdateType UpdateType, int Count)> UpsertAsync(T entity, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        cancellationToken.ThrowIfCancellationRequested();
        var document = PrepareDocumentForInsert(entity);
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var id = document.TryGetValue("_id", out var documentId) ? documentId : BsonNull.Value;
            var existingDocument = id.IsNull
                ? null
                : await _engine.FindByIdAsync(_name, id, cancellationToken).ConfigureAwait(false);
            if (existingDocument == null)
            {
                ((Transaction)currentTransaction).RecordInsert(_name, document);
                return (UpdateType.Insert, 1);
            }

            ((Transaction)currentTransaction).RecordUpdate(_name, existingDocument, document);
            return (UpdateType.Update, 1);
        }

        return await _engine.UpsertDocumentAsync(_name, document, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 异步插入或更新多个文档
    /// </summary>
    /// <param name="entities">要插入或更新的实体集合</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>插入和更新的文档数量</returns>
    public async Task<(int InsertedCount, int UpdatedCount)> UpsertAsync(IEnumerable<T> entities, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var transactionInsertedCount = 0;
            var transactionUpdatedCount = 0;
            foreach (var entity in entities)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (entity == null) continue;

                var (updateType, count) = await UpsertAsync(entity, cancellationToken).ConfigureAwait(false);
                if (updateType == UpdateType.Insert)
                {
                    transactionInsertedCount += count;
                }
                else
                {
                    transactionUpdatedCount += count;
                }
            }

            return (transactionInsertedCount, transactionUpdatedCount);
        }

        const int BatchSize = 1000;
        var insertedCount = 0;
        var updatedCount = 0;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entity == null) continue;

            var document = PrepareDocumentForInsert(entity);
            docBatch.Add(document);

            if (docBatch.Count >= BatchSize)
            {
                var result = await _engine.UpsertDocumentsAsync(_name, docBatch, cancellationToken).ConfigureAwait(false);
                insertedCount += result.InsertedCount;
                updatedCount += result.UpdatedCount;
                docBatch.Clear();
            }
        }

        if (docBatch.Count > 0)
        {
            var result = await _engine.UpsertDocumentsAsync(_name, docBatch, cancellationToken).ConfigureAwait(false);
            insertedCount += result.InsertedCount;
            updatedCount += result.UpdatedCount;
        }

        return (insertedCount, updatedCount);
    }

    #endregion
}
