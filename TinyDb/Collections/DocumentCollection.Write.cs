using System;
using System.Collections.Generic;
using System.Threading;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;

namespace TinyDb.Collections;

public sealed partial class DocumentCollection<T> where T : class
{
    /// <summary>
    /// 插入单个文档
    /// </summary>
    /// <param name="entity">要插入的实体</param>
    /// <returns>插入文档的ID</returns>
    public BsonValue Insert(T entity)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var document = PrepareDocumentForInsert(entity);

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，记录操作而不是直接写入
            return ((Transaction)currentTransaction).RecordInsert(_name, document);
        }
        else
        {
            // 不在事务中，直接插入到数据库
            return _engine.InsertDocument(_name, document);
        }
    }

    /// <summary>
    /// 插入多个文档
    /// </summary>
    /// <param name="entities">要插入的实体集合</param>
    /// <returns>插入的文档数量</returns>
    public int Insert(IEnumerable<T> entities)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            return InsertInTransaction(entities, (Transaction)currentTransaction);
        }

        int totalInserted = 0;
        const int BatchSize = 1000;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            if (entity == null) continue;

            var document = PrepareDocumentForInsert(entity);

            docBatch.Add(document);

            if (docBatch.Count >= BatchSize)
            {
                totalInserted += InsertDocumentBatch(docBatch);
                docBatch.Clear();
            }
        }

        if (docBatch.Count > 0)
        {
            totalInserted += InsertDocumentBatch(docBatch);
        }

        return totalInserted;
    }

    private int InsertDocumentBatch(List<BsonDocument> documents)
    {
        if (documents.Count == 0) return 0;

        // 批量插入到数据库
        return _engine.InsertDocuments(_name, documents);
    }

    private int InsertInTransaction(IEnumerable<T> entities, Transaction transaction, CancellationToken cancellationToken = default)
    {
        var insertedCount = 0;
        foreach (var entity in entities)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (entity == null) continue;

            var document = PrepareDocumentForInsert(entity);
            var documentId = transaction.RecordInsert(_name, document);
            insertedCount++;
        }

        return insertedCount;
    }

    private BsonDocument PrepareDocumentForInsert(T entity)
    {
        EnsureEntityHasId(entity);

        var document = AotBsonMapper.ToDocument(entity);
        if (!document.ContainsKey("_id"))
        {
            var newId = ObjectId.NewObjectId();
            document = document.Set("_id", newId);
            UpdateEntityId(entity, newId);
        }

        return document;
    }

    private BsonDocument PrepareDocumentForUpdate(T entity, out BsonValue id)
    {
        if (!AotIdAccessor<T>.HasValidId(entity))
        {
            throw new ArgumentException("Entity must have a valid ID for update", nameof(entity));
        }

        id = AotIdAccessor<T>.GetId(entity);
        return AotBsonMapper.ToDocument(entity);
    }

    /// <summary>
    /// 更新文档
    /// </summary>
    /// <param name="entity">要更新的实体</param>
    /// <returns>更新的文档数量</returns>
    public int Update(T entity)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var document = PrepareDocumentForUpdate(entity, out var id);

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，需要先获取原始文档用于回滚
            var originalDocument = _engine.FindById(_name, id);
            if (originalDocument == null)
            {
                // 如果原始文档不存在，这是插入操作
                return 0;
            }
            else
            {
                // 记录更新操作
                ((Transaction)currentTransaction).RecordUpdate(_name, originalDocument, document);
                return 1;
            }
        }
        else
        {
            // 不在事务中，直接更新到数据库
            return _engine.UpdateDocument(_name, document);
        }
    }

    /// <summary>
    /// 更新多个文档
    /// </summary>
    /// <param name="entities">要更新的实体集合</param>
    /// <returns>更新的文档数量</returns>
    public int Update(IEnumerable<T> entities)
    {
        ThrowIfDisposed();
        if (entities == null) throw new ArgumentNullException(nameof(entities));

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            return UpdateInTransaction(entities, (Transaction)currentTransaction);
        }

        int updatedCount = 0;
        const int BatchSize = 1000;
        var docBatch = new List<BsonDocument>(BatchSize);

        foreach (var entity in entities)
        {
            if (entity != null)
            {
                var document = PrepareDocumentForUpdate(entity, out _);
                docBatch.Add(document);

                if (docBatch.Count >= BatchSize)
                {
                    updatedCount += _engine.UpdateDocuments(_name, docBatch);
                    docBatch.Clear();
                }
            }
        }

        if (docBatch.Count > 0)
        {
            updatedCount += _engine.UpdateDocuments(_name, docBatch);
        }

        return updatedCount;
    }

    private int UpdateInTransaction(IEnumerable<T> entities, Transaction transaction)
    {
        var prepared = new List<(BsonDocument Document, BsonValue Id)>();
        var ids = new List<BsonValue>();

        foreach (var entity in entities)
        {
            if (entity == null) continue;

            var document = PrepareDocumentForUpdate(entity, out var id);
            prepared.Add((document, id));
            ids.Add(id);
        }

        if (prepared.Count == 0)
        {
            return 0;
        }

        var originalDocuments = _engine.FindByIds(_name, ids);
        return RecordPreparedUpdatesInTransaction(prepared, originalDocuments, transaction);
    }

    private int RecordPreparedUpdatesInTransaction(
        IReadOnlyList<(BsonDocument Document, BsonValue Id)> prepared,
        IReadOnlyList<BsonDocument?> originalDocuments,
        Transaction transaction)
    {
        var currentDocuments = new Dictionary<BsonValue, BsonDocument?>(BsonValueComparer.EqualityComparer);
        for (int i = 0; i < prepared.Count; i++)
        {
            currentDocuments.TryAdd(prepared[i].Id, originalDocuments[i]);
        }

        var updatedCount = 0;
        foreach (var (document, id) in prepared)
        {
            currentDocuments.TryGetValue(id, out var originalDocument);
            if (originalDocument == null)
            {
                continue;
            }
            else
            {
                transaction.RecordUpdate(_name, originalDocument, document);
            }

            currentDocuments[id] = document;
            updatedCount++;
        }

        return updatedCount;
    }

    /// <summary>
    /// 删除文档
    /// </summary>
    /// <param name="id">要删除的文档ID</param>
    /// <returns>删除的文档数量</returns>
    public int Delete(BsonValue id)
    {
        ThrowIfDisposed();
        if (id == null || id.IsNull) return 0;

        // 检查是否在事务中
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            // 在事务中，需要先获取要删除的文档用于回滚
            var documentToDelete = _engine.FindById(_name, id);
            if (documentToDelete == null)
            {
                return 0; // 文档不存在，无需删除
            }
            else
            {
                // 记录删除操作
                ((Transaction)currentTransaction).RecordDelete(_name, documentToDelete);
                return 1;
            }
        }
        else
        {
            // 不在事务中，直接删除
            return _engine.DeleteDocument(_name, id);
        }
    }

    /// <summary>
    /// 删除多个文档
    /// </summary>
    /// <param name="ids">要删除的文档ID集合</param>
    /// <returns>删除的文档数量</returns>
    public int Delete(IEnumerable<BsonValue> ids)
    {
        ThrowIfDisposed();
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var idList = ids
            .Where(static id => id != null && !id.IsNull)
            .Distinct(BsonValueComparer.EqualityComparer)
            .ToArray();
        if (idList.Length == 0)
        {
            return 0;
        }

        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction == null)
        {
            return _engine.DeleteDocuments(_name, idList);
        }

        var documents = _engine.FindByIds(_name, idList);
        var deletedCount = 0;
        for (var i = 0; i < documents.Count; i++)
        {
            var documentToDelete = documents[i];
            if (documentToDelete == null)
            {
                continue;
            }

            ((Transaction)currentTransaction).RecordDelete(_name, documentToDelete);
            deletedCount++;
        }

        return deletedCount;
    }

}
