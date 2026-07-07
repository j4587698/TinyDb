using System;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Core;

namespace TinyDb.Collections;

public sealed partial class DocumentCollection<T> where T : class
{
    /// <summary>
    /// 插入或更新文档（如果存在ID则更新，否则插入）
    /// </summary>
    /// <param name="entity">要插入或更新的实体</param>
    /// <returns>操作类型和影响的文档数量</returns>
    public (UpdateType UpdateType, int Count) Upsert(T entity)
    {
        ThrowIfDisposed();
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var document = PrepareDocumentForInsert(entity);
        var currentTransaction = _engine.GetCurrentTransaction();
        if (currentTransaction != null)
        {
            var id = document.TryGetValue("_id", out var documentId) ? documentId : BsonNull.Value;
            var existingDocument = id.IsNull ? null : _engine.FindById(_name, id);
            if (existingDocument == null)
            {
                ((Transaction)currentTransaction).RecordInsert(_name, document);
                return (UpdateType.Insert, 1);
            }

            ((Transaction)currentTransaction).RecordUpdate(_name, existingDocument, document);
            return (UpdateType.Update, 1);
        }

        return _engine.UpsertDocument(_name, document);
    }

    /// <summary>
    /// 插入或更新多个文档
    /// </summary>
    /// <param name="entities">要插入或更新的实体集合</param>
    /// <returns>插入和更新的文档数量</returns>
    public (int InsertedCount, int UpdatedCount) Upsert(IEnumerable<T> entities)
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
                if (entity == null) continue;

                var (updateType, count) = Upsert(entity);
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
            if (entity == null) continue;

            var document = PrepareDocumentForInsert(entity);
            docBatch.Add(document);

            if (docBatch.Count >= BatchSize)
            {
                var result = _engine.UpsertDocuments(_name, docBatch);
                insertedCount += result.InsertedCount;
                updatedCount += result.UpdatedCount;
                docBatch.Clear();
            }
        }

        if (docBatch.Count > 0)
        {
            var result = _engine.UpsertDocuments(_name, docBatch);
            insertedCount += result.InsertedCount;
            updatedCount += result.UpdatedCount;
        }

        return (insertedCount, updatedCount);
    }

    /// <summary>
    /// 确保实体有ID
    /// </summary>
    /// <param name="entity">实体</param>

}
