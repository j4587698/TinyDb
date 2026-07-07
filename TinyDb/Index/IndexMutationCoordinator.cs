using System.Buffers;
using TinyDb.Bson;

namespace TinyDb.Index;

internal static class IndexMutationCoordinator
{
    internal static void InsertDocument(IReadOnlyCollection<BTreeIndex> indexes, BsonDocument document, BsonValue documentId)
    {
        (BTreeIndex Index, IndexKey Key)[]? inserted = null;
        var insertedCount = 0;

        try
        {
            foreach (var index in indexes)
            {
                try
                {
                    var key = ExtractIndexKey(index, document);
                    if (key == null) continue;

                    if (!index.Insert(key, documentId))
                    {
                        throw new InvalidOperationException($"Duplicate key detected in unique index '{index.Name}'");
                    }

                    inserted ??= ArrayPool<(BTreeIndex Index, IndexKey Key)>.Shared.Rent(indexes.Count);
                    inserted[insertedCount++] = (index, key);
                }
                catch (InvalidOperationException ex)
                {
                    ThrowWithRollbackErrors(ex, RollbackInsertedIndexes(inserted, insertedCount, documentId));
                    throw;
                }
                catch (Exception ex)
                {
                    var insertException = new InvalidOperationException($"Failed to insert into index '{index.Name}': {ex.Message}", ex);
                    ThrowWithRollbackErrors(insertException, RollbackInsertedIndexes(inserted, insertedCount, documentId));
                    throw insertException;
                }
            }
        }
        finally
        {
            if (inserted != null)
            {
                ArrayPool<(BTreeIndex Index, IndexKey Key)>.Shared.Return(inserted, clearArray: true);
            }
        }
    }

    internal static void RebuildIndex(BTreeIndex index, IEnumerable<BsonDocument> documents)
    {
        foreach (var document in documents)
        {
            if (document == null) continue;
            if (!document.TryGetValue("_id", out var documentId) || documentId == null || documentId.IsNull) continue;

            var key = ExtractIndexKey(index, document);
            if (key != null && !index.Insert(key, documentId))
            {
                throw new InvalidOperationException($"Duplicate key detected in unique index '{index.Name}' while rebuilding index");
            }
        }
    }

    internal static void UpdateDocument(IReadOnlyCollection<BTreeIndex> indexes, BsonDocument oldDoc, BsonDocument newDoc, BsonValue id)
    {
        var applied = new List<(BTreeIndex Index, IndexKey? OldKey, IndexKey? NewKey)>();
        foreach (var index in indexes)
        {
            var oldKey = ExtractIndexKey(index, oldDoc);
            var newKey = ExtractIndexKey(index, newDoc);

            if (oldKey != null && newKey != null && oldKey.Equals(newKey))
            {
                continue;
            }

            var oldDeleted = false;
            var newInserted = false;
            try
            {
                if (oldKey != null)
                {
                    oldDeleted = index.Delete(oldKey, id);
                }

                if (newKey != null)
                {
                    if (!index.Insert(newKey, id))
                    {
                        throw new InvalidOperationException($"Duplicate key detected in unique index '{index.Name}'");
                    }

                    newInserted = true;
                }

                applied.Add((index, oldKey, newKey));

                oldDeleted = false;
                newInserted = false;
            }
            catch (Exception ex)
            {
                var rollbackErrors = new List<Exception>();
                if (newInserted && newKey != null)
                {
                    try
                    {
                        index.Delete(newKey, id);
                    }
                    catch (Exception rollbackEx)
                    {
                        rollbackErrors.Add(new InvalidOperationException(
                            $"Failed to rollback inserted key in index '{index.Name}'.",
                            rollbackEx));
                    }
                }

                if (oldDeleted && oldKey != null)
                {
                    try
                    {
                        if (!index.Insert(oldKey, id))
                        {
                            throw new InvalidOperationException($"Failed to restore old key in index '{index.Name}'.");
                        }
                    }
                    catch (Exception rollbackEx)
                    {
                        rollbackErrors.Add(new InvalidOperationException(
                            $"Failed to rollback deleted key in index '{index.Name}'.",
                            rollbackEx));
                    }
                }

                rollbackErrors.AddRange(RollbackUpdatedIndexes(applied, id));
                ThrowWithRollbackErrors(ex, rollbackErrors);
                throw;
            }
        }
    }

    internal static void DeleteDocument(IEnumerable<BTreeIndex> indexes, BsonDocument doc, BsonValue id)
    {
        foreach (var index in indexes)
        {
            var key = ExtractIndexKey(index, doc);
            if (key != null) index.Delete(key, id);
        }
    }

    internal static IndexKey? ExtractIndexKey(BTreeIndex index, BsonDocument doc)
    {
        if (index.Fields.Count == 1)
        {
            if (doc.TryGetValue(index.Fields[0], out var val))
            {
                return IndexKey.Create(val);
            }

            return index.IsSparse ? null : IndexKey.Create(BsonNull.Value);
        }

        var values = new BsonValue[index.Fields.Count];
        for (var i = 0; i < index.Fields.Count; i++)
        {
            if (doc.TryGetValue(index.Fields[i], out var val))
            {
                values[i] = val;
                continue;
            }

            if (index.IsSparse) return null;
            values[i] = BsonNull.Value;
        }

        return new IndexKey(values);
    }

    private static List<Exception> RollbackInsertedIndexes(
        (BTreeIndex Index, IndexKey Key)[]? inserted,
        int insertedCount,
        BsonValue documentId)
    {
        var errors = new List<Exception>();
        if (inserted == null) return errors;

        for (var i = insertedCount - 1; i >= 0; i--)
        {
            try
            {
                inserted[i].Index.Delete(inserted[i].Key, documentId);
            }
            catch (Exception ex)
            {
                errors.Add(new InvalidOperationException(
                    $"Failed to rollback inserted key in index '{inserted[i].Index.Name}'.",
                    ex));
            }
        }

        return errors;
    }

    private static List<Exception> RollbackUpdatedIndexes(
        List<(BTreeIndex Index, IndexKey? OldKey, IndexKey? NewKey)> applied,
        BsonValue documentId)
    {
        var errors = new List<Exception>();
        for (var i = applied.Count - 1; i >= 0; i--)
        {
            var (index, oldKey, newKey) = applied[i];
            try
            {
                if (newKey != null) index.Delete(newKey, documentId);
                if (oldKey != null && !index.Insert(oldKey, documentId))
                {
                    throw new InvalidOperationException($"Failed to rollback index '{index.Name}'");
                }
            }
            catch (Exception ex)
            {
                errors.Add(new InvalidOperationException(
                    $"Failed to rollback update in index '{index.Name}'.",
                    ex));
            }
        }

        return errors;
    }

    private static void ThrowWithRollbackErrors(Exception original, List<Exception> rollbackErrors)
    {
        if (rollbackErrors.Count == 0) return;

        var errors = new List<Exception> { original };
        errors.AddRange(rollbackErrors);
        throw new AggregateException("Index operation failed and rollback encountered errors.", errors);
    }
}
