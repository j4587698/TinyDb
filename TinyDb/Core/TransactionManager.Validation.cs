using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Metadata;
using System.Collections.Concurrent;

namespace TinyDb.Core;

public sealed partial class TransactionManager
{

    /// <summary>
    /// 验证操作
    /// </summary>
    /// <param name="operations">事务操作快照</param>
    private void ValidateOperations(IReadOnlyList<TransactionOperation> operations)
    {
        // 检查重复的文档ID插入（只检查非null ID的重复）
        var insertedIds = new HashSet<(string CollectionName, BsonValue DocumentId)>();
        foreach (var op in operations)
        {
            if (op.OperationType != TransactionOperationType.Insert ||
                op.DocumentId == null)
            {
                continue;
            }

            if (!insertedIds.Add((op.CollectionName, op.DocumentId)))
            {
                throw new InvalidOperationException("Duplicate document IDs detected in transaction");
            }
        }

        // 检查外键约束
        ValidateForeignKeys(operations);
        ValidateWriteConflicts(operations);
    }


    private void ValidateForeignKeys(IReadOnlyList<TransactionOperation> transactionOperations)
    {
        var operations = transactionOperations
            .Where(op => op.OperationType == TransactionOperationType.Insert || op.OperationType == TransactionOperationType.Update)
            .ToList();

        foreach (var op in operations)
        {
            if (op.NewDocument == null) continue;

            var foreignKeys = GetForeignKeyDefinitions(op.CollectionName);
            if (foreignKeys.Count == 0) continue;

            foreach (var fk in foreignKeys)
            {
                if (TryGetForeignKeyValue(op.NewDocument, fk.FieldName, out var fkValue))
                {
                    var pendingExistence = GetPendingDocumentExistence(transactionOperations, fk.ReferencedCollection, fkValue!);
                    if (pendingExistence == true)
                    {
                        continue;
                    }

                    if (pendingExistence == false)
                    {
                        throw new InvalidOperationException($"Foreign key constraint violation: Field '{fk.FieldName}' in collection '{op.CollectionName}' references non-existent document ID '{fkValue}' in collection '{fk.ReferencedCollection}'.");
                    }

                    BsonDocument? referencedDoc;
                    using (_engine.SuppressCurrentTransaction())
                    {
                        referencedDoc = _engine.FindById(fk.ReferencedCollection, fkValue!);
                    }

                    if (referencedDoc == null)
                    {
                         throw new InvalidOperationException($"Foreign key constraint violation: Field '{fk.FieldName}' in collection '{op.CollectionName}' references non-existent document ID '{fkValue}' in collection '{fk.ReferencedCollection}'.");
                    }
                }
                else
                {
                    // FK field missing, skip validation (nullable FK)
                }
            }
        }
    }


    private static bool? GetPendingDocumentExistence(
        IReadOnlyList<TransactionOperation> operations,
        string collectionName,
        BsonValue documentId)
    {
        bool? exists = null;

        foreach (var operation in operations)
        {
            if (!string.Equals(operation.CollectionName, collectionName, StringComparison.Ordinal) ||
                operation.DocumentId == null ||
                !BsonValuesValueEqual(operation.DocumentId, documentId))
            {
                continue;
            }

            exists = operation.OperationType switch
            {
                TransactionOperationType.Insert or TransactionOperationType.Update => true,
                TransactionOperationType.Delete => false,
                _ => exists
            };
        }

        return exists;
    }


    private static bool TryGetForeignKeyValue(BsonDocument document, string fieldName, out BsonValue value)
    {
        if (document.TryGetValue(fieldName, out var directValue) && directValue != null && !directValue.IsNull)
        {
            value = directValue;
            return true;
        }

        var camelName = BsonFieldName.ToCamelCase(fieldName);
        if (camelName != fieldName &&
            document.TryGetValue(camelName, out var camelValue) &&
            camelValue != null &&
            !camelValue.IsNull)
        {
            value = camelValue;
            return true;
        }

        value = BsonNull.Value;
        return false;
    }


    private void ValidateWriteConflicts(IReadOnlyList<TransactionOperation> operations)
    {
        var checkedDocuments = new HashSet<string>(StringComparer.Ordinal);

        foreach (var op in operations)
        {
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;
            if (op.OperationType is not (TransactionOperationType.Insert or TransactionOperationType.Update or TransactionOperationType.Delete)) continue;

            var key = $"{op.CollectionName}\0{op.DocumentId}";
            if (!checkedDocuments.Add(key)) continue;

            var committedDocument = _engine.FindCommittedById(op.CollectionName, op.DocumentId);

            if (op.OperationType == TransactionOperationType.Insert)
            {
                // 插入冲突由提交阶段的主键和唯一索引约束兜底。这里提前读取会和事务内可见性路径互相干扰，
                // 将未提交插入误判为已提交文档。
                continue;
            }

            if (op.OriginalDocument == null)
            {
                if (committedDocument != null)
                {
                    throw new InvalidOperationException($"Write conflict: document '{op.DocumentId}' changed in collection '{op.CollectionName}'.");
                }

                continue;
            }

            if (committedDocument == null || !BsonDocumentsValueEqual(committedDocument, op.OriginalDocument))
            {
                throw new InvalidOperationException($"Write conflict: document '{op.DocumentId}' changed in collection '{op.CollectionName}'.");
            }
        }
    }


    private static bool BsonDocumentsValueEqual(BsonDocument? left, BsonDocument? right)
    {
        if (ReferenceEquals(left, right)) return true;
        if (left is null || right is null || left.Count != right.Count) return false;

        foreach (var (key, leftValue) in left)
        {
            if (!right.TryGetValue(key, out var rightValue)) return false;
            if (!BsonValuesValueEqual(leftValue, rightValue)) return false;
        }

        return true;
    }


    private static bool BsonArraysValueEqual(BsonArray left, BsonArray right)
    {
        if (left.Count != right.Count) return false;

        for (int i = 0; i < left.Count; i++)
        {
            if (!BsonValuesValueEqual(left[i], right[i])) return false;
        }

        return true;
    }


    private static bool BsonValuesValueEqual(BsonValue? left, BsonValue? right)
    {
        if (ReferenceEquals(left, right)) return true;
        if (left is BsonDocument leftDocument && right is BsonDocument rightDocument)
        {
            return BsonDocumentsValueEqual(leftDocument, rightDocument);
        }

        if (left is BsonArray leftArray && right is BsonArray rightArray)
        {
            return BsonArraysValueEqual(leftArray, rightArray);
        }

        return BsonValueComparer.ValueEquals(left, right);
    }


    internal List<ForeignKeyDefinition> GetForeignKeyDefinitions(string collectionName)
    {
        var lazy = _foreignKeyCache.GetOrAdd(
            collectionName,
            name => new Lazy<List<ForeignKeyDefinition>>(
                () => LoadForeignKeyDefinitions(name),
                LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            return lazy.Value;
        }
        catch
        {
            if (_foreignKeyCache.TryGetValue(collectionName, out var current) &&
                ReferenceEquals(current, lazy))
            {
                _foreignKeyCache.TryRemove(collectionName, out _);
            }

            throw;
        }
    }


    private List<ForeignKeyDefinition> LoadForeignKeyDefinitions(string name)
    {
        var definitions = new List<ForeignKeyDefinition>();

        // 扫描所有元数据集合以找到匹配的集合名称
        var metadataCollectionNames = new List<string> { "__sys_catalog" };

        foreach (var metaColName in metadataCollectionNames)
        {
            try
            {
                using var _ = _engine.SuppressCurrentTransaction();
                var metaCol = _engine.GetCollection<MetadataDocument>(metaColName);
                var metaDoc = metaCol.FindById(name);

                if (metaDoc != null)
                {
                    var entityMeta = metaDoc.ToEntityMetadata();
                    foreach (var prop in entityMeta.Properties)
                    {
                        if (!string.IsNullOrEmpty(prop.ForeignKeyCollection))
                        {
                            definitions.Add(new ForeignKeyDefinition(prop.PropertyName, prop.ForeignKeyCollection!));
                        }
                    }

                    break;
                }
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to read foreign key metadata for collection '{name}' from '{metaColName}'.", ex);
            }
        }

        return definitions;
    }


    internal void ClearForeignKeyCache()
    {
        _foreignKeyCache.Clear();
    }

}
