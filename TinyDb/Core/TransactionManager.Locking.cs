using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Metadata;
using System.Collections.Concurrent;

namespace TinyDb.Core;

public sealed partial class TransactionManager
{

    private IEnumerable<string> GetTransactionLockCollectionNames(IReadOnlyList<TransactionOperation> operations)
    {
        var collectionNames = new HashSet<string>(StringComparer.Ordinal);

        foreach (var operation in operations)
        {
            if (!string.IsNullOrWhiteSpace(operation.CollectionName))
            {
                collectionNames.Add(operation.CollectionName);
            }

            if (operation.NewDocument == null ||
                operation.OperationType is not (TransactionOperationType.Insert or TransactionOperationType.Update))
            {
                continue;
            }

            foreach (var foreignKey in GetForeignKeyDefinitions(operation.CollectionName))
            {
                if (!string.IsNullOrWhiteSpace(foreignKey.ReferencedCollection))
                {
                    collectionNames.Add(foreignKey.ReferencedCollection);
                }
            }
        }

        return collectionNames;
    }


    private IEnumerable<CollectionDocumentLockKey> GetTransactionDocumentLockKeys(IReadOnlyList<TransactionOperation> operations)
    {
        foreach (var operation in operations)
        {
            if (operation.DocumentId != null &&
                !operation.DocumentId.IsNull &&
                operation.OperationType is TransactionOperationType.Insert or TransactionOperationType.Update or TransactionOperationType.Delete)
            {
                yield return new CollectionDocumentLockKey(operation.CollectionName, operation.DocumentId);
            }

            if (operation.NewDocument == null ||
                operation.OperationType is not (TransactionOperationType.Insert or TransactionOperationType.Update))
            {
                continue;
            }

            foreach (var foreignKey in GetForeignKeyDefinitions(operation.CollectionName))
            {
                if (string.IsNullOrWhiteSpace(foreignKey.ReferencedCollection)) continue;

                if (TryGetForeignKeyValue(operation.NewDocument, foreignKey.FieldName, out var foreignKeyValue))
                {
                    yield return new CollectionDocumentLockKey(foreignKey.ReferencedCollection, foreignKeyValue);
                }
            }
        }
    }

}
