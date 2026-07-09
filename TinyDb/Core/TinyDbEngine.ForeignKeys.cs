using System.Collections.Generic;
using TinyDb.Bson;

namespace TinyDb.Core;

public sealed partial class TinyDbEngine
{
    private IEnumerable<string> GetWriteGateCollectionsForForeignKeys(string collectionName)
    {
        yield return collectionName;

        foreach (var foreignKey in _transactionManager.GetForeignKeyDefinitions(collectionName))
        {
            if (!string.IsNullOrWhiteSpace(foreignKey.ReferencedCollection))
            {
                yield return foreignKey.ReferencedCollection;
            }
        }
    }

    private void ValidateDocumentForWrite(string collectionName, BsonDocument document)
    {
        _metadataManager.ValidateDocumentForWrite(collectionName, document, _options.SchemaValidationMode);
        ValidateForeignKeysForDocumentWrite(collectionName, document);
    }

    private void ValidateForeignKeysForDocumentWrite(string collectionName, BsonDocument document)
    {
        if (GetCurrentTransaction() != null)
        {
            return;
        }

        var foreignKeys = _transactionManager.GetForeignKeyDefinitions(collectionName);
        if (foreignKeys.Count == 0)
        {
            return;
        }

        foreach (var foreignKey in foreignKeys)
        {
            if (!TransactionManager.TryGetForeignKeyValue(document, foreignKey.FieldName, out var foreignKeyValue))
            {
                continue;
            }

            BsonDocument? referencedDocument;
            using (SuppressCurrentTransaction())
            {
                referencedDocument = FindCommittedById(foreignKey.ReferencedCollection, foreignKeyValue);
            }

            if (referencedDocument == null)
            {
                throw new InvalidOperationException(
                    $"Foreign key constraint violation: Field '{foreignKey.FieldName}' in collection '{collectionName}' references non-existent document ID '{foreignKeyValue}' in collection '{foreignKey.ReferencedCollection}'.");
            }
        }
    }
}
