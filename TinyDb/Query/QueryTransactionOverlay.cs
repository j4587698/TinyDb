using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Core;

namespace TinyDb.Query;

internal static class QueryTransactionOverlay
{
    public static bool TryGetDocument(Transaction tx, string collectionName, BsonValue id, out BsonDocument? document)
    {
        document = null;
        var operations = tx.GetOperationsSnapshot();
        if (operations.Length == 0) return false;

        for (int i = operations.Length - 1; i >= 0; i--)
        {
            var op = operations[i];
            if (op.CollectionName != collectionName) continue;
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;
            if (!BsonValueComparer.ValueEquals(op.DocumentId, id)) continue;

            if (op.OperationType == TransactionOperationType.Delete)
            {
                document = null;
                return true;
            }

            if (op.NewDocument != null)
            {
                document = op.NewDocument;
                return true;
            }

            document = null;
            return true;
        }

        return false;
    }

    public static Dictionary<BsonValue, BsonDocument?>? Build(Transaction tx, string collectionName)
    {
        Dictionary<BsonValue, BsonDocument?>? overlay = null;

        foreach (var op in tx.GetOperationsSnapshot())
        {
            if (op.CollectionName != collectionName) continue;
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;

            overlay ??= new Dictionary<BsonValue, BsonDocument?>(BsonValueComparer.EqualityComparer);

            if (op.OperationType == TransactionOperationType.Delete)
            {
                overlay[op.DocumentId] = null;
            }
            else if (op.NewDocument != null)
            {
                overlay[op.DocumentId] = op.NewDocument;
            }
        }

        return overlay is { Count: > 0 } ? overlay : null;
    }
}
