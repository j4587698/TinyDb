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
    internal int UpdateDocumentInternal(string col, BsonDocument d) => UpdateDocument(col, d);
    internal int DeleteDocumentInternal(string col, BsonValue id) => DeleteDocument(col, id);
    public int GetCachedDocumentCount(string col) => GetCollectionState(col).Index.Count;

    internal long GetTransactionalDocumentCount(string col, Transaction transaction)
    {
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));

        var operations = transaction.GetOperationsSnapshot()
            .Where(op => op.CollectionName == col &&
                         op.DocumentId != null &&
                         !op.DocumentId.IsNull &&
                         op.OperationType is TransactionOperationType.Insert or TransactionOperationType.Update or TransactionOperationType.Delete)
            .ToArray();

        if (operations.Length == 0)
        {
            return GetCachedDocumentCount(col);
        }

        var initialExistsById = new Dictionary<BsonValue, bool>(BsonValueComparer.EqualityComparer);
        var currentExistsById = new Dictionary<BsonValue, bool>(BsonValueComparer.EqualityComparer);

        foreach (var operation in operations)
        {
            var id = operation.DocumentId!;
            if (!currentExistsById.TryGetValue(id, out var exists))
            {
                exists = FindCommittedById(col, id) != null;
                initialExistsById[id] = exists;
            }

            currentExistsById[id] = operation.OperationType switch
            {
                TransactionOperationType.Insert => true,
                TransactionOperationType.Delete => false,
                _ => exists
            };
        }

        long delta = 0;
        foreach (var (id, currentExists) in currentExistsById)
        {
            var initiallyExists = initialExistsById[id];
            if (currentExists && !initiallyExists) delta++;
            else if (!currentExists && initiallyExists) delta--;
        }

        return GetCachedDocumentCount(col) + delta;
    }
}
