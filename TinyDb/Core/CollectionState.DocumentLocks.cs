using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;

namespace TinyDb.Core;

internal sealed partial class CollectionState
{
    public IDisposable EnterDocumentLock(BsonValue id)
    {
        if (id == null || id.IsNull)
        {
            return EmptyLockScope.Instance;
        }

        return new SingleKeyedDocumentLockScope(this, AcquireDocumentLockReference(id));
    }

    public IDisposable EnterDocumentLocks(IEnumerable<BsonValue> ids)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));
        return ids is IReadOnlyList<BsonValue> list
            ? EnterDocumentLocksCore(list, static id => id)
            : EnterDocumentLocksCore(ids, static id => id);
    }

    public IDisposable EnterDocumentLocks<T>(IReadOnlyList<T> items, Func<T, BsonValue> idSelector)
    {
        return EnterDocumentLocksCore(items, idSelector);
    }

    public ValueTask<IDisposable> EnterDocumentLockAsync(BsonValue id, CancellationToken cancellationToken = default)
    {
        if (id == null || id.IsNull)
        {
            return new ValueTask<IDisposable>(EmptyLockScope.Instance);
        }

        var owner = GetOrCreateDocumentLockOwner();
        return SingleAsyncKeyedDocumentLockScope.EnterAsync(this, AcquireDocumentLockReference(id), owner, cancellationToken);
    }

    public async ValueTask<IDisposable> EnterDocumentLocksAsync(
        IEnumerable<BsonValue> ids,
        CancellationToken cancellationToken = default)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));
        var locks = ids is IReadOnlyList<BsonValue> list
            ? CreateDocumentLockReferences(list, static id => id, out var singleLock)
            : CreateDocumentLockReferences(ids, static id => id, out singleLock);

        var owner = CreateDocumentLockOwnerIfNeeded(locks, singleLock);
        return await EnterDocumentLocksAsyncCore(locks, singleLock, owner, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<IDisposable> EnterDocumentLocksAsync<T>(
        IReadOnlyList<T> items,
        Func<T, BsonValue> idSelector,
        CancellationToken cancellationToken = default)
    {
        var locks = CreateDocumentLockReferences(items, idSelector, out var singleLock);
        var owner = CreateDocumentLockOwnerIfNeeded(locks, singleLock);
        return EnterDocumentLocksAsyncCore(locks, singleLock, owner, cancellationToken);
    }

    private IDisposable EnterDocumentLocksCore<T>(IEnumerable<T> items, Func<T, BsonValue> idSelector)
    {
        var locks = CreateDocumentLockReferences(items, idSelector, out var singleLock);
        return CreateDocumentLockScope(locks, singleLock);
    }

    private IDisposable EnterDocumentLocksCore<T>(IReadOnlyList<T> items, Func<T, BsonValue> idSelector)
    {
        var locks = CreateDocumentLockReferences(items, idSelector, out var singleLock);
        return CreateDocumentLockScope(locks, singleLock);
    }

    private IDisposable CreateDocumentLockScope(KeyedDocumentLock[]? locks, KeyedDocumentLock? singleLock)
    {
        if (singleLock != null)
        {
            return new SingleKeyedDocumentLockScope(this, singleLock);
        }

        return locks == null || locks.Length == 0
            ? EmptyLockScope.Instance
            : new KeyedDocumentLockScope(this, locks);
    }

    private async ValueTask<IDisposable> EnterDocumentLocksAsyncCore(
        KeyedDocumentLock[]? locks,
        KeyedDocumentLock? singleLock,
        DocumentLockOwner? owner,
        CancellationToken cancellationToken)
    {
        if (singleLock != null)
        {
            return await SingleAsyncKeyedDocumentLockScope.EnterAsync(this, singleLock, owner!, cancellationToken)
                .ConfigureAwait(false);
        }

        if (locks == null || locks.Length == 0)
        {
            return EmptyLockScope.Instance;
        }

        return await AsyncKeyedDocumentLockScope.EnterAsync(this, locks, owner!, cancellationToken).ConfigureAwait(false);
    }

    private KeyedDocumentLock[]? CreateDocumentLockReferences<T>(
        IReadOnlyList<T> items,
        Func<T, BsonValue> idSelector,
        out KeyedDocumentLock? singleLock)
    {
        if (items == null) throw new ArgumentNullException(nameof(items));
        if (idSelector == null) throw new ArgumentNullException(nameof(idSelector));

        var keys = CollectDistinctDocumentKeys(items, idSelector, items.Count, out var singleKey);
        return CreateDocumentLockReferences(keys, singleKey, out singleLock);
    }

    private KeyedDocumentLock[]? CreateDocumentLockReferences<T>(
        IEnumerable<T> items,
        Func<T, BsonValue> idSelector,
        out KeyedDocumentLock? singleLock)
    {
        if (items == null) throw new ArgumentNullException(nameof(items));
        if (idSelector == null) throw new ArgumentNullException(nameof(idSelector));

        var keys = CollectDistinctDocumentKeys(items, idSelector, 0, out var singleKey);
        return CreateDocumentLockReferences(keys, singleKey, out singleLock);
    }

    private KeyedDocumentLock[]? CreateDocumentLockReferences(
        List<BsonValue>? keys,
        BsonValue? singleKey,
        out KeyedDocumentLock? singleLock)
    {
        singleLock = null;

        if (keys == null)
        {
            if (singleKey == null)
            {
                return null;
            }

            singleLock = AcquireDocumentLockReference(singleKey);
            return null;
        }

        if (keys.Count == 0)
        {
            return null;
        }

        if (keys.Count == 1)
        {
            singleLock = AcquireDocumentLockReference(keys[0]);
            return null;
        }

        keys.Sort(BsonValueSortComparer.Instance);
        var locks = new KeyedDocumentLock[keys.Count];
        for (var i = 0; i < keys.Count; i++)
        {
            locks[i] = AcquireDocumentLockReference(keys[i]);
        }

        return locks;
    }

    private static List<BsonValue>? CollectDistinctDocumentKeys<T>(
        IEnumerable<T> items,
        Func<T, BsonValue> idSelector,
        int capacity,
        out BsonValue? singleKey)
    {
        singleKey = null;
        List<BsonValue>? keys = null;
        HashSet<BsonValue>? seen = null;

        foreach (var item in items)
        {
            var key = idSelector(item);
            if (key == null || key.IsNull)
            {
                continue;
            }

            if (singleKey == null)
            {
                singleKey = key;
                continue;
            }

            if (seen == null)
            {
                seen = new HashSet<BsonValue>(BsonValueComparer.EqualityComparer) { singleKey };
                keys = capacity > 0 ? new List<BsonValue>(capacity) : new List<BsonValue>();
                keys.Add(singleKey);
            }

            if (seen.Add(key))
            {
                keys!.Add(key);
            }
        }

        return keys;
    }

    private KeyedDocumentLock AcquireDocumentLockReference(BsonValue key)
    {
        while (true)
        {
            var documentLock = _documentLocks.GetOrAdd(key, static id => new KeyedDocumentLock(id));
            var retry = false;
            lock (documentLock.ReferenceSyncRoot)
            {
                if (documentLock.IsRemoved)
                {
                    retry = true;
                }
                else
                {
                    documentLock.ReferenceCount++;
                    return documentLock;
                }
            }

            if (retry)
            {
                Thread.Yield();
            }
        }
    }

    private void ReleaseDocumentLockReference(KeyedDocumentLock documentLock)
    {
        lock (documentLock.ReferenceSyncRoot)
        {
            documentLock.ReferenceCount--;
            if (documentLock.ReferenceCount != 0)
            {
                return;
            }

            documentLock.IsRemoved = true;
            var removed = ((ICollection<KeyValuePair<BsonValue, KeyedDocumentLock>>)_documentLocks)
                .Remove(new KeyValuePair<BsonValue, KeyedDocumentLock>(documentLock.Key, documentLock));
            if (!removed)
            {
                documentLock.IsRemoved = false;
                return;
            }

            documentLock.Semaphore.Dispose();
        }
    }

}
