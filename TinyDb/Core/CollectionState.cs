using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using TinyDb.Bson;

namespace TinyDb.Core;

internal sealed class CollectionState
{
    private int _cacheInitialized;
    private readonly ConcurrentDictionary<BsonValue, KeyedDocumentLock> _documentLocks = new(BsonValueComparer.EqualityComparer);
    private readonly ConcurrentDictionary<uint, object> _pageMutationLocks = new();

    public DataPageState PageState { get; } = new();
    public object CommitGate { get; } = new();
    
    // 抽象层：可以是内存索引，也可以是磁盘索引
    public IDocumentIndex Index { get; set; } = default!;
    
    // 追踪集合拥有的页面ID，优化全表扫描
    public ConcurrentDictionary<uint, byte> OwnedPages { get; } = new();
    
    public bool IsCacheInitialized => Volatile.Read(ref _cacheInitialized) == 1;
    public void MarkCacheInitialized() => Volatile.Write(ref _cacheInitialized, 1);

    public IDisposable EnterDocumentLock(BsonValue id)
    {
        return EnterDocumentLocks(new[] { id });
    }

    public IDisposable EnterDocumentLocks(IEnumerable<BsonValue> ids)
    {
        if (ids == null) throw new ArgumentNullException(nameof(ids));

        var keys = ids
            .Where(static id => id != null && !id.IsNull)
            .Distinct(BsonValueComparer.EqualityComparer)
            .OrderBy(static id => id, BsonValueSortComparer.Instance)
            .ToArray();

        var locks = keys
            .Select(AcquireDocumentLockReference)
            .ToArray();

        return new KeyedDocumentLockScope(this, locks);
    }

    public IDisposable EnterPageMutationLock(uint pageId)
    {
        return EnterPageMutationLocks(new[] { pageId });
    }

    public IDisposable EnterPageMutationLocks(IEnumerable<uint> pageIds)
    {
        if (pageIds == null) throw new ArgumentNullException(nameof(pageIds));

        var locks = pageIds
            .Where(static pageId => pageId != 0)
            .Distinct()
            .OrderBy(static pageId => pageId)
            .Select(pageId => _pageMutationLocks.GetOrAdd(pageId, static _ => new object()))
            .ToArray();

        return new MonitorLockScope(locks);
    }

    private KeyedDocumentLock AcquireDocumentLockReference(BsonValue key)
    {
        while (true)
        {
            var documentLock = _documentLocks.GetOrAdd(key, static id => new KeyedDocumentLock(id));
            lock (documentLock.ReferenceSyncRoot)
            {
                if (documentLock.IsRemoved)
                {
                    continue;
                }

                documentLock.ReferenceCount++;
                return documentLock;
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
            }
        }
    }

    private sealed class KeyedDocumentLock
    {
        public KeyedDocumentLock(BsonValue key)
        {
            Key = key;
        }

        public BsonValue Key { get; }
        public object SyncRoot { get; } = new();
        public object ReferenceSyncRoot { get; } = new();
        public int ReferenceCount { get; set; }
        public bool IsRemoved { get; set; }
    }

    private sealed class KeyedDocumentLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedDocumentLock[] _locks;
        private int _entered;
        private bool _disposed;

        public KeyedDocumentLockScope(CollectionState state, KeyedDocumentLock[] locks)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _locks = locks ?? throw new ArgumentNullException(nameof(locks));
            try
            {
                for (var i = 0; i < _locks.Length; i++)
                {
                    Monitor.Enter(_locks[i].SyncRoot);
                    _entered++;
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            for (var i = _entered - 1; i >= 0; i--)
            {
                Monitor.Exit(_locks[i].SyncRoot);
            }

            _entered = 0;

            for (var i = _locks.Length - 1; i >= 0; i--)
            {
                _state.ReleaseDocumentLockReference(_locks[i]);
            }
        }
    }

    private sealed class BsonValueSortComparer : IComparer<BsonValue>
    {
        public static BsonValueSortComparer Instance { get; } = new();

        public int Compare(BsonValue? x, BsonValue? y)
        {
            return BsonValueComparer.Compare(x, y);
        }
    }

    internal sealed class MonitorLockScope : IDisposable
    {
        private readonly object[] _locks;
        private int _entered;
        private bool _disposed;

        public MonitorLockScope(object[] locks)
        {
            _locks = locks ?? throw new ArgumentNullException(nameof(locks));
            try
            {
                for (var i = 0; i < _locks.Length; i++)
                {
                    Monitor.Enter(_locks[i]);
                    _entered++;
                }
            }
            catch
            {
                Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            for (var i = _entered - 1; i >= 0; i--)
            {
                Monitor.Exit(_locks[i]);
            }

            _entered = 0;
        }
    }
}

internal sealed class DataPageState
{
    public long PageId;
    public readonly object SyncRoot = new();
}
