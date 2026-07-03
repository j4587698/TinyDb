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
    private readonly ConcurrentDictionary<uint, KeyedPageLock> _pageMutationLocks = new();
    private static readonly AsyncLocal<Dictionary<object, int>?> HeldDocumentLocks = new();

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
            .Select(AcquirePageLockReference)
            .ToArray();

        return new KeyedPageLockScope(this, locks);
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
                return;
            }

            documentLock.Semaphore.Dispose();
        }
    }

    private KeyedPageLock AcquirePageLockReference(uint pageId)
    {
        while (true)
        {
            var pageLock = _pageMutationLocks.GetOrAdd(pageId, static id => new KeyedPageLock(id));
            lock (pageLock.ReferenceSyncRoot)
            {
                if (pageLock.IsRemoved)
                {
                    continue;
                }

                pageLock.ReferenceCount++;
                return pageLock;
            }
        }
    }

    private void ReleasePageLockReference(KeyedPageLock pageLock)
    {
        lock (pageLock.ReferenceSyncRoot)
        {
            pageLock.ReferenceCount--;
            if (pageLock.ReferenceCount != 0)
            {
                return;
            }

            pageLock.IsRemoved = true;
            var removed = ((ICollection<KeyValuePair<uint, KeyedPageLock>>)_pageMutationLocks)
                .Remove(new KeyValuePair<uint, KeyedPageLock>(pageLock.PageId, pageLock));
            if (!removed)
            {
                pageLock.IsRemoved = false;
                return;
            }

            pageLock.Semaphore.Dispose();
        }
    }

    private sealed class KeyedDocumentLock
    {
        public KeyedDocumentLock(BsonValue key)
        {
            Key = key;
        }

        public BsonValue Key { get; }
        public SemaphoreSlim Semaphore { get; } = new(1, 1);
        public object ReferenceSyncRoot { get; } = new();
        public int ReferenceCount { get; set; }
        public bool IsRemoved { get; set; }
    }

    private sealed class KeyedPageLock
    {
        public KeyedPageLock(uint pageId)
        {
            PageId = pageId;
        }

        public uint PageId { get; }
        public SemaphoreSlim Semaphore { get; } = new(1, 1);
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
                    EnterDocumentSemaphore(_locks[i]);
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
                ExitDocumentSemaphore(_locks[i]);
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

    private static void EnterDocumentSemaphore(KeyedDocumentLock documentLock)
    {
        var heldLocks = HeldDocumentLocks.Value;
        if (heldLocks != null && heldLocks.TryGetValue(documentLock, out var count))
        {
            heldLocks[documentLock] = count + 1;
            return;
        }

        documentLock.Semaphore.Wait();
        heldLocks ??= new Dictionary<object, int>(ReferenceEqualityComparer.Instance);
        heldLocks[documentLock] = 1;
        HeldDocumentLocks.Value = heldLocks;
    }

    private static void ExitDocumentSemaphore(KeyedDocumentLock documentLock)
    {
        var heldLocks = HeldDocumentLocks.Value;
        if (heldLocks == null || !heldLocks.TryGetValue(documentLock, out var count))
        {
            documentLock.Semaphore.Release();
            return;
        }

        if (count > 1)
        {
            heldLocks[documentLock] = count - 1;
            return;
        }

        heldLocks.Remove(documentLock);
        if (heldLocks.Count == 0)
        {
            HeldDocumentLocks.Value = null;
        }

        documentLock.Semaphore.Release();
    }

    private sealed class KeyedPageLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedPageLock[] _locks;
        private int _entered;
        private bool _disposed;

        public KeyedPageLockScope(CollectionState state, KeyedPageLock[] locks)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _locks = locks ?? throw new ArgumentNullException(nameof(locks));
            try
            {
                for (var i = 0; i < _locks.Length; i++)
                {
                    _locks[i].Semaphore.Wait();
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
                _locks[i].Semaphore.Release();
            }

            _entered = 0;

            for (var i = _locks.Length - 1; i >= 0; i--)
            {
                _state.ReleasePageLockReference(_locks[i]);
            }
        }
    }
}

internal sealed class DataPageState
{
    public long PageId;
    public readonly object SyncRoot = new();
}
