using System;
using System.Collections.Generic;
using System.Threading;

namespace TinyDb.Core;

internal sealed partial class CollectionState
{
    public IDisposable EnterPageMutationLock(uint pageId)
    {
        if (pageId == 0)
        {
            return EmptyLockScope.Instance;
        }

        return new SingleKeyedPageLockScope(this, AcquirePageLockReference(pageId));
    }

    public IDisposable EnterPageMutationLocks(IEnumerable<uint> pageIds)
    {
        if (pageIds == null) throw new ArgumentNullException(nameof(pageIds));
        return pageIds is IReadOnlyList<uint> list
            ? EnterPageMutationLocksCore(list)
            : EnterPageMutationLocksCore(pageIds);
    }

    private IDisposable EnterPageMutationLocksCore(IEnumerable<uint> pageIds)
    {
        var locks = CreatePageLockReferences(pageIds, 0, out var singleLock);
        return CreatePageLockScope(locks, singleLock);
    }

    private IDisposable EnterPageMutationLocksCore(IReadOnlyList<uint> pageIds)
    {
        var locks = CreatePageLockReferences(pageIds, pageIds.Count, out var singleLock);
        return CreatePageLockScope(locks, singleLock);
    }

    private IDisposable CreatePageLockScope(KeyedPageLock[]? locks, KeyedPageLock? singleLock)
    {
        if (singleLock != null)
        {
            return new SingleKeyedPageLockScope(this, singleLock);
        }

        return locks == null || locks.Length == 0
            ? EmptyLockScope.Instance
            : new KeyedPageLockScope(this, locks);
    }

    private KeyedPageLock[]? CreatePageLockReferences(
        IEnumerable<uint> pageIds,
        int capacity,
        out KeyedPageLock? singleLock)
    {
        singleLock = null;
        uint singlePageId = 0;
        List<uint>? ids = null;
        HashSet<uint>? seen = null;

        foreach (var pageId in pageIds)
        {
            if (pageId == 0)
            {
                continue;
            }

            if (singlePageId == 0)
            {
                singlePageId = pageId;
                continue;
            }

            if (seen == null)
            {
                seen = new HashSet<uint> { singlePageId };
                ids = capacity > 0 ? new List<uint>(capacity) : new List<uint>();
                ids.Add(singlePageId);
            }

            if (seen.Add(pageId))
            {
                ids!.Add(pageId);
            }
        }

        if (ids == null)
        {
            if (singlePageId == 0)
            {
                return null;
            }

            singleLock = AcquirePageLockReference(singlePageId);
            return null;
        }

        if (ids.Count == 1)
        {
            singleLock = AcquirePageLockReference(ids[0]);
            return null;
        }

        ids.Sort();
        var locks = new KeyedPageLock[ids.Count];
        for (var i = 0; i < ids.Count; i++)
        {
            locks[i] = AcquirePageLockReference(ids[i]);
        }

        return locks;
    }

    private KeyedPageLock AcquirePageLockReference(uint pageId)
    {
        while (true)
        {
            var pageLock = _pageMutationLocks.GetOrAdd(pageId, static id => new KeyedPageLock(id));
            var retry = false;
            lock (pageLock.ReferenceSyncRoot)
            {
                if (pageLock.IsRemoved)
                {
                    retry = true;
                }
                else
                {
                    pageLock.ReferenceCount++;
                    return pageLock;
                }
            }

            if (retry)
            {
                Thread.Yield();
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

    private sealed class SingleKeyedPageLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedPageLock _lock;
        private bool _entered;
        private bool _disposed;

        public SingleKeyedPageLockScope(CollectionState state, KeyedPageLock pageLock)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _lock = pageLock ?? throw new ArgumentNullException(nameof(pageLock));
            try
            {
                _lock.Semaphore.Wait();
                _entered = true;
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

            if (_entered)
            {
                _lock.Semaphore.Release();
                _entered = false;
            }

            _state.ReleasePageLockReference(_lock);
        }
    }}
