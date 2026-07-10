using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;

namespace TinyDb.Core;

internal sealed partial class CollectionState
{
    private static readonly AsyncLocal<PageMutationLockRetentionContext?> s_pageMutationLockRetention = new();

    internal static IDisposable RetainPageMutationLocksForCurrentContext()
    {
        var context = s_pageMutationLockRetention.Value;
        if (context != null)
        {
            context.AddRef();
            return new PageMutationLockRetentionScope(context);
        }

        context = new PageMutationLockRetentionContext();
        s_pageMutationLockRetention.Value = context;
        return new PageMutationLockRetentionScope(context);
    }

    public IDisposable EnterPageMutationLock(uint pageId)
    {
        if (pageId == 0)
        {
            return EmptyLockScope.Instance;
        }

        if (s_pageMutationLockRetention.Value is { } retention)
        {
            retention.Enter(this, pageId);
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
        if (s_pageMutationLockRetention.Value is { } retention)
        {
            retention.Enter(this, pageIds);
            return EmptyLockScope.Instance;
        }

        var locks = CreatePageLockReferences(pageIds, 0, out var singleLock);
        return CreatePageLockScope(locks, singleLock);
    }

    private IDisposable EnterPageMutationLocksCore(IReadOnlyList<uint> pageIds)
    {
        if (s_pageMutationLockRetention.Value is { } retention)
        {
            retention.Enter(this, pageIds);
            return EmptyLockScope.Instance;
        }

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

    private readonly struct RetainedPageLockKey : IEquatable<RetainedPageLockKey>
    {
        public RetainedPageLockKey(CollectionState state, uint pageId)
        {
            State = state;
            PageId = pageId;
        }

        public CollectionState State { get; }
        public uint PageId { get; }

        public bool Equals(RetainedPageLockKey other)
        {
            return ReferenceEquals(State, other.State) && PageId == other.PageId;
        }

        public override bool Equals(object? obj)
        {
            return obj is RetainedPageLockKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(RuntimeHelpers.GetHashCode(State), PageId);
        }
    }

    private sealed class RetainedPageLock
    {
        public RetainedPageLock(CollectionState state, KeyedPageLock pageLock)
        {
            State = state;
            PageLock = pageLock;
        }

        public CollectionState State { get; }
        public KeyedPageLock PageLock { get; }
    }

    private sealed class PageMutationLockRetentionContext
    {
        private readonly Dictionary<RetainedPageLockKey, RetainedPageLock> _heldLocks = new();
        private readonly List<RetainedPageLock> _releaseOrder = new();
        private int _refCount = 1;

        public void AddRef()
        {
            checked
            {
                _refCount++;
            }
        }

        public void Enter(CollectionState state, IEnumerable<uint> pageIds)
        {
            var locks = state.CreatePageLockReferences(pageIds, 0, out var singleLock);
            if (singleLock != null)
            {
                Enter(state, singleLock);
                return;
            }

            if (locks == null)
            {
                return;
            }

            foreach (var pageLock in locks)
            {
                Enter(state, pageLock);
            }
        }

        public void Enter(CollectionState state, uint pageId)
        {
            Enter(state, state.AcquirePageLockReference(pageId));
        }

        private void Enter(CollectionState state, KeyedPageLock pageLock)
        {
            var key = new RetainedPageLockKey(state, pageLock.PageId);
            if (_heldLocks.ContainsKey(key))
            {
                state.ReleasePageLockReference(pageLock);
                return;
            }

            var entered = false;
            try
            {
                pageLock.Semaphore.Wait();
                entered = true;

                var retainedLock = new RetainedPageLock(state, pageLock);
                _heldLocks.Add(key, retainedLock);
                _releaseOrder.Add(retainedLock);
            }
            catch
            {
                if (entered)
                {
                    pageLock.Semaphore.Release();
                }

                state.ReleasePageLockReference(pageLock);
                throw;
            }
        }

        public bool Release()
        {
            _refCount--;
            if (_refCount > 0)
            {
                return false;
            }

            for (var i = _releaseOrder.Count - 1; i >= 0; i--)
            {
                var retainedLock = _releaseOrder[i];
                retainedLock.PageLock.Semaphore.Release();
            }

            for (var i = _releaseOrder.Count - 1; i >= 0; i--)
            {
                var retainedLock = _releaseOrder[i];
                retainedLock.State.ReleasePageLockReference(retainedLock.PageLock);
            }

            _releaseOrder.Clear();
            _heldLocks.Clear();
            return true;
        }
    }

    private sealed class PageMutationLockRetentionScope : IDisposable
    {
        private readonly PageMutationLockRetentionContext _context;
        private bool _disposed;

        public PageMutationLockRetentionScope(PageMutationLockRetentionContext context)
        {
            _context = context;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (_context.Release() && ReferenceEquals(s_pageMutationLockRetention.Value, _context))
            {
                s_pageMutationLockRetention.Value = null;
            }
        }
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
