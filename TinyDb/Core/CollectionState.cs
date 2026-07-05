using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;

namespace TinyDb.Core;

internal sealed class CollectionState
{
    private static readonly AsyncLocal<DocumentLockOwner?> CurrentDocumentLockOwner = new();

    private int _cacheInitialized;
    private readonly ConcurrentDictionary<BsonValue, KeyedDocumentLock> _documentLocks = new(BsonValueComparer.EqualityComparer);
    private readonly ConcurrentDictionary<uint, KeyedPageLock> _pageMutationLocks = new();

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
        public DocumentLockOwner? Owner { get; set; }
        public int OwnerThreadId { get; set; }
        public int? OwnerTaskId { get; set; }
        public int OwnerDepth { get; set; }
    }

    private sealed class DocumentLockOwner
    {
        public DocumentLockOwner(int? taskId)
        {
            CreatorTaskId = taskId;
        }

        public int? CreatorTaskId { get; }
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

    private sealed class EmptyLockScope : IDisposable
    {
        public static EmptyLockScope Instance { get; } = new();

        public void Dispose()
        {
        }
    }

    private sealed class SingleKeyedDocumentLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedDocumentLock _lock;
        private readonly DocumentLockOwner _owner;
        private bool _entered;
        private bool _disposed;

        public SingleKeyedDocumentLockScope(CollectionState state, KeyedDocumentLock documentLock)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _lock = documentLock ?? throw new ArgumentNullException(nameof(documentLock));
            try
            {
                _owner = EnterDocumentSemaphore(_lock);
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
                ExitDocumentSemaphore(_lock, _owner);
                _entered = false;
            }

            _state.ReleaseDocumentLockReference(_lock);
        }
    }

    private sealed class KeyedDocumentLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedDocumentLock[] _locks;
        private DocumentLockOwner? _owner;
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
                    _owner = EnterDocumentSemaphore(_locks[i]);
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
                ExitDocumentSemaphore(_locks[i], _owner!);
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

    private static DocumentLockOwner EnterDocumentSemaphore(KeyedDocumentLock documentLock)
    {
        var owner = GetOrCreateDocumentLockOwner();
        var ownerThreadId = Environment.CurrentManagedThreadId;
        var ownerTaskId = Task.CurrentId;

        lock (documentLock.ReferenceSyncRoot)
        {
            if (IsOwnedByCurrentExecution(documentLock, owner, ownerThreadId, ownerTaskId))
            {
                documentLock.OwnerDepth++;
                return owner;
            }
        }

        documentLock.Semaphore.Wait();

        lock (documentLock.ReferenceSyncRoot)
        {
            SetDocumentLockOwner(documentLock, owner, ownerThreadId, ownerTaskId);
        }

        return owner;
    }

    private static async ValueTask EnterDocumentSemaphoreAsync(
        KeyedDocumentLock documentLock,
        DocumentLockOwner owner,
        CancellationToken cancellationToken)
    {
        var ownerThreadId = Environment.CurrentManagedThreadId;
        var ownerTaskId = Task.CurrentId;

        lock (documentLock.ReferenceSyncRoot)
        {
            if (IsOwnedByCurrentExecution(documentLock, owner, ownerThreadId, ownerTaskId))
            {
                documentLock.OwnerDepth++;
                return;
            }
        }

        await documentLock.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

        ownerThreadId = Environment.CurrentManagedThreadId;
        ownerTaskId = Task.CurrentId;
        lock (documentLock.ReferenceSyncRoot)
        {
            SetDocumentLockOwner(documentLock, owner, ownerThreadId, ownerTaskId);
        }
    }

    private static void ExitDocumentSemaphore(KeyedDocumentLock documentLock, DocumentLockOwner scopeOwner)
    {
        var owner = CurrentDocumentLockOwner.Value;
        var ownerThreadId = Environment.CurrentManagedThreadId;
        var ownerTaskId = Task.CurrentId;

        lock (documentLock.ReferenceSyncRoot)
        {
            if (!IsOwnedByCurrentExecution(documentLock, owner, ownerThreadId, ownerTaskId) &&
                !ReferenceEquals(documentLock.Owner, scopeOwner))
            {
                throw new SynchronizationLockException("Document lock is not owned by the current execution context.");
            }

            if (documentLock.OwnerDepth > 1)
            {
                documentLock.OwnerDepth--;
                return;
            }

            documentLock.Owner = null;
            documentLock.OwnerThreadId = 0;
            documentLock.OwnerTaskId = null;
            documentLock.OwnerDepth = 0;
        }

        documentLock.Semaphore.Release();
    }

    private static DocumentLockOwner GetOrCreateDocumentLockOwner()
    {
        var owner = CurrentDocumentLockOwner.Value;
        var currentTaskId = Task.CurrentId;
        if (owner != null && ShouldReuseDocumentLockOwner(owner, currentTaskId)) return owner;

        owner = new DocumentLockOwner(currentTaskId);
        CurrentDocumentLockOwner.Value = owner;
        return owner;
    }

    private static bool ShouldReuseDocumentLockOwner(DocumentLockOwner owner, int? currentTaskId)
    {
        return currentTaskId == null || owner.CreatorTaskId == currentTaskId;
    }

    private static DocumentLockOwner? CreateDocumentLockOwnerIfNeeded(
        KeyedDocumentLock[]? locks,
        KeyedDocumentLock? singleLock)
    {
        return singleLock != null || (locks != null && locks.Length > 0)
            ? GetOrCreateDocumentLockOwner()
            : null;
    }

    private static void SetDocumentLockOwner(
        KeyedDocumentLock documentLock,
        DocumentLockOwner owner,
        int ownerThreadId,
        int? ownerTaskId)
    {
        documentLock.Owner = owner;
        documentLock.OwnerThreadId = ownerThreadId;
        documentLock.OwnerTaskId = ownerTaskId;
        documentLock.OwnerDepth = 1;
    }

    private static bool IsOwnedByCurrentExecution(
        KeyedDocumentLock documentLock,
        DocumentLockOwner? owner,
        int ownerThreadId,
        int? ownerTaskId)
    {
        return documentLock.OwnerDepth > 0 &&
               ((owner != null && ReferenceEquals(documentLock.Owner, owner)) ||
                (documentLock.Owner == null &&
                 documentLock.OwnerThreadId == ownerThreadId &&
                 documentLock.OwnerTaskId == ownerTaskId));
    }

    private sealed class AsyncKeyedDocumentLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedDocumentLock[] _locks;
        private readonly DocumentLockOwner _owner;
        private int _entered;
        private bool _disposed;

        private AsyncKeyedDocumentLockScope(CollectionState state, KeyedDocumentLock[] locks, DocumentLockOwner owner)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _locks = locks ?? throw new ArgumentNullException(nameof(locks));
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        }

        public static async ValueTask<AsyncKeyedDocumentLockScope> EnterAsync(
            CollectionState state,
            KeyedDocumentLock[] locks,
            DocumentLockOwner owner,
            CancellationToken cancellationToken)
        {
            var scope = new AsyncKeyedDocumentLockScope(state, locks, owner);
            try
            {
                for (var i = 0; i < scope._locks.Length; i++)
                {
                    await EnterDocumentSemaphoreAsync(scope._locks[i], owner, cancellationToken).ConfigureAwait(false);
                    scope._entered++;
                }

                return scope;
            }
            catch
            {
                scope.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            for (var i = _entered - 1; i >= 0; i--)
            {
                ExitDocumentSemaphore(_locks[i], _owner);
            }

            _entered = 0;

            for (var i = _locks.Length - 1; i >= 0; i--)
            {
                _state.ReleaseDocumentLockReference(_locks[i]);
            }
        }
    }

    private sealed class SingleAsyncKeyedDocumentLockScope : IDisposable
    {
        private readonly CollectionState _state;
        private readonly KeyedDocumentLock _lock;
        private readonly DocumentLockOwner _owner;
        private bool _entered;
        private bool _disposed;

        private SingleAsyncKeyedDocumentLockScope(CollectionState state, KeyedDocumentLock documentLock, DocumentLockOwner owner)
        {
            _state = state ?? throw new ArgumentNullException(nameof(state));
            _lock = documentLock ?? throw new ArgumentNullException(nameof(documentLock));
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
        }

        public static async ValueTask<IDisposable> EnterAsync(
            CollectionState state,
            KeyedDocumentLock documentLock,
            DocumentLockOwner owner,
            CancellationToken cancellationToken)
        {
            var scope = new SingleAsyncKeyedDocumentLockScope(state, documentLock, owner);
            try
            {
                await EnterDocumentSemaphoreAsync(documentLock, owner, cancellationToken).ConfigureAwait(false);
                scope._entered = true;
                return scope;
            }
            catch
            {
                scope.Dispose();
                throw;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (_entered)
            {
                ExitDocumentSemaphore(_lock, _owner);
                _entered = false;
            }

            _state.ReleaseDocumentLockReference(_lock);
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
    }
}

internal sealed class DataPageState
{
    public long PageId;
    public readonly object SyncRoot = new();
}
