using TinyDb.Bson;
using TinyDb.Storage;
using System.Reflection;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace TinyDb.Index;

/// <summary>
/// B+ 树索引实现 (基于磁盘分段并发优化的中间件)
/// </summary>
public sealed class BTreeIndex : IDisposable
{
    private readonly string _name;
    private readonly string[] _fields;
    private readonly bool _unique;
    private readonly bool _sparse;
    private readonly DiskBTree _tree;
    private int _disposed;

    private readonly bool _ownsPageManager;
    private readonly string? _tempFilePath;
    private readonly PageManager? _tempPm;
    private readonly int _maxKeys;
    private readonly ReaderWriterLockSlim _lock = new();
    private const int MaxConcurrentAsyncReaders = 64;
    private readonly SemaphoreSlim _asyncReadGate = new(MaxConcurrentAsyncReaders, MaxConcurrentAsyncReaders);
    private readonly SemaphoreSlim _asyncWriteGate = new(1, 1);
    private const int DefaultScanBatchSize = 1024;

    public string Name => _name;
    public IReadOnlyList<string> Fields => _fields;
    public bool IsUnique => _unique;
    public bool IsSparse => _sparse;
    internal uint RootPageId => _tree.RootPageId;
    internal int MaxKeys => _maxKeys;
    
    public int NodeCount 
    { 
        get 
        { 
            ThrowIfDisposed();
            _lock.EnterReadLock(); 
            try { return _tree.NodeCount; } 
            finally { _lock.ExitReadLock(); } 
        } 
    }
    
    public int EntryCount 
    { 
        get 
        { 
            ThrowIfDisposed();
            _lock.EnterReadLock(); 
            try { return ClampEntryCount(_tree.EntryCount); }
            finally { _lock.ExitReadLock(); } 
        } 
    }

    public IndexType Type => IndexType.BTree;

    // 兼容性字段
    private BTreeNode _root = null!;

    private void UpdateRootField()
    {
        _root = new BTreeNode(_tree.RootNode, _tree);
    }

    public BTreeIndex(PageManager pm, string name, string[] fields, bool unique = false)
        : this(pm, name, fields, unique, 200)
    {
    }

    public BTreeIndex(PageManager pm, string name, string[] fields, bool unique, int maxKeys, bool sparse = false)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _fields = fields ?? throw new ArgumentNullException(nameof(fields));
        if (fields.Length == 0) throw new ArgumentException("Fields cannot be empty", nameof(fields));    
        _unique = unique;
        _sparse = sparse;
        _maxKeys = maxKeys;
        _tree = DiskBTree.Create(pm, maxKeys);
        _ownsPageManager = false;
        UpdateRootField();
    }

    internal BTreeIndex(PageManager pm, string name, string[] fields, bool unique, uint rootPageId, int maxKeys, bool sparse = false)
    {
        if (rootPageId == 0) throw new ArgumentOutOfRangeException(nameof(rootPageId));

        _name = name ?? throw new ArgumentNullException(nameof(name));
        _fields = fields ?? throw new ArgumentNullException(nameof(fields));
        if (fields.Length == 0) throw new ArgumentException("Fields cannot be empty", nameof(fields));
        _unique = unique;
        _sparse = sparse;
        _maxKeys = maxKeys > 0 ? maxKeys : 200;
        _tree = new DiskBTree(pm ?? throw new ArgumentNullException(nameof(pm)), rootPageId, _maxKeys);
        _ownsPageManager = false;
        UpdateRootField();
    }

    public BTreeIndex(string name, string[] fields, bool unique, int maxKeys, bool sparse = false)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _fields = fields ?? throw new ArgumentNullException(nameof(fields));
        if (fields.Length == 0) throw new ArgumentException("Fields cannot be empty", nameof(fields));    
        _unique = unique;
        _sparse = sparse;
        _maxKeys = maxKeys;

        _tempFilePath = Path.Combine(Path.GetTempPath(), $"btree_idx_{Guid.NewGuid():N}.db");
        var ds = new DiskStream(
            _tempFilePath,
            FileAccess.ReadWrite,
            FileShare.ReadWrite | FileShare.Delete,
            FileOptions.DeleteOnClose);
        _tempPm = new PageManager(ds);
        _ownsPageManager = true;

        _tree = DiskBTree.Create(_tempPm, maxKeys);
        UpdateRootField();
    }

    public BTreeIndex(string name, string[] fields, bool unique = false)
        : this(name, fields, unique, 200)
    {
    }

    public bool Insert(IndexKey key, BsonValue documentId)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (documentId == null) throw new ArgumentNullException(nameof(documentId));

        using (EnterAsyncWriteGate())
        {
            return InsertCore(key, documentId);
        }
    }

    internal async Task<bool> InsertAsync(IndexKey key, BsonValue documentId, CancellationToken cancellationToken = default)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (documentId == null) throw new ArgumentNullException(nameof(documentId));

        using (await EnterAsyncWriteGateAsync(cancellationToken).ConfigureAwait(false))
        {
            return InsertCore(key, documentId);
        }
    }

    private bool InsertCore(IndexKey key, BsonValue documentId)
    {
        _lock.EnterWriteLock();
        try
        {
            if (_unique)
            {
                if (!_tree.TryInsertUnique(key, documentId))
                {
                    return false;
                }
            }
            else
            {
                _tree.Insert(key, documentId);
            }

            UpdateRootField();
            return true;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public bool Delete(IndexKey key, BsonValue documentId)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using (EnterAsyncWriteGate())
        {
            return DeleteCore(key, documentId);
        }
    }

    internal async Task<bool> DeleteAsync(IndexKey key, BsonValue documentId, CancellationToken cancellationToken = default)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        using (await EnterAsyncWriteGateAsync(cancellationToken).ConfigureAwait(false))
        {
            return DeleteCore(key, documentId);
        }
    }

    private bool DeleteCore(IndexKey key, BsonValue documentId)
    {
        _lock.EnterWriteLock();
        try
        {
            var res = _tree.Delete(key, documentId);
            UpdateRootField();
            return res;
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    public bool Contains(IndexKey key) 
    { 
        ThrowIfDisposed();
        _lock.EnterReadLock(); 
        try { return _tree.Contains(key); } 
        finally { _lock.ExitReadLock(); } 
    }

    public bool Contains(IndexKey key, BsonValue documentId) 
    { 
        ThrowIfDisposed();
        _lock.EnterReadLock(); 
        try { return _tree.Contains(key, documentId); } 
        finally { _lock.ExitReadLock(); } 
    }

    public IEnumerable<BsonValue> Find(IndexKey key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        return ScanRangeIterator(key, key, includeStart: true, includeEnd: true, descending: false);
    }

    public BsonValue? FindExact(IndexKey key) 
    { 
        ThrowIfDisposed();
        _lock.EnterReadLock(); 
        try { return _tree.FindExact(key); } 
        finally { _lock.ExitReadLock(); } 
    }

    public IEnumerable<BsonValue> FindRange(IndexKey startKey, IndexKey endKey, bool includeStart = true, bool includeEnd = true)
    {
        if (startKey == null) throw new ArgumentNullException(nameof(startKey));
        if (endKey == null) throw new ArgumentNullException(nameof(endKey));
        return ScanRangeIterator(startKey, endKey, includeStart, includeEnd, descending: false);
    }

    public IEnumerable<BsonValue> FindRangeReverse(IndexKey startKey, IndexKey endKey, bool includeStart = true, bool includeEnd = true)
    {
        if (startKey == null) throw new ArgumentNullException(nameof(startKey));
        if (endKey == null) throw new ArgumentNullException(nameof(endKey));
        return ScanRangeIterator(startKey, endKey, includeStart, includeEnd, descending: true);
    }

    public IEnumerable<BsonValue> GetAll() 
    { 
        return GetAllIterator();
    } 

    public IEnumerable<BsonValue> GetAllReverse()
    {
        return GetAllReverseIterator();
    }

    internal IAsyncEnumerable<BsonValue> FindAsync(IndexKey key, CancellationToken cancellationToken = default)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        return ScanRangeIteratorAsync(key, key, includeStart: true, includeEnd: true, descending: false, cancellationToken);
    }

    internal async Task<BsonValue?> FindExactAsync(IndexKey key, CancellationToken cancellationToken = default)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));

        ThrowIfDisposed();
        await _asyncReadGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfDisposed();
            return await _tree.FindExactAsync(key, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _asyncReadGate.Release();
        }
    }

    internal IAsyncEnumerable<BsonValue> FindRangeAsync(
        IndexKey startKey,
        IndexKey endKey,
        bool includeStart = true,
        bool includeEnd = true,
        CancellationToken cancellationToken = default)
    {
        if (startKey == null) throw new ArgumentNullException(nameof(startKey));
        if (endKey == null) throw new ArgumentNullException(nameof(endKey));
        return ScanRangeIteratorAsync(startKey, endKey, includeStart, includeEnd, descending: false, cancellationToken);
    }

    internal IAsyncEnumerable<BsonValue> FindRangeReverseAsync(
        IndexKey startKey,
        IndexKey endKey,
        bool includeStart = true,
        bool includeEnd = true,
        CancellationToken cancellationToken = default)
    {
        if (startKey == null) throw new ArgumentNullException(nameof(startKey));
        if (endKey == null) throw new ArgumentNullException(nameof(endKey));
        return ScanRangeIteratorAsync(startKey, endKey, includeStart, includeEnd, descending: true, cancellationToken);
    }

    internal IAsyncEnumerable<BsonValue> GetAllAsync(CancellationToken cancellationToken = default)
    {
        return ScanRangeIteratorAsync(IndexKey.MinValue, IndexKey.MaxValue, includeStart: true, includeEnd: true, descending: false, cancellationToken);
    }

    internal IAsyncEnumerable<BsonValue> GetAllReverseAsync(CancellationToken cancellationToken = default)
    {
        return ScanRangeIteratorAsync(IndexKey.MinValue, IndexKey.MaxValue, includeStart: true, includeEnd: true, descending: true, cancellationToken);
    }

    private IEnumerable<BsonValue> GetAllIterator()
    {
        return ScanRangeIterator(IndexKey.MinValue, IndexKey.MaxValue, includeStart: true, includeEnd: true, descending: false);
    }

    private IEnumerable<BsonValue> GetAllReverseIterator()
    {
        return ScanRangeIterator(IndexKey.MinValue, IndexKey.MaxValue, includeStart: true, includeEnd: true, descending: true);
    }

    private async IAsyncEnumerable<BsonValue> ScanRangeIteratorAsync(
        IndexKey startKey,
        IndexKey endKey,
        bool includeStart,
        bool includeEnd,
        bool descending,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        IndexKey? continuationKey = null;
        BsonValue? continuationValue = null;
        uint continuationPageId = 0;
        int continuationIndex = -1;
        BsonValue? lastYieldedValue = null;
        var hasLastYieldedValue = false;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfDisposed();

            DiskBTree.IndexScanBatch batch;
            await _asyncReadGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                ThrowIfDisposed();
                batch = descending
                    ? await _tree.FindRangeReverseBatchAsync(startKey, endKey, includeStart, includeEnd, continuationKey, continuationValue, continuationPageId, continuationIndex, DefaultScanBatchSize, cancellationToken).ConfigureAwait(false)
                    : await _tree.FindRangeBatchAsync(startKey, endKey, includeStart, includeEnd, continuationKey, continuationValue, continuationPageId, continuationIndex, DefaultScanBatchSize, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _asyncReadGate.Release();
            }

            if (batch.Values.Count == 0)
            {
                yield break;
            }

            var yieldedFromBatch = false;
            foreach (var value in batch.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (hasLastYieldedValue && BsonValueComparer.ValueEquals(lastYieldedValue, value))
                {
                    continue;
                }

                hasLastYieldedValue = true;
                lastYieldedValue = value;
                yieldedFromBatch = true;
                yield return value;
            }

            if (!batch.HasMore || batch.LastKey == null || batch.LastValue == null)
            {
                yield break;
            }

            if (!yieldedFromBatch &&
                IsSameContinuation(
                    continuationKey,
                    continuationValue,
                    continuationPageId,
                    continuationIndex,
                    batch))
            {
                yield break;
            }

            continuationKey = batch.LastKey;
            continuationValue = batch.LastValue;
            continuationPageId = batch.LastPageId;
            continuationIndex = batch.LastIndex;
        }
    }

    private IEnumerable<BsonValue> ScanRangeIterator(
        IndexKey startKey,
        IndexKey endKey,
        bool includeStart,
        bool includeEnd,
        bool descending)
    {
        IndexKey? continuationKey = null;
        BsonValue? continuationValue = null;
        uint continuationPageId = 0;
        int continuationIndex = -1;
        BsonValue? lastYieldedValue = null;
        var hasLastYieldedValue = false;

        while (true)
        {
            DiskBTree.IndexScanBatch batch;
            ThrowIfDisposed();
            _lock.EnterReadLock();
            try
            {
                batch = descending
                    ? _tree.FindRangeReverseBatch(startKey, endKey, includeStart, includeEnd, continuationKey, continuationValue, continuationPageId, continuationIndex, DefaultScanBatchSize)
                    : _tree.FindRangeBatch(startKey, endKey, includeStart, includeEnd, continuationKey, continuationValue, continuationPageId, continuationIndex, DefaultScanBatchSize);
            }
            finally
            {
                _lock.ExitReadLock();
            }

            if (batch.Values.Count == 0)
            {
                yield break;
            }

            var yieldedFromBatch = false;
            foreach (var value in batch.Values)
            {
                if (hasLastYieldedValue && BsonValueComparer.ValueEquals(lastYieldedValue, value))
                {
                    continue;
                }

                hasLastYieldedValue = true;
                lastYieldedValue = value;
                yieldedFromBatch = true;
                yield return value;
            }

            if (!batch.HasMore || batch.LastKey == null || batch.LastValue == null)
            {
                yield break;
            }

            if (!yieldedFromBatch &&
                IsSameContinuation(
                    continuationKey,
                    continuationValue,
                    continuationPageId,
                    continuationIndex,
                    batch))
            {
                yield break;
            }

            continuationKey = batch.LastKey;
            continuationValue = batch.LastValue;
            continuationPageId = batch.LastPageId;
            continuationIndex = batch.LastIndex;
        }
    }

    private static bool IsSameContinuation(
        IndexKey? continuationKey,
        BsonValue? continuationValue,
        uint continuationPageId,
        int continuationIndex,
        DiskBTree.IndexScanBatch batch)
    {
        return continuationKey != null &&
               continuationValue != null &&
               batch.LastKey != null &&
               batch.LastValue != null &&
               continuationPageId == batch.LastPageId &&
               continuationIndex == batch.LastIndex &&
               continuationKey.CompareTo(batch.LastKey) == 0 &&
               BsonValueComparer.ValueEquals(continuationValue, batch.LastValue);
    }

    public void Clear()
    {
        using (EnterAsyncWriteGate())
        {
            ClearCore();
        }
    }

    internal async Task ClearAsync(CancellationToken cancellationToken = default)
    {
        using (await EnterAsyncWriteGateAsync(cancellationToken).ConfigureAwait(false))
        {
            ClearCore();
        }
    }

    private void ClearCore()
    {
        _lock.EnterWriteLock();
        try
        {
            _tree.Clear();
            UpdateRootField();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    internal void DropStorage()
    {
        using (EnterAsyncWriteGate())
        {
            DropStorageCore();
        }
    }

    internal async Task DropStorageAsync(CancellationToken cancellationToken = default)
    {
        using (await EnterAsyncWriteGateAsync(cancellationToken).ConfigureAwait(false))
        {
            DropStorageCore();
        }
    }

    private void DropStorageCore()
    {
        _lock.EnterWriteLock();
        try
        {
            _tree.DropPages();
        }
        finally
        {
            _lock.ExitWriteLock();
        }
    }

    private IDisposable EnterAsyncWriteGate(bool allowDisposed = false)
    {
        if (!allowDisposed) ThrowIfDisposed();
        _asyncWriteGate.Wait();
        var acquired = 0;
        try
        {
            for (; acquired < MaxConcurrentAsyncReaders; acquired++)
            {
                _asyncReadGate.Wait();
            }

            if (!allowDisposed) ThrowIfDisposed();
            return new AsyncWriteGateScope(this);
        }
        catch
        {
            for (var i = 0; i < acquired; i++)
            {
                _asyncReadGate.Release();
            }

            _asyncWriteGate.Release();
            throw;
        }
    }

    private async Task<IDisposable> EnterAsyncWriteGateAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        await _asyncWriteGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        var acquired = 0;
        try
        {
            for (; acquired < MaxConcurrentAsyncReaders; acquired++)
            {
                await _asyncReadGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            ThrowIfDisposed();
            return new AsyncWriteGateScope(this);
        }
        catch
        {
            for (var i = 0; i < acquired; i++)
            {
                _asyncReadGate.Release();
            }

            _asyncWriteGate.Release();
            throw;
        }
    }

    private void ExitAsyncWriteGate()
    {
        _asyncReadGate.Release(MaxConcurrentAsyncReaders);
        _asyncWriteGate.Release();
    }

    private sealed class AsyncWriteGateScope : IDisposable
    {
        private readonly BTreeIndex _owner;
        private bool _disposed;

        public AsyncWriteGateScope(BTreeIndex owner)
        {
            _owner = owner;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _owner.ExitAsyncWriteGate();
        }
    }

    public bool Validate() 
    { 
        ThrowIfDisposed();
        _lock.EnterReadLock(); 
        try { return _tree.Validate(); } 
        finally { _lock.ExitReadLock(); } 
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        IDisposable? gate = null;
        try
        {
            gate = EnterAsyncWriteGate(allowDisposed: true);
            _lock.EnterWriteLock();
            try
            {
                _tree.Dispose();
            }
            finally
            {
                _lock.ExitWriteLock();
            }

            if (_ownsPageManager)
            {
                _tempPm!.Dispose();
                if (!string.IsNullOrEmpty(_tempFilePath) && File.Exists(_tempFilePath))
                {
                    File.Delete(_tempFilePath);
                }
            }
        }
        finally
        {
            gate?.Dispose();
            _lock.Dispose();
            _asyncReadGate.Dispose();
            _asyncWriteGate.Dispose();
        }
    }

    public IndexStatistics GetStatistics()
    {
        ThrowIfDisposed();
        _lock.EnterReadLock();
        try
        {
            var nodeCount = _tree.NodeCount;
            var entryCount = ClampEntryCount(_tree.EntryCount);
            return new IndexStatistics
            {
                Name = Name,
                Type = Type,
                Fields = _fields,
                IsUnique = IsUnique,
                IsSparse = IsSparse,
                NodeCount = nodeCount,
                EntryCount = entryCount,
                AverageKeysPerNode = (double)entryCount / nodeCount,
                TreeHeight = _tree.Height,
                MaxKeysPerNode = _maxKeys
            };
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(BTreeIndex));
    }

    private static int ClampEntryCount(long entryCount)
    {
        return entryCount > int.MaxValue ? int.MaxValue : (int)entryCount;
    }

    public override string ToString()
    {
        var countText = Volatile.Read(ref _disposed) != 0 ? "disposed" : EntryCount.ToString();
        return $"BTreeIndex[{Name}] ({_fields.Length} fields, {countText} entries)";
    }
}
