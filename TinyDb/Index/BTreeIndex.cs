using TinyDb.Bson;
using TinyDb.Storage;
using System.Reflection;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System;
using System.Threading;

namespace TinyDb.Index;

/// <summary>
/// B+ 树索引实现 (基于磁盘分段并发优化的中间件)
/// </summary>
public sealed class BTreeIndex : IDisposable
{
    private readonly string _name;
    private readonly string[] _fields;
    private readonly bool _unique;
    private readonly DiskBTree _tree;
    private bool _disposed;

    private readonly bool _ownsPageManager;
    private readonly string? _tempFilePath;
    private readonly PageManager? _tempPm;
    private readonly int _maxKeys;
    private readonly ReaderWriterLockSlim _lock = new();

    public string Name => _name;
    public IReadOnlyList<string> Fields => _fields;
    public bool IsUnique => _unique;
    
    public int NodeCount 
    { 
        get 
        { 
            _lock.EnterReadLock(); 
            try { return _tree.NodeCount; } 
            finally { _lock.ExitReadLock(); } 
        } 
    }
    
    public int EntryCount 
    { 
        get 
        { 
            _lock.EnterReadLock(); 
            try { return (int)_tree.EntryCount; } 
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

    public BTreeIndex(PageManager pm, string name, string[] fields, bool unique, int maxKeys)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _fields = fields ?? throw new ArgumentNullException(nameof(fields));
        if (fields.Length == 0) throw new ArgumentException("Fields cannot be empty", nameof(fields));    
        _unique = unique;
        _maxKeys = maxKeys;
        _tree = DiskBTree.Create(pm, maxKeys);
        _ownsPageManager = false;
        UpdateRootField();
    }

    public BTreeIndex(string name, string[] fields, bool unique, int maxKeys)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _fields = fields ?? throw new ArgumentNullException(nameof(fields));
        if (fields.Length == 0) throw new ArgumentException("Fields cannot be empty", nameof(fields));    
        _unique = unique;
        _maxKeys = maxKeys;

        _tempFilePath = Path.Combine(Path.GetTempPath(), $"btree_idx_{Guid.NewGuid():N}.db");
        var ds = new DiskStream(_tempFilePath);
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

        _lock.EnterWriteLock();
        try
        {
            if (_unique && _tree.Contains(key))
            {
                return false;
            }
            _tree.Insert(key, documentId);
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
        _lock.EnterReadLock(); 
        try { return _tree.Contains(key); } 
        finally { _lock.ExitReadLock(); } 
    }

    public bool Contains(IndexKey key, BsonValue documentId) 
    { 
        _lock.EnterReadLock(); 
        try { return _tree.Contains(key, documentId); } 
        finally { _lock.ExitReadLock(); } 
    }

    public IEnumerable<BsonValue> Find(IndexKey key)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        _lock.EnterReadLock();
        try { return _tree.Find(key); }
        finally { _lock.ExitReadLock(); }
    }

    public BsonValue? FindExact(IndexKey key) 
    { 
        _lock.EnterReadLock(); 
        try { return _tree.FindExact(key); } 
        finally { _lock.ExitReadLock(); } 
    }

    public IEnumerable<BsonValue> FindRange(IndexKey startKey, IndexKey endKey, bool includeStart = true, bool includeEnd = true)
    {
        if (startKey == null) throw new ArgumentNullException(nameof(startKey));
        if (endKey == null) throw new ArgumentNullException(nameof(endKey));
        return FindRangeIterator(startKey, endKey, includeStart, includeEnd);
    }

    public IEnumerable<BsonValue> FindRangeReverse(IndexKey startKey, IndexKey endKey, bool includeStart = true, bool includeEnd = true)
    {
        if (startKey == null) throw new ArgumentNullException(nameof(startKey));
        if (endKey == null) throw new ArgumentNullException(nameof(endKey));
        return FindRangeReverseIterator(startKey, endKey, includeStart, includeEnd);
    }

    public IEnumerable<BsonValue> GetAll() 
    { 
        return GetAllIterator();
    } 

    public IEnumerable<BsonValue> GetAllReverse()
    {
        return GetAllReverseIterator();
    }

    private IEnumerable<BsonValue> FindRangeIterator(IndexKey startKey, IndexKey endKey, bool includeStart, bool includeEnd)
    {
        _lock.EnterReadLock();
        try
        {
            foreach (var id in _tree.FindRange(startKey, endKey, includeStart, includeEnd))
            {
                yield return id;
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private IEnumerable<BsonValue> FindRangeReverseIterator(IndexKey startKey, IndexKey endKey, bool includeStart, bool includeEnd)
    {
        _lock.EnterReadLock();
        try
        {
            foreach (var id in _tree.FindRangeReverse(startKey, endKey, includeStart, includeEnd))
            {
                yield return id;
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private IEnumerable<BsonValue> GetAllIterator()
    {
        _lock.EnterReadLock();
        try
        {
            foreach (var id in _tree.GetAll())
            {
                yield return id;
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    private IEnumerable<BsonValue> GetAllReverseIterator()
    {
        _lock.EnterReadLock();
        try
        {
            foreach (var id in _tree.GetAllReverse())
            {
                yield return id;
            }
        }
        finally
        {
            _lock.ExitReadLock();
        }
    }

    public void Clear()
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

    public bool Validate() 
    { 
        _lock.EnterReadLock(); 
        try { return _tree.Validate(); } 
        finally { _lock.ExitReadLock(); } 
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _lock.Dispose();
            _tree.Dispose();
            if (_ownsPageManager)
            {
                _tempPm!.Dispose();
                if (!string.IsNullOrEmpty(_tempFilePath) && File.Exists(_tempFilePath))
                {
                    try { File.Delete(_tempFilePath); } catch { }
                }
            }
            _disposed = true;
        }
    }

    public IndexStatistics GetStatistics()
    {
        ThrowIfDisposed();
        _lock.EnterReadLock();
        try
        {
            var nodeCount = _tree.NodeCount;
            var entryCount = (int)_tree.EntryCount;
            return new IndexStatistics
            {
                Name = Name,
                Type = Type,
                Fields = _fields,
                IsUnique = IsUnique,
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

    private void ThrowIfDisposed() { if (_disposed) throw new ObjectDisposedException(nameof(BTreeIndex)); }

    public override string ToString()
    {
        int count = 0;
        try { count = EntryCount; } catch { }
        return $"BTreeIndex[{Name}] ({_fields.Length} fields, {count} entries)";
    }
}
