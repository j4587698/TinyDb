using TinyDb.Bson;
using TinyDb.Storage;
using System.Reflection;

namespace TinyDb.Index;

/// <summary>
/// B+ 树索引实现 (基于磁盘分页的中间件)
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
    private readonly object _lock = new();

    public string Name => _name;
    public IReadOnlyList<string> Fields => _fields;
    public bool IsUnique => _unique;
    public int NodeCount { get { lock(_lock) return _tree.NodeCount; } }
    public int EntryCount { get { lock(_lock) return (int)_tree.EntryCount; } }
    public IndexType Type => IndexType.BTree;

    // 兼容性字段：供反射测试使用 (必须是字段 Field)
    // ReSharper disable once InconsistentNaming
    // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
    private BTreeNode _root = null!;

    private void UpdateRootField()
    {
        _root = new BTreeNode(_tree.RootNode, _tree);
    }

    // 构造函数 1: 生产环境使用
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

    // 构造函数 2: 旧测试环境兼容
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

    // 构造函数 3: 最简调用兼容
    public BTreeIndex(string name, string[] fields, bool unique = false)
        : this(name, fields, unique, 200)
    {
    }

    public bool Insert(IndexKey key, BsonValue documentId)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (documentId == null) throw new ArgumentNullException(nameof(documentId));
        
        lock (_lock)
        {
            if (_unique && _tree.Contains(key)) return false;
            _tree.Insert(key, documentId);
            UpdateRootField();
            return true;
        }
    }

    public bool Delete(IndexKey key, BsonValue documentId)
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        
        lock (_lock)
        {
            var res = _tree.Delete(key, documentId);
            UpdateRootField();
            return res;
        }
    }

    public bool Contains(IndexKey key) { lock(_lock) return _tree.Contains(key); }
    public bool Contains(IndexKey key, BsonValue documentId) { lock(_lock) return _tree.Contains(key, documentId); }

    public IEnumerable<BsonValue> Find(IndexKey key) 
    {
        if (key == null) throw new ArgumentNullException(nameof(key));
        lock (_lock) return _tree.Find(key);
    }
    
    public BsonValue? FindExact(IndexKey key) { lock(_lock) return _tree.FindExact(key); }

    public IEnumerable<BsonValue> FindRange(IndexKey startKey, IndexKey endKey, bool includeStart = true, bool includeEnd = true)
    {
        lock (_lock) return _tree.FindRange(startKey, endKey, includeStart, includeEnd).ToList(); // Materialize to avoid locking issues during yield
    }

    public IEnumerable<BsonValue> GetAll() { lock(_lock) return _tree.GetAll().ToList(); }
    
    public void Clear() 
    {
        lock (_lock)
        {
            _tree.Clear();
            UpdateRootField();
        }
    }
    
    public bool Validate() { lock(_lock) return _tree.Validate(); }

    public void Dispose()
    {
        if (!_disposed)
        {
            lock (_lock) _tree.Dispose();
            if (_ownsPageManager)
            {
                _tempPm?.Dispose();
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
        lock (_lock)
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
                AverageKeysPerNode = nodeCount > 0 ? (double)entryCount / nodeCount : 0,
                TreeHeight = _tree.Height,
                MaxKeysPerNode = _maxKeys
            };
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

