using TinyDb.Bson;
using TinyDb.Storage;
using System.Collections.Concurrent;

namespace TinyDb.Index;

public sealed class IndexManager : IDisposable
{
    private readonly ConcurrentDictionary<string, BTreeIndex> _indexes;
    private readonly string _collectionName;
    private readonly PageManager _pm;
    private readonly ReaderWriterLockSlim _rwLock = new();
    private bool _disposed;
    
    // 为了兼容性
    private readonly bool _ownsPageManager;
    private readonly string? _tempFilePath;
    private readonly PageManager? _tempPm;

    /// <summary>
    /// 获取此管理器所属集合的名称。
    /// </summary>
    public string CollectionName => _collectionName;

    /// <summary>
    /// 获取管理的索引数量。
    /// </summary>
    public int IndexCount => _indexes.Count;

    /// <summary>
    /// 获取所有受管理索引的名称。
    /// </summary>
    public IEnumerable<string> IndexNames => _indexes.Keys;

    /// <summary>
    /// 初始化 <see cref="IndexManager"/> 类的新实例。
    /// </summary>
    /// <param name="collectionName">集合名称。</param>
    /// <param name="pm">页面管理器。</param>
    public IndexManager(string collectionName, PageManager pm)
    {
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
        _pm = pm ?? throw new ArgumentNullException(nameof(pm));
        _indexes = new ConcurrentDictionary<string, BTreeIndex>();
        _ownsPageManager = false;
    }

    /// <summary>
    /// 使用临时存储初始化 <see cref="IndexManager"/> 类的新实例。
    /// </summary>
    /// <param name="collectionName">集合名称。</param>
    public IndexManager(string collectionName)
    {
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
        _tempFilePath = Path.Combine(Path.GetTempPath(), $"idx_mgr_{Guid.NewGuid():N}.db");
        var ds = new DiskStream(_tempFilePath);
        _tempPm = new PageManager(ds);
        _pm = _tempPm;
        _indexes = new ConcurrentDictionary<string, BTreeIndex>();
        _ownsPageManager = true;
    }

    /// <summary>
    /// 创建一个新索引。
    /// </summary>
    /// <param name="name">索引名称。</param>
    /// <param name="fields">要索引的字段。</param>
    /// <param name="unique">索引是否应该是唯一的。</param>
    /// <returns>如果创建成功则为 true；如果已存在则为 false。</returns>
    public bool CreateIndex(string name, string[] fields, bool unique = false)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));
        if (fields == null || fields.Length == 0) throw new ArgumentException("Fields cannot be null or empty", nameof(fields));
        
        _rwLock.EnterWriteLock();
        try
        {
            if (_indexes.ContainsKey(name)) return false;
            var normalizedFields = fields.Select(ToCamelCase).ToArray();
            var index = new BTreeIndex(_pm, name, normalizedFields, unique);
            return _indexes.TryAdd(name, index);
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();
        return char.ToLowerInvariant(name[0]) + name.Substring(1);
    }

    /// <summary>
    /// 根据名称删除索引。
    /// </summary>
    /// <param name="name">要删除的索引名称。</param>
    /// <returns>如果删除成功则为 true；如果未找到则为 false。</returns>
    public bool DropIndex(string name)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));
        
        _rwLock.EnterWriteLock();
        try
        {
            if (_indexes.TryRemove(name, out var index))
            {
                index.Dispose();
                return true;
            }
            return false;
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// 根据名称检索索引。
    /// </summary>
    /// <param name="name">索引名称。</param>
    /// <returns>索引实例，如果未找到则为 null。</returns>
    public BTreeIndex? GetIndex(string name)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));
        _indexes.TryGetValue(name, out var index);
        return index;
    }

    /// <summary>
    /// 检查索引是否存在。
    /// </summary>
    /// <param name="name">索引名称。</param>
    /// <returns>如果存在则为 true。</returns>
    public bool IndexExists(string name)
    {
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));
        return _indexes.ContainsKey(name);
    }

    /// <summary>
    /// 获取所有索引的统计信息。
    /// </summary>
    /// <returns>索引统计信息的集合。</returns>
    public IEnumerable<IndexStatistics> GetAllStatistics()
    {
        return _indexes.Values.Select(i => i.GetStatistics());
    }

    /// <summary>
    /// 为一组查询字段找到最佳匹配索引。
    /// </summary>
    /// <param name="fields">查询字段。</param>
    /// <returns>最佳匹配索引，或 null。</returns>
    public BTreeIndex? GetBestIndex(string[] fields)
    {
        if (fields == null || fields.Length == 0) return null;

        BTreeIndex? bestIndex = null;
        int bestScore = -1;

        _rwLock.EnterReadLock();
        try
        {
            foreach (var index in _indexes.Values)
            {
                var score = CalculateIndexScore(index, fields);
                if (score > bestScore)
                {
                    bestScore = score;
                    bestIndex = index;
                }
            }
        }
        finally
        {
            _rwLock.ExitReadLock();
        }

        return bestScore > 0 ? bestIndex : null;
    }

    private static int CalculateIndexScore(BTreeIndex index, string[] fields)
    {
        var indexFields = index.Fields;
        int score = 0;

        for (int i = 0; i < Math.Min(indexFields.Count, fields.Length); i++)
        {
            if (string.Equals(indexFields[i], fields[i], StringComparison.OrdinalIgnoreCase))
            {
                score += (indexFields.Count - i) * 10;
            }
            else
            {
                break;
            }
        }

        if (index.IsUnique)
            score += 5;

        return score;
    }

    /// <summary>
    /// 插入文档时更新所有索引。
    /// </summary>
    /// <param name="document">正在插入的文档。</param>
    /// <param name="documentId">文档的 ID。</param>
    public void InsertDocument(BsonDocument document, BsonValue documentId)
    {
        _rwLock.EnterReadLock();
        try
        {
            foreach (var index in _indexes.Values)
            {
                var key = ExtractIndexKey(index, document);
                if (key != null)
                {
                    if (!index.Insert(key, documentId))
                    {
                        throw new InvalidOperationException($"Duplicate key detected in unique index '{index.Name}'");
                    }
                }
            }
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// 更新文档时更新所有索引。
    /// </summary>
    /// <param name="oldDoc">原始文档。</param>
    /// <param name="newDoc">新文档。</param>
    /// <param name="id">文档 ID。</param>
    public void UpdateDocument(BsonDocument oldDoc, BsonDocument newDoc, BsonValue id)
    {
        _rwLock.EnterReadLock();
        try
        {
            foreach (var index in _indexes.Values)
            {
                var oldKey = ExtractIndexKey(index, oldDoc);
                var newKey = ExtractIndexKey(index, newDoc);
                
                if (oldKey != null) index.Delete(oldKey, id);
                if (newKey != null)
                {
                    if (!index.Insert(newKey, id))
                    {
                        // Attempt to rollback delete
                        if (oldKey != null) index.Insert(oldKey, id);
                        throw new InvalidOperationException($"Duplicate key detected in unique index '{index.Name}'");
                    }
                }
            }
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    /// <summary>
    /// 删除文档时更新所有索引。
    /// </summary>
    /// <param name="doc">正在删除的文档。</param>
    /// <param name="id">文档 ID。</param>
    public void DeleteDocument(BsonDocument doc, BsonValue id)
    {
        _rwLock.EnterReadLock();
        try
        {
            foreach (var index in _indexes.Values)
            {
                var key = ExtractIndexKey(index, doc);
                if (key != null) index.Delete(key, id);
            }
        }
        finally
        {
            _rwLock.ExitReadLock();
        }
    }

    private IndexKey? ExtractIndexKey(BTreeIndex index, BsonDocument doc)
    {
        if (index.Fields.Count == 0) return null;
        
        // 单字段优化
        if (index.Fields.Count == 1)
        {
            return doc.TryGetValue(index.Fields[0], out var val) ? new IndexKey(val) : new IndexKey(BsonNull.Value);
        }

        // 复合索引
        var values = new BsonValue[index.Fields.Count];
        for (int i = 0; i < index.Fields.Count; i++)
        {
            if (doc.TryGetValue(index.Fields[i], out var val))
            {
                values[i] = val;
            }
            else
            {
                values[i] = BsonNull.Value;
            }
        }
        
        return new IndexKey(values);
    }

    /// <summary>
    /// 释放索引管理器和所有索引。
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _rwLock.Dispose();
            foreach (var i in _indexes.Values) i.Dispose();
            
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
}
