using TinyDb.Bson;
using System.Collections.Concurrent;

namespace TinyDb.Index;

/// <summary>
/// 索引管理器
/// </summary>
public sealed class IndexManager : IDisposable
{
    private readonly ConcurrentDictionary<string, BTreeIndex> _indexes;
    private readonly string _collectionName;
    private readonly object _indexLock = new();
    private bool _disposed;

    /// <summary>
    /// 集合名称
    /// </summary>
    public string CollectionName => _collectionName;

    /// <summary>
    /// 索引数量
    /// </summary>
    public int IndexCount => _indexes.Count;

    /// <summary>
    /// 获取所有索引名称
    /// </summary>
    public IEnumerable<string> IndexNames => _indexes.Keys;

    /// <summary>
    /// 初始化索引管理器
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    public IndexManager(string collectionName)
    {
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
        _indexes = new ConcurrentDictionary<string, BTreeIndex>();
    }

    /// <summary>
    /// 创建索引
    /// </summary>
    /// <param name="name">索引名称</param>
    /// <param name="fields">索引字段</param>
    /// <param name="unique">是否唯一</param>
    /// <returns>是否创建成功</returns>
    public bool CreateIndex(string name, string[] fields, bool unique = false)
    {
        ThrowIfDisposed();
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));
        if (fields == null || fields.Length == 0) throw new ArgumentException("At least one field is required", nameof(fields));

        // 检查索引是否已存在
        if (_indexes.ContainsKey(name))
            return false;

        // 创建索引
        var index = new BTreeIndex(name, fields, unique);
        return _indexes.TryAdd(name, index);
    }

    /// <summary>
    /// 删除索引
    /// </summary>
    /// <param name="name">索引名称</param>
    /// <returns>是否删除成功</returns>
    public bool DropIndex(string name)
    {
        ThrowIfDisposed();
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));

        if (_indexes.TryRemove(name, out var index))
        {
            index.Dispose();
            return true;
        }

        return false;
    }

    /// <summary>
    /// 获取索引
    /// </summary>
    /// <param name="name">索引名称</param>
    /// <returns>索引实例</returns>
    public BTreeIndex? GetIndex(string name)
    {
        ThrowIfDisposed();
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));

        _indexes.TryGetValue(name, out var index);
        return index;
    }

    /// <summary>
    /// 检查索引是否存在
    /// </summary>
    /// <param name="name">索引名称</param>
    /// <returns>是否存在</returns>
    public bool IndexExists(string name)
    {
        ThrowIfDisposed();
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));

        return _indexes.ContainsKey(name);
    }

    /// <summary>
    /// 获取字段的最佳索引
    /// </summary>
    /// <param name="fields">字段列表</param>
    /// <returns>最佳索引</returns>
    public BTreeIndex? GetBestIndex(string[] fields)
    {
        ThrowIfDisposed();
        if (fields == null || fields.Length == 0) return null;

        BTreeIndex? bestIndex = null;
        int bestScore = -1;

        foreach (var index in _indexes.Values)
        {
            var score = CalculateIndexScore(index, fields);
            if (score > bestScore)
            {
                bestScore = score;
                bestIndex = index;
            }
        }

        return bestScore > 0 ? bestIndex : null;
    }

    /// <summary>
    /// 计算索引匹配分数
    /// </summary>
    /// <param name="index">索引</param>
    /// <param name="fields">字段列表</param>
    /// <returns>匹配分数</returns>
    private static int CalculateIndexScore(BTreeIndex index, string[] fields)
    {
        var indexFields = index.Fields;
        int score = 0;

        // 检查前缀匹配
        for (int i = 0; i < Math.Min(indexFields.Count, fields.Length); i++)
        {
            if (string.Equals(indexFields[i], fields[i], StringComparison.OrdinalIgnoreCase))
            {
                score += (indexFields.Count - i) * 10; // 前缀匹配权重更高
            }
            else
            {
                break;
            }
        }

        // 唯一索引加分
        if (index.IsUnique)
            score += 5;

        return score;
    }

    /// <summary>
    /// 插入文档到所有索引
    /// </summary>
    /// <param name="document">文档</param>
    /// <param name="documentId">文档ID</param>
    public void InsertDocument(BsonDocument document, BsonValue documentId)
    {
        ThrowIfDisposed();
        if (document == null) throw new ArgumentNullException(nameof(document));
        if (documentId == null) throw new ArgumentNullException(nameof(documentId));

        lock (_indexLock)
        {
            foreach (var index in _indexes.Values)
            {
                try
                {
                    var key = ExtractIndexKey(index, document);
                    if (key != null)
                    {
                        index.Insert(key, documentId);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error inserting document into index {index.Name}: {ex.Message}");
                }
            }
        }
    }

    /// <summary>
    /// 从所有索引中删除文档
    /// </summary>
    /// <param name="document">文档</param>
    /// <param name="documentId">文档ID</param>
    public void DeleteDocument(BsonDocument document, BsonValue documentId)
    {
        ThrowIfDisposed();
        if (document == null) throw new ArgumentNullException(nameof(document));
        if (documentId == null) throw new ArgumentNullException(nameof(documentId));

        lock (_indexLock)
        {
            foreach (var index in _indexes.Values)
            {
                try
                {
                    var key = ExtractIndexKey(index, document);
                    if (key != null)
                    {
                        index.Delete(key, documentId);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error deleting document from index {index.Name}: {ex.Message}");
                }
            }
        }
    }

    /// <summary>
    /// 更新文档在所有索引中的条目
    /// </summary>
    /// <param name="oldDocument">旧文档</param>
    /// <param name="newDocument">新文档</param>
    /// <param name="documentId">文档ID</param>
    public void UpdateDocument(BsonDocument oldDocument, BsonDocument newDocument, BsonValue documentId)
    {
        ThrowIfDisposed();
        if (oldDocument == null) throw new ArgumentNullException(nameof(oldDocument));
        if (newDocument == null) throw new ArgumentNullException(nameof(newDocument));
        if (documentId == null) throw new ArgumentNullException(nameof(documentId));

        lock (_indexLock)
        {
            foreach (var index in _indexes.Values)
            {
                try
                {
                    var oldKey = ExtractIndexKey(index, oldDocument);
                    var newKey = ExtractIndexKey(index, newDocument);

                    if (oldKey != null && newKey != null && oldKey.Equals(newKey))
                        continue;

                    if (oldKey != null)
                    {
                        index.Delete(oldKey, documentId);
                    }

                    if (newKey != null)
                    {
                        index.Insert(newKey, documentId);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error updating document in index {index.Name}: {ex.Message}");
                }
            }
        }
    }

    /// <summary>
    /// 从文档中提取索引键
    /// </summary>
    /// <param name="index">索引</param>
    /// <param name="document">文档</param>
    /// <returns>索引键</returns>
    private static IndexKey? ExtractIndexKey(BTreeIndex index, BsonDocument document)
    {
        var values = new List<BsonValue>();

        foreach (var field in index.Fields)
        {
            // 将C#属性名转换为BSON字段名（camelCase）
            var bsonField = char.ToLowerInvariant(field[0]) + field.Substring(1);

            if (document.TryGetValue(bsonField, out var value))
            {
                values.Add(value);
            }
            else if (document.TryGetValue(field, out value)) // 保持兼容性，也尝试原始字段名
            {
                values.Add(value);
            }
            else
            {
                // 字段不存在，无法创建索引键
                return null;
            }
        }

        return values.Count > 0 ? new IndexKey(values.ToArray()) : null;
    }

    /// <summary>
    /// 重建索引
    /// </summary>
    /// <param name="name">索引名称</param>
    /// <param name="documents">文档集合</param>
    /// <returns>是否重建成功</returns>
    public bool RebuildIndex(string name, IEnumerable<BsonDocument> documents)
    {
        ThrowIfDisposed();
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Index name cannot be null or empty", nameof(name));
        if (documents == null) throw new ArgumentNullException(nameof(documents));

        if (!_indexes.TryGetValue(name, out var index))
            return false;

        // 清空索引
        index.Clear();

        // 重新插入所有文档
        foreach (var document in documents)
        {
            if (document.TryGetValue("_id", out var documentId))
            {
                var key = ExtractIndexKey(index, document);
                if (key != null)
                {
                    index.Insert(key, documentId);
                }
            }
        }

        return true;
    }

    /// <summary>
    /// 获取所有索引统计信息
    /// </summary>
    /// <returns>索引统计信息列表</returns>
    public IEnumerable<IndexStatistics> GetAllStatistics()
    {
        ThrowIfDisposed();

        return _indexes.Values.Select(index => index.GetStatistics());
    }

    /// <summary>
    /// 验证所有索引
    /// </summary>
    /// <returns>验证结果</returns>
    public IndexValidationResult ValidateAllIndexes()
    {
        ThrowIfDisposed();

        var result = new IndexValidationResult
        {
            TotalIndexes = _indexes.Count,
            ValidIndexes = 0,
            InvalidIndexes = 0,
            Errors = new List<string>()
        };

        foreach (var kvp in _indexes)
        {
            try
            {
                if (kvp.Value.Validate())
                {
                    result.ValidIndexes++;
                }
                else
                {
                    result.InvalidIndexes++;
                    result.Errors.Add($"Index '{kvp.Key}' validation failed");
                }
            }
            catch (Exception ex)
            {
                result.InvalidIndexes++;
                result.Errors.Add($"Index '{kvp.Key}' validation error: {ex.Message}");
            }
        }

        return result;
    }

    /// <summary>
    /// 清空所有索引
    /// </summary>
    public void ClearAllIndexes()
    {
        ThrowIfDisposed();

        foreach (var index in _indexes.Values)
        {
            index.Clear();
        }
    }

    /// <summary>
    /// 删除所有索引
    /// </summary>
    public void DropAllIndexes()
    {
        ThrowIfDisposed();

        foreach (var index in _indexes.Values)
        {
            index.Dispose();
        }

        _indexes.Clear();
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(IndexManager));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            foreach (var index in _indexes.Values)
            {
                index.Dispose();
            }

            _indexes.Clear();
            _disposed = true;
        }
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"IndexManager[{_collectionName}]: {_indexes.Count} indexes";
    }
}

/// <summary>
/// 索引验证结果
/// </summary>
public sealed class IndexValidationResult
{
    public int TotalIndexes { get; set; }
    public int ValidIndexes { get; set; }
    public int InvalidIndexes { get; set; }
    public List<string> Errors { get; set; } = new();

    public bool IsValid => InvalidIndexes == 0;

    public override string ToString()
    {
        return $"IndexValidation: {ValidIndexes}/{TotalIndexes} valid, {InvalidIndexes} invalid";
    }
}
