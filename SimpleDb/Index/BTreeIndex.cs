using SimpleDb.Bson;

namespace SimpleDb.Index;

/// <summary>
/// B+ 树索引实现
/// </summary>
public sealed class BTreeIndex : IDisposable
{
    private const int DefaultMaxKeys = 100; // 默认最大键数量

    private BTreeNode? _root;
    private readonly int _maxKeys;
    private readonly string _name;
    private readonly string[] _fields;
    private readonly bool _unique;
    private int _nodeCount;
    private int _entryCount;
    private bool _disposed;

    /// <summary>
    /// 索引名称
    /// </summary>
    public string Name => _name;

    /// <summary>
    /// 索引字段
    /// </summary>
    public IReadOnlyList<string> Fields => _fields;

    /// <summary>
    /// 是否为唯一索引
    /// </summary>
    public bool IsUnique => _unique;

    /// <summary>
    /// 节点数量
    /// </summary>
    public int NodeCount => _nodeCount;

    /// <summary>
    /// 索引条目数量
    /// </summary>
    public int EntryCount => _entryCount;

    /// <summary>
    /// 索引类型
    /// </summary>
    public IndexType Type => IndexType.BTree;

    /// <summary>
    /// 初始化 B+ 树索引
    /// </summary>
    /// <param name="name">索引名称</param>
    /// <param name="fields">索引字段</param>
    /// <param name="unique">是否唯一</param>
    /// <param name="maxKeys">最大键数量</param>
    public BTreeIndex(string name, string[] fields, bool unique = false, int maxKeys = DefaultMaxKeys)
    {
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _fields = fields ?? throw new ArgumentNullException(nameof(fields));
        if (fields.Length == 0) throw new ArgumentException("At least one field is required", nameof(fields));

        _unique = unique;
        _maxKeys = maxKeys;
        _root = new BTreeNode(_maxKeys, true); // 初始为叶子节点
        _nodeCount = 1;
        _entryCount = 0;
    }

    /// <summary>
    /// 插入索引条目
    /// </summary>
    /// <param name="key">索引键</param>
    /// <param name="documentId">文档ID</param>
    /// <returns>是否插入成功</returns>
    public bool Insert(IndexKey key, BsonValue documentId)
    {
        ThrowIfDisposed();
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (documentId == null) throw new ArgumentNullException(nameof(documentId));

        if (_unique && Contains(key))
            return false; // 唯一索引冲突 - 检查键是否已存在

        var result = InsertRecursive(_root!, key, documentId);
        if (result.NeedSplit)
        {
            // 分裂根节点
            var newRoot = new BTreeNode(_maxKeys, false);
            var newSibling = _root!.Split();

            // 提升中间键
            var promotedKey = _root.GetKey(_root.KeyCount - 1);

            newRoot.InsertChild(promotedKey, _root);
            newRoot.InsertChild(newSibling.GetKey(0), newSibling);

            _root = newRoot;
            _nodeCount++;
        }

        if (result.Inserted)
            _entryCount++;

        return result.Inserted;
    }

    /// <summary>
    /// 递归插入
    /// </summary>
    /// <param name="node">当前节点</param>
    /// <param name="key">索引键</param>
    /// <param name="documentId">文档ID</param>
    /// <returns>插入结果</returns>
    private InsertResult InsertRecursive(BTreeNode node, IndexKey key, BsonValue documentId)
    {
        if (node.IsLeaf)
        {
            var needSplit = node.Insert(key, documentId);
            return new InsertResult { Inserted = true, NeedSplit = needSplit };
        }

        // 内部节点，找到合适的子节点
        var childIndex = node.FindKeyPosition(key);
        var child = childIndex < node.ChildCount ? node.GetChild(childIndex) : node.GetChild(childIndex - 1);

        var childResult = InsertRecursive(child, key, documentId);

        if (childResult.NeedSplit)
        {
            // 子节点需要分裂
            var newSibling = child.Split();
            var promotedKey = child.GetKey(child.KeyCount - 1);

            node.InsertChild(promotedKey, newSibling);
            _nodeCount++;

            return new InsertResult { Inserted = childResult.Inserted, NeedSplit = node.IsFull() };
        }

        return new InsertResult { Inserted = childResult.Inserted, NeedSplit = false };
    }

    /// <summary>
    /// 删除索引条目
    /// </summary>
    /// <param name="key">索引键</param>
    /// <param name="documentId">文档ID</param>
    /// <returns>是否删除成功</returns>
    public bool Delete(IndexKey key, BsonValue documentId)
    {
        ThrowIfDisposed();
        if (key == null) throw new ArgumentNullException(nameof(key));
        if (documentId == null) throw new ArgumentNullException(nameof(documentId));

        var deleted = DeleteRecursive(_root!, key, documentId);
        if (deleted)
            _entryCount--;

        return deleted;
    }

    /// <summary>
    /// 递归删除
    /// </summary>
    /// <param name="node">当前节点</param>
    /// <param name="key">索引键</param>
    /// <param name="documentId">文档ID</param>
    /// <returns>是否删除成功</returns>
    private bool DeleteRecursive(BTreeNode node, IndexKey key, BsonValue documentId)
    {
        if (node.IsLeaf)
        {
            return node.Delete(key, documentId);
        }

        // 内部节点，找到合适的子节点
        var childIndex = node.FindKeyPosition(key);
        var child = childIndex < node.ChildCount ? node.GetChild(childIndex) : node.GetChild(childIndex - 1);

        var deleted = DeleteRecursive(child, key, documentId);

        if (deleted && child.NeedsMerge())
        {
            // 子节点需要合并或重平衡
            RebalanceOrMerge(node, childIndex);
        }

        return deleted;
    }

    /// <summary>
    /// 重平衡或合并节点
    /// </summary>
    /// <param name="parent">父节点</param>
    /// <param name="childIndex">子节点索引</param>
    private void RebalanceOrMerge(BTreeNode parent, int childIndex)
    {
        // 尝试从兄弟节点借键
        if (childIndex > 0)
        {
            var leftSibling = parent.GetChild(childIndex - 1);
            if (leftSibling.KeyCount > leftSibling.MinKeys)
            {
                var separatorKey = parent.GetKey(childIndex - 1);
                var newSeparatorKey = parent.GetChild(childIndex).BorrowFromSibling(leftSibling, true, separatorKey);
                parent.SetKey(childIndex - 1, newSeparatorKey);
                return;
            }
        }

        if (childIndex < parent.ChildCount - 1)
        {
            var rightSibling = parent.GetChild(childIndex + 1);
            if (rightSibling.KeyCount > rightSibling.MinKeys)
            {
                var separatorKey = parent.GetKey(childIndex);
                var newSeparatorKey = parent.GetChild(childIndex).BorrowFromSibling(rightSibling, false, separatorKey);
                parent.SetKey(childIndex, newSeparatorKey);
                return;
            }
        }

        // 需要合并
        MergeNodes(parent, childIndex);
    }

    /// <summary>
    /// 合并节点
    /// </summary>
    /// <param name="parent">父节点</param>
    /// <param name="childIndex">子节点索引</param>
    private void MergeNodes(BTreeNode parent, int childIndex)
    {
        if (childIndex >= parent.ChildCount - 1)
            childIndex--; // 合并到左兄弟

        var leftChild = parent.GetChild(childIndex);
        var rightChild = parent.GetChild(childIndex + 1);
        var separatorKey = parent.GetKey(childIndex);

        leftChild.Merge(rightChild, separatorKey);

        // 从父节点中移除分隔键和右兄弟
        parent.RemoveKeyAt(childIndex);
        parent.RemoveChildAt(childIndex + 1);
        _nodeCount--;

        // 如果父节点为根且只有一个键，提升左孩子为新的根
        if (parent == _root && parent.KeyCount == 0 && !parent.IsLeaf)
        {
            _root = leftChild;
            _nodeCount--;
        }
    }

    /// <summary>
    /// 查找索引条目
    /// </summary>
    /// <param name="key">索引键</param>
    /// <returns>匹配的文档ID列表</returns>
    public IEnumerable<BsonValue> Find(IndexKey key)
    {
        ThrowIfDisposed();
        if (key == null) throw new ArgumentNullException(nameof(key));

        var node = _root;
        while (node != null)
        {
            if (node.IsLeaf)
            {
                var position = node.FindKeyPosition(key);
                if (position < node.KeyCount && node.GetKey(position).Equals(key))
                {
                    // 返回该键对应的所有文档ID
                    var documentIds = node.GetDocumentIds(position);
                    foreach (var docId in documentIds)
                    {
                        yield return docId;
                    }
                }
                yield break;
            }

            var childIndex = node.FindKeyPosition(key);
            node = childIndex < node.ChildCount ? node.GetChild(childIndex) : null;
        }
    }

    /// <summary>
    /// 范围查询
    /// </summary>
    /// <param name="startKey">起始键</param>
    /// <param name="endKey">结束键</param>
    /// <param name="includeStart">是否包含起始键</param>
    /// <param name="includeEnd">是否包含结束键</param>
    /// <returns>匹配的文档ID列表</returns>
    public IEnumerable<BsonValue> FindRange(IndexKey startKey, IndexKey endKey, bool includeStart = true, bool includeEnd = true)
    {
        ThrowIfDisposed();
        if (startKey == null) throw new ArgumentNullException(nameof(startKey));
        if (endKey == null) throw new ArgumentNullException(nameof(endKey));

        // 找到起始位置
        var startNode = FindLeafNode(startKey);
        if (startNode == null) yield break;

        var startIndex = startNode.FindKeyPosition(startKey);
        if (!includeStart && startIndex < startNode.KeyCount && startNode.GetKey(startIndex).Equals(startKey))
        {
            startIndex++;
        }

        // 遍历叶子节点
        var currentNode = startNode;
        var currentIndex = startIndex;

        while (currentNode != null)
        {
            while (currentIndex < currentNode.KeyCount)
            {
                var currentKey = currentNode.GetKey(currentIndex);
                var comparison = currentKey.CompareTo(endKey);

                if (comparison > 0) yield break;
                if (comparison == 0 && !includeEnd) yield break;

                yield return currentNode.GetDocumentId(currentIndex);
                currentIndex++;
            }

            // 移动到下一个叶子节点
            currentNode = GetNextLeafNode(currentNode);
            currentIndex = 0;
        }
    }

    /// <summary>
    /// 查找包含指定键的叶子节点
    /// </summary>
    /// <param name="key">要查找的键</param>
    /// <returns>叶子节点</returns>
    private BTreeNode? FindLeafNode(IndexKey key)
    {
        var node = _root;
        while (node != null && !node.IsLeaf)
        {
            var childIndex = node.FindKeyPosition(key);
            node = childIndex < node.ChildCount ? node.GetChild(childIndex) : null;
        }
        return node;
    }

    /// <summary>
    /// 获取下一个叶子节点
    /// </summary>
    /// <param name="currentNode">当前叶子节点</param>
    /// <returns>下一个叶子节点</returns>
    private BTreeNode? GetNextLeafNode(BTreeNode currentNode)
    {
        // 简化实现：这里需要维护叶子节点的链表结构
        // 实际实现中应该在每个节点中存储指向下一个叶子节点的指针
        return null;
    }

    /// <summary>
    /// 检查是否包含指定的键值对
    /// </summary>
    /// <param name="key">索引键</param>
    /// <param name="documentId">文档ID</param>
    /// <returns>是否包含</returns>
    public bool Contains(IndexKey key, BsonValue documentId)
    {
        return Find(key).Contains(documentId);
    }

    /// <summary>
    /// 检查是否包含指定的键
    /// </summary>
    /// <param name="key">索引键</param>
    /// <returns>是否包含</returns>
    public bool Contains(IndexKey key)
    {
        return Find(key).Any();
    }

    /// <summary>
    /// 获取所有文档ID
    /// </summary>
    /// <returns>所有文档ID</returns>
    public IEnumerable<BsonValue> GetAll()
    {
        ThrowIfDisposed();

        // 遍历所有叶子节点
        var leafNode = FindFirstLeafNode();
        while (leafNode != null)
        {
            for (int i = 0; i < leafNode.KeyCount; i++)
            {
                yield return leafNode.GetDocumentId(i);
            }
            leafNode = GetNextLeafNode(leafNode);
        }
    }

    /// <summary>
    /// 查找第一个叶子节点
    /// </summary>
    /// <returns>第一个叶子节点</returns>
    private BTreeNode? FindFirstLeafNode()
    {
        var node = _root;
        while (node != null && !node.IsLeaf)
        {
            node = node.GetChild(0);
        }
        return node;
    }

    /// <summary>
    /// 清空索引
    /// </summary>
    public void Clear()
    {
        ThrowIfDisposed();

        _root = new BTreeNode(_maxKeys, true);
        _nodeCount = 1;
        _entryCount = 0;
    }

    /// <summary>
    /// 获取索引统计信息
    /// </summary>
    /// <returns>统计信息</returns>
    public IndexStatistics GetStatistics()
    {
        ThrowIfDisposed();

        return new IndexStatistics
        {
            Name = _name,
            Type = Type,
            Fields = _fields,
            IsUnique = _unique,
            NodeCount = _nodeCount,
            EntryCount = _entryCount,
            MaxKeysPerNode = _maxKeys,
            AverageKeysPerNode = _nodeCount > 0 ? (double)_entryCount / _nodeCount : 0,
            TreeHeight = GetTreeHeight(),
            RootIsLeaf = _root?.IsLeaf ?? true
        };
    }

    /// <summary>
    /// 获取树的高度
    /// </summary>
    /// <returns>树的高度</returns>
    private int GetTreeHeight()
    {
        var height = 0;
        var node = _root;
        while (node != null)
        {
            height++;
            if (node.IsLeaf) break;
            node = node.GetChild(0);
        }
        return height;
    }

    /// <summary>
    /// 验证索引完整性
    /// </summary>
    /// <returns>是否有效</returns>
    public bool Validate()
    {
        ThrowIfDisposed();
        return _root != null && ValidateNode(_root);
    }

    /// <summary>
    /// 验证节点
    /// </summary>
    /// <param name="node">要验证的节点</param>
    /// <returns>是否有效</returns>
    private bool ValidateNode(BTreeNode node)
    {
        // 检查键的顺序
        for (int i = 1; i < node.KeyCount; i++)
        {
            if (node.GetKey(i - 1).CompareTo(node.GetKey(i)) >= 0)
                return false;
        }

        // 检查唯一索引约束
        if (_unique && node.IsLeaf)
        {
            for (int i = 1; i < node.KeyCount; i++)
            {
                if (node.GetKey(i - 1).Equals(node.GetKey(i)))
                    return false;
            }
        }

        // 递归验证子节点
        if (!node.IsLeaf)
        {
            for (int i = 0; i < node.ChildCount; i++)
            {
                if (!ValidateNode(node.GetChild(i)))
                    return false;
            }
        }

        return true;
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(BTreeIndex));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _root = null;
            _disposed = true;
        }
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"BTreeIndex[{_name}]: {_fields.Length} fields, {_entryCount} entries, {_nodeCount} nodes";
    }
}

/// <summary>
/// 插入结果
/// </summary>
public sealed class InsertResult
{
    public bool Inserted { get; init; }
    public bool NeedSplit { get; init; }
}

/// <summary>
/// 索引统计信息
/// </summary>
public sealed class IndexStatistics
{
    public string Name { get; init; } = string.Empty;
    public IndexType Type { get; init; }
    public string[] Fields { get; init; } = Array.Empty<string>();
    public bool IsUnique { get; init; }
    public int NodeCount { get; init; }
    public int EntryCount { get; init; }
    public int MaxKeysPerNode { get; init; }
    public double AverageKeysPerNode { get; init; }
    public int TreeHeight { get; init; }
    public bool RootIsLeaf { get; init; }

    public override string ToString()
    {
        return $"Index[{Name}]: {Type}, {Fields.Length} fields, {EntryCount} entries, " +
               $"{NodeCount} nodes, Height={TreeHeight}";
    }
}