using SimpleDb.Bson;

namespace SimpleDb.Index;

/// <summary>
/// B+ 树节点
/// </summary>
public sealed class BTreeNode
{
    private readonly List<IndexKey> _keys;
    private readonly List<BTreeNode> _children;
    private readonly List<List<BsonValue>> _documentIdLists;
    private bool _isLeaf;

    /// <summary>
    /// 节点是否为叶子节点
    /// </summary>
    public bool IsLeaf
    {
        get => _isLeaf;
        set => _isLeaf = value;
    }

    /// <summary>
    /// 键的数量
    /// </summary>
    public int KeyCount => _keys.Count;

    /// <summary>
    /// 子节点数量
    /// </summary>
    public int ChildCount => _children.Count;

    /// <summary>
    /// 文档ID数量（仅叶子节点）
    /// </summary>
    public int DocumentCount => _documentIdLists.Sum(list => list.Count);

    /// <summary>
    /// 最大键数量
    /// </summary>
    public int MaxKeys { get; }

    /// <summary>
    /// 最小键数量
    /// </summary>
    public int MinKeys { get; }

    /// <summary>
    /// 获取指定索引的键
    /// </summary>
    /// <param name="index">索引</param>
    /// <returns>键</returns>
    public IndexKey GetKey(int index) => _keys[index];

    /// <summary>
    /// 设置指定索引的键
    /// </summary>
    /// <param name="index">索引</param>
    /// <param name="key">键</param>
    public void SetKey(int index, IndexKey key) => _keys[index] = key;

    /// <summary>
    /// 获取指定索引的子节点
    /// </summary>
    /// <param name="index">索引</param>
    /// <returns>子节点</returns>
    public BTreeNode GetChild(int index) => _children[index];

    /// <summary>
    /// 设置指定索引的子节点
    /// </summary>
    /// <param name="index">索引</param>
    /// <param name="child">子节点</param>
    public void SetChild(int index, BTreeNode child) => _children[index] = child;

    /// <summary>
    /// 移除指定索引的键
    /// </summary>
    /// <param name="index">索引</param>
    public void RemoveKeyAt(int index) => _keys.RemoveAt(index);

    /// <summary>
    /// 移除指定索引的子节点
    /// </summary>
    /// <param name="index">索引</param>
    public void RemoveChildAt(int index) => _children.RemoveAt(index);

    /// <summary>
    /// 获取指定索引的文档ID列表
    /// </summary>
    /// <param name="index">索引</param>
    /// <returns>文档ID列表</returns>
    public IReadOnlyList<BsonValue> GetDocumentIds(int index) => _documentIdLists[index];

    /// <summary>
    /// 获取指定索引的第一个文档ID（为了向后兼容）
    /// </summary>
    /// <param name="index">索引</param>
    /// <returns>文档ID</returns>
    public BsonValue GetDocumentId(int index) => _documentIdLists[index].Count > 0 ? _documentIdLists[index][0] : BsonNull.Value;

    /// <summary>
    /// 初始化 B+ 树节点
    /// </summary>
    /// <param name="maxKeys">最大键数量</param>
    /// <param name="isLeaf">是否为叶子节点</param>
    public BTreeNode(int maxKeys, bool isLeaf = false)
    {
        MaxKeys = maxKeys;
        MinKeys = maxKeys / 2;
        _keys = new List<IndexKey>(maxKeys);
        _children = new List<BTreeNode>(maxKeys + 1);
        _documentIdLists = new List<List<BsonValue>>(maxKeys);
        _isLeaf = isLeaf;
    }

    /// <summary>
    /// 查找键的位置
    /// </summary>
    /// <param name="key">要查找的键</param>
    /// <returns>键的位置，如果不存在则返回应插入的位置</returns>
    public int FindKeyPosition(IndexKey key)
    {
        var left = 0;
        var right = _keys.Count - 1;

        while (left <= right)
        {
            var mid = (left + right) / 2;
            var comparison = key.CompareTo(_keys[mid]);

            if (comparison == 0)
                return mid;
            else if (comparison < 0)
                right = mid - 1;
            else
                left = mid + 1;
        }

        return left;
    }

    /// <summary>
    /// 查找文档ID的位置
    /// </summary>
    /// <param name="documentId">文档ID</param>
    /// <returns>位置，如果不存在则返回-1</returns>
    public int FindDocumentPosition(BsonValue documentId)
    {
        for (int i = 0; i < _documentIdLists.Count; i++)
        {
            if (_documentIdLists[i].Contains(documentId))
                return i;
        }
        return -1;
    }

    /// <summary>
    /// 插入键值对
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="documentId">文档ID</param>
    /// <returns>是否需要分裂</returns>
    public bool Insert(IndexKey key, BsonValue documentId)
    {
        if (!_isLeaf)
            throw new InvalidOperationException("Cannot insert into non-leaf node");

        var position = FindKeyPosition(key);

        // 检查键是否已存在
        if (position < _keys.Count && _keys[position].Equals(key))
        {
            // 键已存在，在对应位置的文档列表前面插入新的文档ID
            _documentIdLists[position].Insert(0, documentId);
            // 不增加KeyCount，因为key是相同的
            return _documentIdLists.Count > MaxKeys;
        }

        // 插入新键和文档ID列表
        _keys.Insert(position, key);
        _documentIdLists.Insert(position, new List<BsonValue> { documentId });

        return _keys.Count > MaxKeys;
    }

    /// <summary>
    /// 插入子节点
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="child">子节点</param>
    /// <returns>是否需要分裂</returns>
    public bool InsertChild(IndexKey key, BTreeNode child)
    {
        if (_isLeaf)
            throw new InvalidOperationException("Cannot insert child into leaf node");

        var position = FindKeyPosition(key);

        // 确保position在有效范围内
        position = Math.Min(position, _keys.Count);

        _keys.Insert(position, key);

        // 对于子节点，插入位置应该是position+1或position
        if (position < _children.Count)
        {
            _children.Insert(position + 1, child);
        }
        else
        {
            _children.Add(child);
        }

        return _keys.Count > MaxKeys;
    }

    /// <summary>
    /// 删除键值对
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="documentId">文档ID</param>
    /// <returns>是否删除成功</returns>
    public bool Delete(IndexKey key, BsonValue documentId)
    {
        if (!_isLeaf)
            throw new InvalidOperationException("Cannot delete from non-leaf node");

        var position = FindKeyPosition(key);

        if (position >= _keys.Count || !_keys[position].Equals(key))
            return false; // 键不存在

        // 从该键对应的文档列表中删除指定的文档ID
        var docList = _documentIdLists[position];
        var removed = docList.Remove(documentId);

        if (removed && docList.Count == 0)
        {
            // 如果文档列表为空，删除整个键
            _keys.RemoveAt(position);
            _documentIdLists.RemoveAt(position);
        }

        return removed;
    }

    /// <summary>
    /// 分裂节点
    /// </summary>
    /// <returns>分裂后的新节点</returns>
    public BTreeNode Split()
    {
        var mid = _keys.Count / 2;
        var newNode = new BTreeNode(MaxKeys, _isLeaf);

        if (_isLeaf)
        {
            // 叶子节点分裂
            newNode._keys.AddRange(_keys.GetRange(mid, _keys.Count - mid));
            newNode._documentIdLists.AddRange(_documentIdLists.GetRange(mid, _documentIdLists.Count - mid));

            _keys.RemoveRange(mid, _keys.Count - mid);
            _documentIdLists.RemoveRange(mid, _documentIdLists.Count - mid);
        }
        else
        {
            // 内部节点分裂
            newNode._keys.AddRange(_keys.GetRange(mid + 1, _keys.Count - mid - 1));
            newNode._children.AddRange(_children.GetRange(mid + 1, _children.Count - mid - 1));

            _keys.RemoveRange(mid, _keys.Count - mid);
            _children.RemoveRange(mid + 1, _children.Count - mid - 1);
        }

        return newNode;
    }

    /// <summary>
    /// 合并节点
    /// </summary>
    /// <param name="other">要合并的节点</param>
    /// <param name="separatorKey">分隔键</param>
    public void Merge(BTreeNode other, IndexKey separatorKey)
    {
        if (_isLeaf != other._isLeaf)
            throw new InvalidOperationException("Cannot merge nodes of different types");

        if (_isLeaf)
        {
            // 叶子节点合并
            _keys.AddRange(other._keys);
            _documentIdLists.AddRange(other._documentIdLists);
        }
        else
        {
            // 内部节点合并
            _keys.Add(separatorKey);
            _keys.AddRange(other._keys);
            _children.AddRange(other._children);
        }
    }

    /// <summary>
    /// 从兄弟节点借一个键
    /// </summary>
    /// <param name="sibling">兄弟节点</param>
    /// <param name="isLeftSibling">是否为左兄弟</param>
    /// <param name="separatorKey">分隔键</param>
    /// <returns>新的分隔键</returns>
    public IndexKey BorrowFromSibling(BTreeNode sibling, bool isLeftSibling, IndexKey separatorKey)
    {
        if (_isLeaf != sibling._isLeaf)
            throw new InvalidOperationException("Cannot borrow from sibling of different type");

        IndexKey newSeparatorKey;

        if (_isLeaf)
        {
            // 叶子节点借键
            if (isLeftSibling)
            {
                // 从右兄弟借第一个键
                var borrowedKey = sibling._keys[0];
                var borrowedDocs = sibling._documentIdLists[0];

                _keys.Insert(0, borrowedKey);
                _documentIdLists.Insert(0, borrowedDocs);

                sibling._keys.RemoveAt(0);
                sibling._documentIdLists.RemoveAt(0);

                newSeparatorKey = sibling._keys.Count > 0 ? sibling._keys[0] : separatorKey;
            }
            else
            {
                // 从左兄弟借第一个键（最接近当前节点的键）
                var borrowedKey = sibling._keys[0];
                var borrowedDocs = sibling._documentIdLists[0];

                _keys.Add(borrowedKey);
                _documentIdLists.Add(borrowedDocs);

                sibling._keys.RemoveAt(0);
                sibling._documentIdLists.RemoveAt(0);

                newSeparatorKey = borrowedKey;
            }
        }
        else
        {
            // 内部节点借键
            if (isLeftSibling)
            {
                // 从右兄弟借第一个键和子节点
                var borrowedKey = sibling._keys[0];
                var borrowedChild = sibling._children[0];

                _keys.Insert(0, separatorKey);
                _children.Insert(0, borrowedChild);

                sibling._keys.RemoveAt(0);
                sibling._children.RemoveAt(0);

                newSeparatorKey = sibling._keys.Count > 0 ? sibling._keys[0] : separatorKey;
            }
            else
            {
                // 从左兄弟借最后一个键和子节点
                var borrowedKey = sibling._keys[^1];
                var borrowedChild = sibling._children[^1];

                _keys.Add(separatorKey);
                _children.Add(borrowedChild);

                sibling._keys.RemoveAt(sibling._keys.Count - 1);
                sibling._children.RemoveAt(sibling._children.Count - 1);

                newSeparatorKey = borrowedKey;
            }
        }

        return newSeparatorKey;
    }

    /// <summary>
    /// 检查节点是否已满
    /// </summary>
    /// <returns>是否已满</returns>
    public bool IsFull() => _keys.Count >= MaxKeys;

    /// <summary>
    /// 检查节点是否需要合并
    /// </summary>
    /// <returns>是否需要合并</returns>
    public bool NeedsMerge() => _keys.Count < MinKeys;

    /// <summary>
    /// 获取节点的统计信息
    /// </summary>
    /// <returns>统计信息</returns>
    public BTreeNodeStatistics GetStatistics()
    {
        return new BTreeNodeStatistics
        {
            IsLeaf = _isLeaf,
            KeyCount = _keys.Count,
            ChildCount = _children.Count,
            DocumentCount = DocumentCount,
            MaxKeys = MaxKeys,
            MinKeys = MinKeys,
            IsFull = IsFull(),
            NeedsMerge = NeedsMerge()
        };
    }

    /// <summary>
    /// 转换为字符串表示
    /// </summary>
    /// <returns>字符串表示</returns>
    public override string ToString()
    {
        var type = _isLeaf ? "Leaf" : "Internal";
        return $"BTreeNode({type}): {_keys.Count} keys, {_children.Count} children, {DocumentCount} documents";
    }
}

/// <summary>
/// B+ 树节点统计信息
/// </summary>
public sealed class BTreeNodeStatistics
{
    public bool IsLeaf { get; init; }
    public int KeyCount { get; init; }
    public int ChildCount { get; init; }
    public int DocumentCount { get; init; }
    public int MaxKeys { get; init; }
    public int MinKeys { get; init; }
    public bool IsFull { get; init; }
    public bool NeedsMerge { get; init; }

    public override string ToString()
    {
        return $"Node({(IsLeaf ? "Leaf" : "Internal")}): {KeyCount}/{MaxKeys} keys, " +
               $"{ChildCount} children, {DocumentCount} docs";
    }
}