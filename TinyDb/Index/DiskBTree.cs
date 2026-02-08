using System;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;

namespace TinyDb.Index;

public sealed class DiskBTree : IDisposable
{
    private readonly PageManager _pm;
    private readonly uint _rootPageId;
    private readonly int _maxKeys;
    private bool _disposed;
    
    /// <summary>
    /// 获取根节点的页面 ID。
    /// </summary>
    public uint RootPageId => _rootPageId;
    
    /// <summary>
    /// 获取根节点实例。
    /// </summary>
    public DiskBTreeNode RootNode => LoadNode(_rootPageId);

    /// <summary>
    /// 获取树中条目的总数。
    /// </summary>
    public long EntryCount
    {
        get
        {
            var root = LoadNode(_rootPageId);
            return root.TreeEntryCount;
        }
    }

    /// <summary>
    /// 获取树中节点的总数。
    /// </summary>
    public int NodeCount => CountNodes(_rootPageId);

    /// <summary>
    /// 获取树的高度。
    /// </summary>
    public int Height
    {
        get
        {
            var node = LoadNode(_rootPageId);
            int height = 1;
            while (!node.IsLeaf)
            {
                if (node.ChildrenIds.Count == 0) break;
                node = LoadNode(node.ChildrenIds[0]);
                height++;
            }
            return height;
        }
    }

    private int CountNodes(uint pageId)
    {
        var node = LoadNode(pageId);
        int count = 1;
        if (!node.IsLeaf)
        {
            foreach (var childId in node.ChildrenIds)
            {
                count += CountNodes(childId);
            }
        }
        return count;
    }

    /// <summary>
    /// 初始化 <see cref="DiskBTree"/> 类的新实例。
    /// </summary>
    /// <param name="pm">页面管理器。</param>
    /// <param name="rootPageId">根页面 ID。</param>
    /// <param name="maxKeys">每个节点的最大键数。</param>
    public DiskBTree(PageManager pm, uint rootPageId, int maxKeys = 200)
    {
        _pm = pm;
        _rootPageId = rootPageId;
        _maxKeys = maxKeys > 0 ? maxKeys : 200;
    }

    /// <summary>
    /// 创建一个新的 B 树。
    /// </summary>
    /// <param name="pm">页面管理器。</param>
    /// <param name="maxKeys">每个节点的最大键数。</param>
    /// <returns>新的 B 树实例。</returns>
    public static DiskBTree Create(PageManager pm, int maxKeys = 200)
    {
        var rootPage = pm.NewPage(PageType.Index);
        var node = new DiskBTreeNode(rootPage, pm);
        node.InitAsRoot();
        node.Save(pm);
        return new DiskBTree(pm, rootPage.PageID, maxKeys);
    }

    /// <summary>
    /// 向树中插入一个键值对。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    public void Insert(IndexKey key, BsonValue value)
    {
        ThrowIfDisposed();
        var root = LoadNode(_rootPageId);
        if (root.IsFull((int)_pm.PageSize) || root.KeyCount >= _maxKeys)
        {
            var newChildPage = _pm.NewPage(PageType.Index);
            var newChild = new DiskBTreeNode(newChildPage, _pm);
            // Ensure garbage from recycled page is cleared
            newChild.Keys.Clear(); newChild.ChildrenIds.Clear(); newChild.Values.Clear();
            
            newChild.SetLeaf(root.IsLeaf);
            newChild.Keys.AddRange(root.Keys);
            newChild.ChildrenIds.AddRange(root.ChildrenIds);
            newChild.Values.AddRange(root.Values);
            newChild.SetNext(root.NextSiblingId); 
            newChild.Save(_pm);

            root = new DiskBTreeNode(_pm.GetPage(_rootPageId), _pm);
            long currentCount = root.TreeEntryCount;
            
            root.InitAsRoot(); 
            root.SetLeaf(false);
            root.SetTreeEntryCount(currentCount); 
            
            root.ChildrenIds.Add(newChild.PageId);
            root.Save(_pm);
            
            newChild.SetParent(root.PageId);
            newChild.Save(_pm);
            
            if (!newChild.IsLeaf)
            {
                foreach (var childId in newChild.ChildrenIds)
                {
                    var c = LoadNode(childId);
                    c.SetParent(newChild.PageId);
                    c.Save(_pm);
                }
            }
            
            SplitChild(root, 0, newChild);
            InsertNonFull(root, key, value);
        }
        else
        {
            InsertNonFull(root, key, value);
        }
        
        root = LoadNode(_rootPageId);
        root.SetTreeEntryCount(root.TreeEntryCount + 1);
        root.Save(_pm);
    }

    private void InsertNonFull(DiskBTreeNode node, IndexKey key, BsonValue value)
    {
        if (node.IsLeaf)
        {
            int pos = 0;
            while(pos < node.Keys.Count && node.Keys[pos].CompareTo(key) < 0) pos++;
            node.Keys.Insert(pos, key);
            node.Values.Insert(pos, value);
            node.MarkDirty();
            node.Save(_pm);
        }
        else
        {
            int i = node.Keys.Count - 1;
            while (i >= 0 && node.Keys[i].CompareTo(key) > 0) i--;
            i++;
            var child = LoadNode(node.ChildrenIds[i]);
            if (child.IsFull((int)_pm.PageSize) || child.KeyCount >= _maxKeys)
            {
                SplitChild(node, i, child);
                if (node.Keys[i].CompareTo(key) < 0) i++;
                child = LoadNode(node.ChildrenIds[i]); 
            }
            InsertNonFull(child, key, value);
        }
    }

    private void SplitChild(DiskBTreeNode parent, int index, DiskBTreeNode child)
    {
        var newNode = new DiskBTreeNode(_pm.NewPage(PageType.Index), _pm);
        // Ensure garbage from recycled page is cleared
        newNode.Keys.Clear(); newNode.ChildrenIds.Clear(); newNode.Values.Clear();

        newNode.SetLeaf(child.IsLeaf);
        newNode.SetParent(parent.PageId);
        
        int mid = child.KeyCount / 2;
        IndexKey upKey = child.Keys[mid];

        if (child.IsLeaf)
        {
            for(int j = mid; j < child.KeyCount; j++)
            {
                newNode.Keys.Add(child.Keys[j]);
                newNode.Values.Add(child.Values[j]);
            }
            int removeCount = child.KeyCount - mid;
            child.Keys.RemoveRange(mid, removeCount);
            child.Values.RemoveRange(mid, removeCount);
            child.MarkDirty();
            
            newNode.SetNext(child.NextSiblingId);
            newNode.SetPrev(child.PageId);
            child.SetNext(newNode.PageId);
            
            if (newNode.NextSiblingId != 0)
            {
                var nextSib = LoadNode(newNode.NextSiblingId);
                nextSib.SetPrev(newNode.PageId);
                nextSib.Save(_pm);
            }
        }
        else
        {
            for(int j = mid + 1; j < child.KeyCount; j++)
                newNode.Keys.Add(child.Keys[j]);
            for(int j = mid + 1; j < child.ChildrenIds.Count; j++)
                newNode.ChildrenIds.Add(child.ChildrenIds[j]);
            
            foreach(var movedChildId in newNode.ChildrenIds)
            {
                var movedChild = LoadNode(movedChildId);
                movedChild.SetParent(newNode.PageId);
                movedChild.Save(_pm);
            }

            child.Keys.RemoveRange(mid, child.KeyCount - mid);
            child.ChildrenIds.RemoveRange(mid + 1, child.ChildrenIds.Count - (mid + 1));
            child.MarkDirty();
        }
        newNode.Save(_pm);
        child.Save(_pm);
        
        parent.Keys.Insert(index, upKey);
        parent.ChildrenIds.Insert(index + 1, newNode.PageId);
        parent.MarkDirty();
        parent.Save(_pm);
    }

    /// <summary>
    /// 从树中删除一个键值对。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    /// <returns>如果找到并删除成功则为 true。</returns>
    public bool Delete(IndexKey key, BsonValue value)
    {
        var node = FindLeafNode(key);

        while (node.KeyCount > 0 && node.Keys[0].CompareTo(key) == 0 && node.PrevSiblingId != 0)
        {
             var prev = LoadNode(node.PrevSiblingId);
             if (prev.KeyCount > 0 && prev.Keys[prev.KeyCount - 1].CompareTo(key) == 0)
             {
                 node = prev;
             }
             else
             {
                 break; 
             }
        }
        
        bool deleted = false;
        DiskBTreeNode? deletionNode = null;

        while (node != null)
        {
            bool passed = false;
            for (int i = 0; i < node.Keys.Count; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0 && node.Values[i].Equals(value))
                {
                    node.Keys.RemoveAt(i);
                    node.Values.RemoveAt(i);
                    node.MarkDirty();
                    node.Save(_pm);
                    deleted = true;
                    deletionNode = node;
                    break;
                }
                else if (cmp > 0)
                {
                    passed = true;
                    break;
                }
            }
            
            if (deleted) break;
            if (passed) break;
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }
        
        if (deleted)
        {
            var root = LoadNode(_rootPageId);
            root.SetTreeEntryCount(root.TreeEntryCount - 1);
            root.Save(_pm);
            
            if (deletionNode != null && deletionNode.PageId != _rootPageId)
            {
                Rebalance(deletionNode);
            }
        }
        return deleted;
    }

    /// <summary>
    /// 查找与键关联的所有值。
    /// </summary>
    /// <param name="key">键。</param>
    /// <returns>值列表。</returns>
    public List<BsonValue> Find(IndexKey key)
    {
        var node = FindLeafNode(key);

        while (node.KeyCount > 0 && node.Keys[0].CompareTo(key) == 0 && node.PrevSiblingId != 0)
        {
             var prev = LoadNode(node.PrevSiblingId);
             if (prev.KeyCount > 0 && prev.Keys[prev.KeyCount - 1].CompareTo(key) == 0)
             {
                 node = prev;
             }
             else
             {
                 break; 
             }
        }

        var res = new List<BsonValue>();
        bool passed = false;

        while (node != null)
        {
            for(int i=0; i<node.KeyCount; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0)
                {
                    res.Add(node.Values[i]);
                }
                else if (cmp > 0)
                {
                    passed = true;
                    break;
                }
            }
            
            if (passed) break;
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }
        return res;
    }

    /// <summary>
    /// 查找键的单个值（第一个匹配项）。
    /// 优化版本：直接在叶子节点中查找，无需创建 List。
    /// </summary>
    /// <param name="key">键。</param>
    /// <returns>值，如果未找到则为 null。</returns>
    public BsonValue? FindExact(IndexKey key)
    {
        var node = FindLeafNode(key);

        // 查找精确匹配的第一个值
        for (int i = 0; i < node.KeyCount; i++)
        {
            int cmp = node.Keys[i].CompareTo(key);
            if (cmp == 0)
            {
                return node.Values[i];
            }
            if (cmp > 0)
            {
                break; // 已经超过目标键，不可能找到了
            }
        }
        return null;
    }
    
    /// <summary>
    /// 在键范围内查找值。
    /// </summary>
    /// <param name="startKey">开始键。</param>
    /// <param name="endKey">结束键。</param>
    /// <param name="includeStart">是否包含开始键。</param>
    /// <param name="includeEnd">是否包含结束键。</param>
    /// <returns>值的集合。</returns>
    public IEnumerable<BsonValue> FindRange(IndexKey startKey, IndexKey endKey, bool includeStart, bool includeEnd)
    {
        var node = FindLeafNode(startKey);

        while (node != null)
        {
            bool pastEnd = false;
            for (int i = 0; i < node.KeyCount; i++)
            {
                var key = node.Keys[i];
                int cmpStart = key.CompareTo(startKey);
                int cmpEnd = key.CompareTo(endKey);

                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    pastEnd = true;
                    break;
                }

                if (cmpStart > 0 || (cmpStart == 0 && includeStart))
                {
                    yield return node.Values[i];
                }
            }
            
            if (pastEnd) break;
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }
    }

    private DiskBTreeNode FindLeafNode(IndexKey key)
    {
        var node = LoadNode(_rootPageId);
        while (!node.IsLeaf)
        {
            int i = 0;
            while(i < node.KeyCount && key.CompareTo(node.Keys[i]) >= 0) i++;
            
            int childIdx = node.KeyCount; 
            for(int k=0; k<node.KeyCount; k++)
            {
                if (key.CompareTo(node.Keys[k]) < 0) 
                {
                    childIdx = k;
                    break;
                }
            }
            node = LoadNode(node.ChildrenIds[childIdx]);
        }
        return node;
    }

    /// <summary>
    /// 检查树是否包含某个键。
    /// </summary>
    public bool Contains(IndexKey key) => Find(key).Count > 0;

    /// <summary>
    /// 检查树是否包含特定的键值对。
    /// </summary>
    public bool Contains(IndexKey key, BsonValue value) => Find(key).Contains(value);

    /// <summary>
    /// 获取树中的所有值。
    /// </summary>
    public IEnumerable<BsonValue> GetAll()
    {
        var node = LoadNode(_rootPageId);
        while (!node.IsLeaf) node = LoadNode(node.ChildrenIds[0]); 
        while (node != null)
        {
            foreach (var val in node.Values) yield return val;
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }
    }

    /// <summary>
    /// 清空树。
    /// </summary>
    public void Clear()
    {
        var root = LoadNode(_rootPageId);
        root.InitAsRoot();
        root.Save(_pm);
    }

    private void Rebalance(DiskBTreeNode node)
    {
        if (node.PageId == _rootPageId)
        {
            if (!node.IsLeaf && node.KeyCount == 0 && node.ChildrenIds.Count > 0)
            {
                var child = LoadNode(node.ChildrenIds[0]);
                node.SetLeaf(child.IsLeaf);
                node.Keys.Clear(); node.Keys.AddRange(child.Keys);
                node.Values.Clear(); node.Values.AddRange(child.Values);
                node.ChildrenIds.Clear(); node.ChildrenIds.AddRange(child.ChildrenIds);
                node.SetNext(child.NextSiblingId);
                
                if (!node.IsLeaf)
                {
                    foreach(var gcId in node.ChildrenIds)
                    {
                        var gc = LoadNode(gcId);
                        gc.SetParent(node.PageId);
                        gc.Save(_pm);
                    }
                }
                
                node.MarkDirty();
                node.Save(_pm);
                _pm.FreePage(child.PageId);
            }
            return;
        }

        int minKeys = _maxKeys / 2;
        if (node.KeyCount >= minKeys) return;

        var parent = LoadNode(node.ParentId);
        int childIndex = -1;
        for(int i=0; i<parent.ChildrenIds.Count; i++)
        {
            if (parent.ChildrenIds[i] == node.PageId)
            {
                childIndex = i;
                break;
            }
        }
        
        if (childIndex == -1) return;

        if (childIndex > 0)
        {
            var leftSibling = LoadNode(parent.ChildrenIds[childIndex - 1]);
            if (leftSibling.KeyCount > minKeys)
            {
                BorrowFromLeft(node, leftSibling, parent, childIndex - 1);
                return;
            }
        }

        if (childIndex < parent.ChildrenIds.Count - 1)
        {
            var rightSibling = LoadNode(parent.ChildrenIds[childIndex + 1]);
            if (rightSibling.KeyCount > minKeys)
            {
                BorrowFromRight(node, rightSibling, parent, childIndex);
                return;
            }
        }

        int pageCapacity = (int)_pm.PageSize - 41;

        if (childIndex > 0)
        {
            var leftSibling = LoadNode(parent.ChildrenIds[childIndex - 1]);
            if (CanMerge(leftSibling, node, parent, childIndex - 1, pageCapacity))
            {
                MergeNodes(parent, leftSibling, node, childIndex - 1);
                Rebalance(parent);
                return;
            }
        }
        else if (childIndex < parent.ChildrenIds.Count - 1)
        {
            var rightSibling = LoadNode(parent.ChildrenIds[childIndex + 1]);
            if (CanMerge(node, rightSibling, parent, childIndex, pageCapacity))
            {
                MergeNodes(parent, node, rightSibling, childIndex);
                Rebalance(parent);
                return;
            }
        }
    }

    private bool CanMerge(DiskBTreeNode left, DiskBTreeNode right, DiskBTreeNode parent, int separatorIndex, int capacity)
    {
        // Estimate the size of the merged node
        // It will contain all keys/values from left, all from right, plus the separator from parent.
        int leftSize = left.CalculateSize();
        int rightSize = right.CalculateSize();
        
        // Rough estimate for the separator key size being added
        // Since we don't have easy access to BsonValue size here without the helper, 
        // we'll assume it's comparable to the average key size in the nodes or just safely check sum.
        // Actually, CalculateSize includes overhead. Merging removes one node overhead but adds separator.
        // Let's be conservative: sum of sizes should be less than capacity.
        // Note: CalculateSize() returns the byte size of the node content.
        
        return (leftSize + rightSize) <= capacity;
    }
    
    /// <summary>
    /// 验证树结构。
    /// </summary>
    /// <returns>如果有效则为 true。</returns>
    public bool Validate()
    {
        try 
        {
            var root = LoadNode(_rootPageId);
            return ValidateNode(root, null, null);
        }
        catch
        {
            return false;
        }
    }

    private bool ValidateNode(DiskBTreeNode node, IndexKey? minKey, IndexKey? maxKey)
    {
        // 1. Validate keys are sorted
        for (int i = 0; i < node.KeyCount - 1; i++)
        {
            if (node.Keys[i].CompareTo(node.Keys[i + 1]) >= 0) return false;
        }

        // 2. Validate keys are within range [minKey, maxKey]
        if (node.KeyCount > 0)
        {
            if (minKey != null && node.Keys[0].CompareTo(minKey) < 0) return false;
            if (maxKey != null && node.Keys[node.KeyCount - 1].CompareTo(maxKey) > 0) return false;
        }

        // 3. Recursively validate children
        if (!node.IsLeaf)
        {
            for (int i = 0; i < node.ChildrenIds.Count; i++)
            {
                var child = LoadNode(node.ChildrenIds[i]);
                if (child.ParentId != node.PageId) return false; // Parent pointer mismatch

                IndexKey? childMin = (i == 0) ? minKey : node.Keys[i - 1];
                IndexKey? childMax = (i == node.KeyCount) ? maxKey : node.Keys[i];

                if (!ValidateNode(child, childMin, childMax)) return false;
            }
        }

        return true;
    }

    private void BorrowFromLeft(DiskBTreeNode node, DiskBTreeNode leftSibling, DiskBTreeNode parent, int leftSiblingIndex)
    {
        if (node.IsLeaf)
        {
            var key = leftSibling.Keys[leftSibling.KeyCount - 1];
            var val = leftSibling.Values[leftSibling.Values.Count - 1];
            leftSibling.Keys.RemoveAt(leftSibling.KeyCount - 1);
            leftSibling.Values.RemoveAt(leftSibling.Values.Count - 1);
            
            node.Keys.Insert(0, key);
            node.Values.Insert(0, val);
            
            // 更新父节点的分隔符为当前节点新的第一个键
            parent.Keys[leftSiblingIndex] = node.Keys[0];
        }
        else
        {
            var key = leftSibling.Keys[leftSibling.KeyCount - 1];
            var childId = leftSibling.ChildrenIds[leftSibling.ChildrenIds.Count - 1];
            leftSibling.Keys.RemoveAt(leftSibling.KeyCount - 1);
            leftSibling.ChildrenIds.RemoveAt(leftSibling.ChildrenIds.Count - 1);
            
            // 旋转操作：父节点分隔符移入子节点，左兄弟最后一个键移入父节点
            var separator = parent.Keys[leftSiblingIndex];
            node.Keys.Insert(0, separator);
            node.ChildrenIds.Insert(0, childId);
            
            parent.Keys[leftSiblingIndex] = key;
            
            var child = LoadNode(childId);
            child.SetParent(node.PageId);
            child.Save(_pm);
        }
        
        leftSibling.MarkDirty(); leftSibling.Save(_pm);
        node.MarkDirty(); node.Save(_pm);
        parent.MarkDirty(); parent.Save(_pm);
    }

    private void BorrowFromRight(DiskBTreeNode node, DiskBTreeNode rightSibling, DiskBTreeNode parent, int nodeIndex)
    {
        if (node.IsLeaf)
        {
            var key = rightSibling.Keys[0];
            var val = rightSibling.Values[0];
            rightSibling.Keys.RemoveAt(0);
            rightSibling.Values.RemoveAt(0);
            
            node.Keys.Add(key);
            node.Values.Add(val);
            
            // 更新父节点的分隔符为右兄弟新的第一个键
            parent.Keys[nodeIndex] = rightSibling.Keys[0];
        }
        else
        {
            var key = rightSibling.Keys[0];
            var childId = rightSibling.ChildrenIds[0];
            rightSibling.Keys.RemoveAt(0);
            rightSibling.ChildrenIds.RemoveAt(0);
            
            // 旋转操作：父节点分隔符移入子节点，右兄弟第一个键移入父节点
            var separator = parent.Keys[nodeIndex];
            node.Keys.Add(separator);
            node.ChildrenIds.Add(childId);
            
            parent.Keys[nodeIndex] = key;
            
            var child = LoadNode(childId);
            child.SetParent(node.PageId);
            child.Save(_pm);
        }
        
        rightSibling.MarkDirty(); rightSibling.Save(_pm);
        node.MarkDirty(); node.Save(_pm);
        parent.MarkDirty(); parent.Save(_pm);
    }

    private void MergeNodes(DiskBTreeNode parent, DiskBTreeNode left, DiskBTreeNode right, int leftIndex)
    {
        if (left.IsLeaf)
        {
            left.Keys.AddRange(right.Keys);
            left.Values.AddRange(right.Values);
            
            left.SetNext(right.NextSiblingId);
            if (right.NextSiblingId != 0)
            {
                var next = LoadNode(right.NextSiblingId);
                next.SetPrev(left.PageId);
                next.Save(_pm);
            }
            
            parent.Keys.RemoveAt(leftIndex);
            parent.ChildrenIds.RemoveAt(leftIndex + 1);
        }
        else
        {
            var separator = parent.Keys[leftIndex];
            left.Keys.Add(separator);
            
            left.Keys.AddRange(right.Keys);
            left.ChildrenIds.AddRange(right.ChildrenIds);
            
            foreach(var childId in right.ChildrenIds)
            {
                var c = LoadNode(childId);
                c.SetParent(left.PageId);
                c.Save(_pm);
            }
            
            parent.Keys.RemoveAt(leftIndex);
            parent.ChildrenIds.RemoveAt(leftIndex + 1);
        }
        
        left.MarkDirty(); left.Save(_pm);
        _pm.FreePage(right.PageId);
        parent.MarkDirty(); parent.Save(_pm);
    }



    private DiskBTreeNode LoadNode(uint id)
    {
        var page = _pm.GetPage(id);
        if (page.CachedParsedData is DiskBTreeNode node)
        {
            return node;
        }
        node = new DiskBTreeNode(page, _pm);
        page.CachedParsedData = node;
        return node;
    }

    /// <summary>
    /// 释放 B 树。
    /// </summary>
    public void Dispose() { _disposed = true; }
    private void ThrowIfDisposed() { if (_disposed) throw new ObjectDisposedException(nameof(DiskBTree)); }
}
