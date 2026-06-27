using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
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
    private readonly ReaderWriterLockSlim _lock = new();
    private bool _disposed;

    [ThreadStatic]
    private static PageLease? _currentLease;

    public void EnterReadLock() => _lock.EnterReadLock();
    public void ExitReadLock() => _lock.ExitReadLock();
    public void EnterWriteLock() => _lock.EnterWriteLock();
    public void ExitWriteLock() => _lock.ExitWriteLock();
    
    /// <summary>
    /// 获取根节点的页面 ID。
    /// </summary>
    public uint RootPageId => _rootPageId;
    
    /// <summary>
    /// 获取根节点实例。
    /// </summary>
    public DiskBTreeNode RootNode
    {
        get
        {
            ThrowIfDisposed();
            using var lease = BeginPageLease();
            return LoadNode(_rootPageId);
        }
    }

    /// <summary>
    /// 获取树中条目的总数。
    /// </summary>
    public long EntryCount
    {
        get
        {
            ThrowIfDisposed();
            using var lease = BeginPageLease();
            var root = LoadNode(_rootPageId);
            return root.TreeEntryCount;
        }
    }

    /// <summary>
    /// 获取树中节点的总数。
    /// </summary>
    public int NodeCount
    {
        get
        {
            ThrowIfDisposed();
            using var lease = BeginPageLease();
            return CountNodes(_rootPageId);
        }
    }

    /// <summary>
    /// 获取树的高度。
    /// </summary>
    public int Height
    {
        get
        {
            ThrowIfDisposed();
            using var lease = BeginPageLease();
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

    private static PageLease BeginPageLease()
    {
        var lease = new PageLease(_currentLease);
        _currentLease = lease;
        return lease;
    }

    private Page GetPage(uint pageId)
    {
        return _currentLease?.GetPage(_pm, pageId) ?? _pm.GetPage(pageId);
    }

    private Page NewIndexPage()
    {
        return _currentLease?.NewPage(_pm, PageType.Index) ?? _pm.NewPage(PageType.Index);
    }

    private void FreePage(uint pageId)
    {
        _currentLease?.ReleasePage(pageId);
        _pm.FreePage(pageId);
    }

    private sealed class PageLease : IDisposable
    {
        private readonly PageLease? _previous;
        private readonly Dictionary<uint, Page> _pagesById = new();
        private readonly List<Page> _pages = new();
        private bool _disposed;

        public PageLease(PageLease? previous)
        {
            _previous = previous;
        }

        public Page GetPage(PageManager pageManager, uint pageId)
        {
            if (_pagesById.TryGetValue(pageId, out var page))
            {
                return page;
            }

            page = pageManager.GetPagePinned(pageId);
            return AddPinnedPage(page);
        }

        public Page NewPage(PageManager pageManager, PageType pageType)
        {
            var page = pageManager.NewPagePinned(pageType);
            return AddPinnedPage(page);
        }

        private Page AddPinnedPage(Page page)
        {
            if (_disposed)
            {
                page.Unpin();
                throw new ObjectDisposedException(nameof(PageLease));
            }

            if (_pagesById.TryAdd(page.PageID, page))
            {
                _pages.Add(page);
                return page;
            }

            page.Unpin();
            return _pagesById[page.PageID];
        }

        public void ReleasePage(uint pageId)
        {
            if (_disposed) return;

            if (_pagesById.Remove(pageId, out var page))
            {
                for (int i = _pages.Count - 1; i >= 0; i--)
                {
                    if (_pages[i].PageID == pageId)
                    {
                        _pages.RemoveAt(i);
                        break;
                    }
                }

                page.Unpin();
            }

            _previous?.ReleasePage(pageId);
        }

        public void Dispose()
        {
            if (_disposed) return;

            for (int i = _pages.Count - 1; i >= 0; i--)
            {
                _pages[i].Unpin();
            }

            _currentLease = _previous;
            _disposed = true;
        }
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
        using var lease = BeginPageLease();
        var root = LoadNode(_rootPageId);
        if (root.IsFull((int)_pm.PageSize) || root.KeyCount >= _maxKeys)
        {
            var newChildPage = NewIndexPage();
            var newChild = new DiskBTreeNode(newChildPage, _pm);
            // Ensure garbage from recycled page is cleared
            newChild.Keys.Clear(); newChild.ChildrenIds.Clear(); newChild.Values.Clear();
            
            newChild.SetLeaf(root.IsLeaf);
            newChild.Keys.AddRange(root.Keys);
            newChild.ChildrenIds.AddRange(root.ChildrenIds);
            newChild.Values.AddRange(root.Values);
            newChild.SetNext(root.NextSiblingId); 
            newChild.Save(_pm);

            root = new DiskBTreeNode(GetPage(_rootPageId), _pm);
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
            int pos = LowerBound(node.Keys, key);
            node.Keys.Insert(pos, key);
            node.Values.Insert(pos, value);
            node.MarkDirty();
            node.Save(_pm);
        }
        else
        {
            int i = UpperBound(node.Keys, key);
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

    private static int LowerBound(List<IndexKey> keys, IndexKey key)
    {
        int low = 0;
        int high = keys.Count;

        while (low < high)
        {
            int mid = low + ((high - low) >> 1);
            if (keys[mid].CompareTo(key) < 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        return low;
    }

    private static int UpperBound(List<IndexKey> keys, IndexKey key)
    {
        int low = 0;
        int high = keys.Count;

        while (low < high)
        {
            int mid = low + ((high - low) >> 1);
            if (keys[mid].CompareTo(key) <= 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        return low;
    }

    private void SplitChild(DiskBTreeNode parent, int index, DiskBTreeNode child)
    {
        var newNode = new DiskBTreeNode(NewIndexPage(), _pm);
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
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(key);
        
        bool deleted = false;
        DiskBTreeNode? deletionNode = null;

        while (node != null)
        {
            bool passed = false;
            for (int i = 0; i < node.Keys.Count; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0 && ValuesEqual(node.Values[i], value))
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
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(key);

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
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(key);

        // 查找精确匹配的第一个值
        bool passed = false;
        for (int i = 0; i < node.KeyCount; i++)
        {
            int cmp = node.Keys[i].CompareTo(key);
            if (cmp == 0)
            {
                return node.Values[i];
            }
            if (cmp > 0)
            {
                passed = true;
                break; // 已经超过目标键，不可能找到了
            }
        }
        if (passed) return null;

        while (node.NextSiblingId != 0)
        {
            node = LoadNode(node.NextSiblingId);
            for (int i = 0; i < node.KeyCount; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0)
                {
                    return node.Values[i];
                }

                if (cmp > 0)
                {
                    return null;
                }
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
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(startKey);

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

    /// <summary>
    /// 在键范围内查找值（按降序返回）。
    /// </summary>
    /// <param name="startKey">开始键。</param>
    /// <param name="endKey">结束键。</param>
    /// <param name="includeStart">是否包含开始键。</param>
    /// <param name="includeEnd">是否包含结束键。</param>
    /// <returns>值的集合（降序）。</returns>
    public IEnumerable<BsonValue> FindRangeReverse(IndexKey startKey, IndexKey endKey, bool includeStart, bool includeEnd)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindLeafNode(endKey);

        while (node != null)
        {
            bool pastStart = false;

            for (int i = node.KeyCount - 1; i >= 0; i--)
            {
                var key = node.Keys[i];
                int cmpStart = key.CompareTo(startKey);
                int cmpEnd = key.CompareTo(endKey);

                if (cmpStart < 0 || (cmpStart == 0 && !includeStart))
                {
                    pastStart = true;
                    break;
                }

                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    continue;
                }

                yield return node.Values[i];
            }

            if (pastStart) break;
            if (node.PrevSiblingId == 0) break;
            node = LoadNode(node.PrevSiblingId);
        }
    }

    private DiskBTreeNode FindLeafNode(IndexKey key)
    {
        var node = LoadNode(_rootPageId);
        while (!node.IsLeaf)
        {
            int childIdx = UpperBound(node.Keys, key);
            node = LoadNode(node.ChildrenIds[childIdx]);
        }
        return node;
    }

    private DiskBTreeNode FindFirstCandidateLeafNode(IndexKey key)
    {
        var node = FindLeafNode(key);

        while (node.PrevSiblingId != 0)
        {
            var prev = LoadNode(node.PrevSiblingId);
            if (prev.KeyCount == 0 || prev.Keys[prev.KeyCount - 1].CompareTo(key) < 0)
            {
                break;
            }

            node = prev;
        }

        return node;
    }

    /// <summary>
    /// 检查树是否包含某个键。
    /// </summary>
    public bool Contains(IndexKey key)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(key);

        while (node != null)
        {
            for (int i = 0; i < node.KeyCount; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0) return true;
                if (cmp > 0) return false;
            }

            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }

        return false;
    }

    /// <summary>
    /// 检查树是否包含特定的键值对。
    /// </summary>
    public bool Contains(IndexKey key, BsonValue value)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(key);

        while (node != null)
        {
            for (int i = 0; i < node.KeyCount; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0 && ValuesEqual(node.Values[i], value)) return true;
                if (cmp > 0) return false;
            }

            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }

        return false;
    }

    /// <summary>
    /// 获取树中的所有值。
    /// </summary>
    public IEnumerable<BsonValue> GetAll()
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
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
    /// 获取树中的所有值（按降序返回）。
    /// </summary>
    public IEnumerable<BsonValue> GetAllReverse()
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = LoadNode(_rootPageId);
        while (!node.IsLeaf) node = LoadNode(node.ChildrenIds[^1]);

        while (node != null)
        {
            for (int i = node.Values.Count - 1; i >= 0; i--)
            {
                yield return node.Values[i];
            }

            if (node.PrevSiblingId == 0) break;
            node = LoadNode(node.PrevSiblingId);
        }
    }

    /// <summary>
    /// 清空树。
    /// </summary>
    public void Clear()
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var root = LoadNode(_rootPageId);
        var visited = new HashSet<uint>();

        foreach (var childId in root.ChildrenIds.ToArray())
        {
            FreeSubtree(childId, visited);
        }

        root.InitAsRoot();
        root.Save(_pm);
    }

    internal void DropPages()
    {
        if (_disposed) return;

        using var lease = BeginPageLease();
        var visited = new HashSet<uint>();
        FreeSubtree(_rootPageId, visited);
        _disposed = true;
        _lock.Dispose();
    }

    private void FreeSubtree(uint pageId, HashSet<uint> visited)
    {
        if (pageId == 0 || !visited.Add(pageId)) return;

        Page? page = null;
        DiskBTreeNode? node = null;
        uint overflowPageId = 0;

        try
        {
            page = GetPage(pageId);
            overflowPageId = page.Header.NextPageID;

            if (page.PageType == PageType.Index && page.Header.ItemCount > 0)
            {
                node = LoadNode(pageId);
            }
        }
        catch
        {
            // 损坏的索引页仍应释放；具体损坏会在校验或读取路径暴露。
        }

        if (node != null && !node.IsLeaf)
        {
            foreach (var childId in node.ChildrenIds.ToArray())
            {
                FreeSubtree(childId, visited);
            }
        }

        FreeOverflowChain(overflowPageId, visited);
        FreePage(pageId);
    }

    private void FreeOverflowChain(uint pageId, HashSet<uint> visited)
    {
        while (pageId != 0 && visited.Add(pageId))
        {
            var page = GetPage(pageId);
            var nextPageId = page.Header.NextPageID;
            FreePage(pageId);
            pageId = nextPageId;
        }
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
                FreePage(child.PageId);
            }
            return;
        }

        int minKeys = Math.Max(1, _maxKeys / 2);
        if (node.KeyCount >= minKeys) return;

        if (!TryResolveParent(node, out var parent, out var childIndex))
            throw new InvalidDataException($"Unable to locate parent for B+Tree node {node.PageId}.");

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

    private bool TryResolveParent(DiskBTreeNode node, [NotNullWhen(true)] out DiskBTreeNode? parent, out int childIndex)
    {
        parent = null;
        childIndex = -1;

        if (node.ParentId != 0)
        {
            var candidate = LoadNode(node.ParentId);
            childIndex = candidate.ChildrenIds.IndexOf(node.PageId);
            if (childIndex >= 0)
            {
                parent = candidate;
                return true;
            }
        }

        if (TryFindParent(_rootPageId, node.PageId, new HashSet<uint>(), depth: 0, out parent, out childIndex))
        {
            node.SetParent(parent.PageId);
            node.Save(_pm);
            return true;
        }

        return false;
    }

    private bool TryFindParent(
        uint currentPageId,
        uint childPageId,
        HashSet<uint> visited,
        int depth,
        [NotNullWhen(true)] out DiskBTreeNode? parent,
        out int childIndex)
    {
        parent = null;
        childIndex = -1;
        if (depth > _maxKeys + 64 || !visited.Add(currentPageId))
        {
            return false;
        }

        var current = LoadNode(currentPageId);
        if (current.IsLeaf) return false;

        childIndex = current.ChildrenIds.IndexOf(childPageId);
        if (childIndex >= 0)
        {
            parent = current;
            return true;
        }

        foreach (var nextChildId in current.ChildrenIds)
        {
            if (TryFindParent(nextChildId, childPageId, visited, depth + 1, out parent, out childIndex))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ValuesEqual(BsonValue left, BsonValue right)
    {
        return BsonValueComparer.Compare(left, right) == 0;
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
        
        int separatorSize = left.IsLeaf ? 0 : EstimateIndexKeySize(parent.Keys[separatorIndex]);
        return leftSize + rightSize + separatorSize <= capacity;
    }

    private static int EstimateIndexKeySize(IndexKey key)
    {
        int size = 4;
        var values = key.ValuesSpan;
        for (int i = 0; i < values.Length; i++)
        {
            size += 1 + EstimateBsonValueSize(values[i]);
        }

        return size;
    }

    private static int EstimateBsonValueSize(BsonValue value)
    {
        if (value == null || value.IsNull) return 0;

        return value.BsonType switch
        {
            BsonType.Double => 8,
            BsonType.String => ((BsonString)value).Value.Length * 2 + 4,
            BsonType.Int32 => 4,
            BsonType.Int64 => 8,
            BsonType.Boolean => 1,
            BsonType.DateTime => 8,
            BsonType.ObjectId => 12,
            BsonType.Decimal128 => 16,
            _ => 20
        };
    }
    
    /// <summary>
    /// 验证树结构。
    /// </summary>
    /// <returns>如果有效则为 true。</returns>
    public bool Validate()
    {
        try 
        {
            ThrowIfDisposed();
            using var lease = BeginPageLease();
            var root = LoadNode(_rootPageId);
            return ValidateNode(root, null, null);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
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
            parent.Keys[nodeIndex] = rightSibling.KeyCount > 0 ? rightSibling.Keys[0] : key;
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
        FreePage(right.PageId);
        parent.MarkDirty(); parent.Save(_pm);
    }



    private DiskBTreeNode LoadNode(uint id)
    {
        var page = GetPage(id);
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
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _lock.Dispose();
    }
    private void ThrowIfDisposed() { if (_disposed) throw new ObjectDisposedException(nameof(DiskBTree)); }
}
