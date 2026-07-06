using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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

    [ThreadStatic]
    private static PageLease? _currentLease;

    internal sealed class IndexScanBatch
    {
        public IndexScanBatch(
            List<BsonValue> values,
            IndexKey? lastKey,
            BsonValue? lastValue,
            uint lastPageId,
            int lastIndex,
            bool hasMore)
        {
            Values = values;
            LastKey = lastKey;
            LastValue = lastValue;
            LastPageId = lastPageId;
            LastIndex = lastIndex;
            HasMore = hasMore;
        }

        public List<BsonValue> Values { get; }
        public IndexKey? LastKey { get; }
        public BsonValue? LastValue { get; }
        public uint LastPageId { get; }
        public int LastIndex { get; }
        public bool HasMore { get; }
    }
    
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
        private PageLease? _previous;
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

            RemoveFromCurrentChain(this);
            _disposed = true;
        }

        private static void RemoveFromCurrentChain(PageLease lease)
        {
            if (_currentLease == lease)
            {
                _currentLease = lease._previous;
                lease._previous = null;
                return;
            }

            var current = _currentLease;
            while (current != null)
            {
                if (current._previous == lease)
                {
                    current._previous = lease._previous;
                    lease._previous = null;
                    return;
                }

                current = current._previous;
            }

            lease._previous = null;
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
            if (root.IsLeaf)
            {
                int pos = LowerBound(root.Keys, key);
                root.Keys.Insert(pos, key);
                root.Values.Insert(pos, value);
                root.SetTreeEntryCount(root.TreeEntryCount + 1);
                root.MarkDirty();
                root.Save(_pm);
                return;
            }

            InsertNonFull(root, key, value);
        }
        
        root.SetTreeEntryCount(root.TreeEntryCount + 1);
        root.Save(_pm);
    }

    public bool TryInsertUnique(IndexKey key, BsonValue value)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();

        var path = new List<(DiskBTreeNode Parent, int ChildIndex)>();
        var root = LoadNode(_rootPageId);
        var node = root;
        while (!node.IsLeaf)
        {
            int childIndex = UpperBound(node.Keys, key);
            path.Add((node, childIndex));
            node = LoadNode(node.ChildrenIds[childIndex]);
        }

        int position = LowerBound(node.Keys, key);
        if (position < node.KeyCount && node.Keys[position].CompareTo(key) == 0)
        {
            return false;
        }

        node.Keys.Insert(position, key);
        node.Values.Insert(position, value);
        node.MarkDirty();

        var shouldSplit = ShouldSplit(node);
        if (node.PageId == _rootPageId && !shouldSplit)
        {
            node.SetTreeEntryCount(node.TreeEntryCount + 1);
            node.Save(_pm);
            return true;
        }

        if (shouldSplit)
        {
            SplitOverflowNode(node, path);
        }
        else
        {
            node.Save(_pm);
        }

        var rootForCount = shouldSplit ? LoadNode(_rootPageId) : root;
        rootForCount.SetTreeEntryCount(rootForCount.TreeEntryCount + 1);
        rootForCount.Save(_pm);
        return true;
    }

    private bool ShouldSplit(DiskBTreeNode node)
    {
        return node.IsFull((int)_pm.PageSize) || node.KeyCount > _maxKeys;
    }

    private void SplitOverflowNode(DiskBTreeNode node, List<(DiskBTreeNode Parent, int ChildIndex)> path)
    {
        while (ShouldSplit(node))
        {
            if (node.PageId == _rootPageId || path.Count == 0)
            {
                SplitRoot(node);
                return;
            }

            var parentRef = path[^1];
            path.RemoveAt(path.Count - 1);
            SplitChild(parentRef.Parent, parentRef.ChildIndex, node);
            node = parentRef.Parent;
        }
    }

    private void SplitRoot(DiskBTreeNode root)
    {
        var newChildPage = NewIndexPage();
        var newChild = new DiskBTreeNode(newChildPage, _pm);
        newChild.Keys.Clear();
        newChild.ChildrenIds.Clear();
        newChild.Values.Clear();

        newChild.SetLeaf(root.IsLeaf);
        newChild.Keys.AddRange(root.Keys);
        newChild.ChildrenIds.AddRange(root.ChildrenIds);
        newChild.Values.AddRange(root.Values);
        newChild.SetNext(root.NextSiblingId);
        newChild.SetParent(root.PageId);

        if (!newChild.IsLeaf)
        {
            foreach (var childId in newChild.ChildrenIds)
            {
                var child = LoadNode(childId);
                child.SetParent(newChild.PageId);
                child.Save(_pm);
            }
        }

        long currentCount = root.TreeEntryCount;
        root.InitAsRoot();
        root.SetLeaf(false);
        root.SetTreeEntryCount(currentCount);
        root.ChildrenIds.Add(newChild.PageId);
        root.MarkDirty();
        root.Save(_pm);

        SplitChild(root, 0, newChild);
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

    internal async Task<BsonValue?> FindExactAsync(IndexKey key, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var node = await FindFirstCandidateLeafNodeAsync(key, cancellationToken).ConfigureAwait(false);

        bool passed = false;
        for (int i = 0; i < node.KeyCount; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int cmp = node.Keys[i].CompareTo(key);
            if (cmp == 0)
            {
                return node.Values[i];
            }
            if (cmp > 0)
            {
                passed = true;
                break;
            }
        }
        if (passed) return null;

        while (node.NextSiblingId != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            node = await LoadNodeAsync(node.NextSiblingId, cancellationToken).ConfigureAwait(false);
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
        var results = new List<BsonValue>();
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
                    results.Add(node.Values[i]);
                }
            }
            
            if (pastEnd) break;
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }

        return results;
    }

    internal IndexScanBatch FindRangeBatch(
        IndexKey startKey,
        IndexKey endKey,
        bool includeStart,
        bool includeEnd,
        IndexKey? continuationKey,
        BsonValue? continuationValue,
        uint continuationPageId,
        int continuationIndex,
        int batchSize)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));

        ThrowIfDisposed();
        var results = new List<BsonValue>(batchSize);
        IndexKey? lastKey = null;
        BsonValue? lastValue = null;
        uint lastPageId = 0;
        int lastIndex = -1;

        using var lease = BeginPageLease();
        DiskBTreeNode node;
        var startIndex = 0;
        var waitingForContinuation = continuationKey != null;
        if (TryResumeFromLeafPosition(continuationPageId, continuationIndex, continuationKey, continuationValue, forward: true, out var resumedNode, out startIndex))
        {
            node = resumedNode;
            waitingForContinuation = false;
        }
        else
        {
            node = FindFirstCandidateLeafNode(continuationKey ?? startKey);
        }

        while (node != null)
        {
            if (startIndex >= node.KeyCount)
            {
                if (node.NextSiblingId == 0) break;
                node = LoadNode(node.NextSiblingId);
                startIndex = 0;
                continue;
            }

            bool pastEnd = false;
            for (int i = startIndex; i < node.KeyCount; i++)
            {
                var key = node.Keys[i];
                var value = node.Values[i];
                int cmpEnd = key.CompareTo(endKey);

                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    pastEnd = true;
                    break;
                }

                if (waitingForContinuation)
                {
                    var cmpContinuation = key.CompareTo(continuationKey!);
                    if (cmpContinuation < 0)
                    {
                        continue;
                    }

                    if (cmpContinuation == 0)
                    {
                        if (continuationValue != null && ValuesEqual(value, continuationValue))
                        {
                            waitingForContinuation = false;
                        }

                        continue;
                    }

                    waitingForContinuation = false;
                }

                int cmpStart = key.CompareTo(startKey);
                if (cmpStart > 0 || (cmpStart == 0 && includeStart))
                {
                    results.Add(value);
                    lastKey = key;
                    lastValue = value;
                    lastPageId = node.PageId;
                    lastIndex = i;

                    if (results.Count >= batchSize)
                    {
                        return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: true);
                    }
                }
            }

            if (pastEnd) break;
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
            startIndex = 0;
        }

        return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: false);
    }

    internal async Task<IndexScanBatch> FindRangeBatchAsync(
        IndexKey startKey,
        IndexKey endKey,
        bool includeStart,
        bool includeEnd,
        IndexKey? continuationKey,
        BsonValue? continuationValue,
        uint continuationPageId,
        int continuationIndex,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));

        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var results = new List<BsonValue>(batchSize);
        IndexKey? lastKey = null;
        BsonValue? lastValue = null;
        uint lastPageId = 0;
        int lastIndex = -1;

        DiskBTreeNode node;
        var startIndex = 0;
        var waitingForContinuation = continuationKey != null;
        var resume = await TryResumeFromLeafPositionAsync(
            continuationPageId,
            continuationIndex,
            continuationKey,
            continuationValue,
            forward: true,
            cancellationToken).ConfigureAwait(false);

        if (resume.Node != null)
        {
            node = resume.Node;
            startIndex = resume.StartIndex;
            waitingForContinuation = false;
        }
        else
        {
            node = await FindFirstCandidateLeafNodeAsync(continuationKey ?? startKey, cancellationToken).ConfigureAwait(false);
        }

        while (node != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (startIndex >= node.KeyCount)
            {
                if (node.NextSiblingId == 0) break;
                node = await LoadNodeAsync(node.NextSiblingId, cancellationToken).ConfigureAwait(false);
                startIndex = 0;
                continue;
            }

            bool pastEnd = false;
            for (int i = startIndex; i < node.KeyCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var key = node.Keys[i];
                var value = node.Values[i];
                int cmpEnd = key.CompareTo(endKey);

                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    pastEnd = true;
                    break;
                }

                if (waitingForContinuation)
                {
                    var cmpContinuation = key.CompareTo(continuationKey!);
                    if (cmpContinuation < 0)
                    {
                        continue;
                    }

                    if (cmpContinuation == 0)
                    {
                        if (continuationValue != null && ValuesEqual(value, continuationValue))
                        {
                            waitingForContinuation = false;
                        }

                        continue;
                    }

                    waitingForContinuation = false;
                }

                int cmpStart = key.CompareTo(startKey);
                if (cmpStart > 0 || (cmpStart == 0 && includeStart))
                {
                    results.Add(value);
                    lastKey = key;
                    lastValue = value;
                    lastPageId = node.PageId;
                    lastIndex = i;

                    if (results.Count >= batchSize)
                    {
                        return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: true);
                    }
                }
            }

            if (pastEnd) break;
            if (node.NextSiblingId == 0) break;
            node = await LoadNodeAsync(node.NextSiblingId, cancellationToken).ConfigureAwait(false);
            startIndex = 0;
        }

        return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: false);
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
        var results = new List<BsonValue>();
        using var lease = BeginPageLease();
        var node = FindLastCandidateLeafNode(endKey);

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

                results.Add(node.Values[i]);
            }

            if (pastStart) break;
            if (node.PrevSiblingId == 0) break;
            node = LoadNode(node.PrevSiblingId);
        }

        return results;
    }

    internal IndexScanBatch FindRangeReverseBatch(
        IndexKey startKey,
        IndexKey endKey,
        bool includeStart,
        bool includeEnd,
        IndexKey? continuationKey,
        BsonValue? continuationValue,
        uint continuationPageId,
        int continuationIndex,
        int batchSize)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));

        ThrowIfDisposed();
        var results = new List<BsonValue>(batchSize);
        IndexKey? lastKey = null;
        BsonValue? lastValue = null;
        uint lastPageId = 0;
        int lastIndex = -1;

        using var lease = BeginPageLease();
        DiskBTreeNode node;
        var startIndex = -1;
        var waitingForContinuation = continuationKey != null;
        if (TryResumeFromLeafPosition(continuationPageId, continuationIndex, continuationKey, continuationValue, forward: false, out var resumedNode, out startIndex))
        {
            node = resumedNode;
            waitingForContinuation = false;
        }
        else
        {
            node = continuationKey != null
                ? FindLastCandidateLeafNode(continuationKey)
                : FindLastCandidateLeafNode(endKey);
            startIndex = node.KeyCount - 1;
        }

        while (node != null)
        {
            if (startIndex < 0)
            {
                if (node.PrevSiblingId == 0) break;
                node = LoadNode(node.PrevSiblingId);
                startIndex = node.KeyCount - 1;
                continue;
            }

            bool pastStart = false;

            for (int i = startIndex; i >= 0; i--)
            {
                var key = node.Keys[i];
                var value = node.Values[i];
                int cmpStart = key.CompareTo(startKey);

                if (cmpStart < 0 || (cmpStart == 0 && !includeStart))
                {
                    pastStart = true;
                    break;
                }

                if (waitingForContinuation)
                {
                    var cmpContinuation = key.CompareTo(continuationKey!);
                    if (cmpContinuation > 0)
                    {
                        continue;
                    }

                    if (cmpContinuation == 0)
                    {
                        if (continuationValue != null && ValuesEqual(value, continuationValue))
                        {
                            waitingForContinuation = false;
                        }

                        continue;
                    }

                    waitingForContinuation = false;
                }

                int cmpEnd = key.CompareTo(endKey);
                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    continue;
                }

                results.Add(value);
                lastKey = key;
                lastValue = value;
                lastPageId = node.PageId;
                lastIndex = i;

                if (results.Count >= batchSize)
                {
                    return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: true);
                }
            }

            if (pastStart) break;
            if (node.PrevSiblingId == 0) break;
            node = LoadNode(node.PrevSiblingId);
            startIndex = node.KeyCount - 1;
        }

        return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: false);
    }

    internal async Task<IndexScanBatch> FindRangeReverseBatchAsync(
        IndexKey startKey,
        IndexKey endKey,
        bool includeStart,
        bool includeEnd,
        IndexKey? continuationKey,
        BsonValue? continuationValue,
        uint continuationPageId,
        int continuationIndex,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));

        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var results = new List<BsonValue>(batchSize);
        IndexKey? lastKey = null;
        BsonValue? lastValue = null;
        uint lastPageId = 0;
        int lastIndex = -1;

        DiskBTreeNode node;
        var startIndex = -1;
        var waitingForContinuation = continuationKey != null;
        var resume = await TryResumeFromLeafPositionAsync(
            continuationPageId,
            continuationIndex,
            continuationKey,
            continuationValue,
            forward: false,
            cancellationToken).ConfigureAwait(false);

        if (resume.Node != null)
        {
            node = resume.Node;
            startIndex = resume.StartIndex;
            waitingForContinuation = false;
        }
        else
        {
            node = continuationKey != null
                ? await FindLastCandidateLeafNodeAsync(continuationKey, cancellationToken).ConfigureAwait(false)
                : await FindLastCandidateLeafNodeAsync(endKey, cancellationToken).ConfigureAwait(false);
            startIndex = node.KeyCount - 1;
        }

        while (node != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (startIndex < 0)
            {
                if (node.PrevSiblingId == 0) break;
                node = await LoadNodeAsync(node.PrevSiblingId, cancellationToken).ConfigureAwait(false);
                startIndex = node.KeyCount - 1;
                continue;
            }

            bool pastStart = false;

            for (int i = startIndex; i >= 0; i--)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var key = node.Keys[i];
                var value = node.Values[i];
                int cmpStart = key.CompareTo(startKey);

                if (cmpStart < 0 || (cmpStart == 0 && !includeStart))
                {
                    pastStart = true;
                    break;
                }

                if (waitingForContinuation)
                {
                    var cmpContinuation = key.CompareTo(continuationKey!);
                    if (cmpContinuation > 0)
                    {
                        continue;
                    }

                    if (cmpContinuation == 0)
                    {
                        if (continuationValue != null && ValuesEqual(value, continuationValue))
                        {
                            waitingForContinuation = false;
                        }

                        continue;
                    }

                    waitingForContinuation = false;
                }

                int cmpEnd = key.CompareTo(endKey);
                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    continue;
                }

                results.Add(value);
                lastKey = key;
                lastValue = value;
                lastPageId = node.PageId;
                lastIndex = i;

                if (results.Count >= batchSize)
                {
                    return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: true);
                }
            }

            if (pastStart) break;
            if (node.PrevSiblingId == 0) break;
            node = await LoadNodeAsync(node.PrevSiblingId, cancellationToken).ConfigureAwait(false);
            startIndex = node.KeyCount - 1;
        }

        return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: false);
    }

    private bool TryResumeFromLeafPosition(
        uint pageId,
        int index,
        IndexKey? key,
        BsonValue? value,
        bool forward,
        [NotNullWhen(true)] out DiskBTreeNode? node,
        out int startIndex)
    {
        node = null;
        startIndex = forward ? 0 : -1;

        if (pageId == 0 || index < 0 || key == null || value == null)
        {
            return false;
        }

        try
        {
            var candidate = LoadNode(pageId);
            if (!candidate.IsLeaf ||
                index >= candidate.KeyCount ||
                candidate.Keys[index].CompareTo(key) != 0 ||
                !ValuesEqual(candidate.Values[index], value))
            {
                return false;
            }

            node = candidate;
            startIndex = forward ? index + 1 : index - 1;
            return true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return false;
        }
    }

    private async Task<(DiskBTreeNode? Node, int StartIndex)> TryResumeFromLeafPositionAsync(
        uint pageId,
        int index,
        IndexKey? key,
        BsonValue? value,
        bool forward,
        CancellationToken cancellationToken)
    {
        var startIndex = forward ? 0 : -1;

        if (pageId == 0 || index < 0 || key == null || value == null)
        {
            return (null, startIndex);
        }

        try
        {
            var candidate = await LoadNodeAsync(pageId, cancellationToken).ConfigureAwait(false);
            if (!candidate.IsLeaf ||
                index >= candidate.KeyCount ||
                candidate.Keys[index].CompareTo(key) != 0 ||
                !ValuesEqual(candidate.Values[index], value))
            {
                return (null, startIndex);
            }

            return (candidate, forward ? index + 1 : index - 1);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not OperationCanceledException)
        {
            return (null, startIndex);
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

    private DiskBTreeNode FindLastCandidateLeafNode(IndexKey key)
    {
        var node = FindLeafNode(key);

        while (node.NextSiblingId != 0)
        {
            var next = LoadNode(node.NextSiblingId);
            if (next.KeyCount == 0 || next.Keys[0].CompareTo(key) > 0)
            {
                break;
            }

            node = next;
        }

        return node;
    }

    private async Task<DiskBTreeNode> FindLeafNodeAsync(IndexKey key, CancellationToken cancellationToken)
    {
        var node = await LoadNodeAsync(_rootPageId, cancellationToken).ConfigureAwait(false);
        while (!node.IsLeaf)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int childIdx = UpperBound(node.Keys, key);
            node = await LoadNodeAsync(node.ChildrenIds[childIdx], cancellationToken).ConfigureAwait(false);
        }
        return node;
    }

    private async Task<DiskBTreeNode> FindFirstCandidateLeafNodeAsync(IndexKey key, CancellationToken cancellationToken)
    {
        var node = await FindLeafNodeAsync(key, cancellationToken).ConfigureAwait(false);

        while (node.PrevSiblingId != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var prev = await LoadNodeAsync(node.PrevSiblingId, cancellationToken).ConfigureAwait(false);
            if (prev.KeyCount == 0 || prev.Keys[prev.KeyCount - 1].CompareTo(key) < 0)
            {
                break;
            }

            node = prev;
        }

        return node;
    }

    private async Task<DiskBTreeNode> FindLastCandidateLeafNodeAsync(IndexKey key, CancellationToken cancellationToken)
    {
        var node = await FindLeafNodeAsync(key, cancellationToken).ConfigureAwait(false);

        while (node.NextSiblingId != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var next = await LoadNodeAsync(node.NextSiblingId, cancellationToken).ConfigureAwait(false);
            if (next.KeyCount == 0 || next.Keys[0].CompareTo(key) > 0)
            {
                break;
            }

            node = next;
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
        var results = new List<BsonValue>();
        using var lease = BeginPageLease();
        var node = LoadNode(_rootPageId);
        while (!node.IsLeaf) node = LoadNode(node.ChildrenIds[0]); 
        while (node != null)
        {
            results.AddRange(node.Values);
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }

        return results;
    }

    /// <summary>
    /// 获取树中的所有值（按降序返回）。
    /// </summary>
    public IEnumerable<BsonValue> GetAllReverse()
    {
        ThrowIfDisposed();
        var results = new List<BsonValue>();
        using var lease = BeginPageLease();
        var node = LoadNode(_rootPageId);
        while (!node.IsLeaf) node = LoadNode(node.ChildrenIds[^1]);

        while (node != null)
        {
            for (int i = node.Values.Count - 1; i >= 0; i--)
            {
                results.Add(node.Values[i]);
            }

            if (node.PrevSiblingId == 0) break;
            node = LoadNode(node.PrevSiblingId);
        }

        return results;
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

        int minKeys = GetMinimumKeyCount(node);
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

        if (childIndex > 0)
        {
            var leftSibling = LoadNode(parent.ChildrenIds[childIndex - 1]);
            if (CanMerge(leftSibling, node))
            {
                MergeNodes(parent, leftSibling, node, childIndex - 1);
                Rebalance(parent);
                return;
            }
        }

        if (childIndex < parent.ChildrenIds.Count - 1)
        {
            var rightSibling = LoadNode(parent.ChildrenIds[childIndex + 1]);
            if (CanMerge(node, rightSibling))
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

    private bool CanMerge(DiskBTreeNode left, DiskBTreeNode right)
    {
        // DiskBTreeNode.Save stores oversized node payloads in an overflow chain, so
        // single-page byte capacity must not block a logically valid merge.
        var mergedKeyCount = left.KeyCount + right.KeyCount + (left.IsLeaf ? 0 : 1);
        return mergedKeyCount <= _maxKeys;
    }

    private int GetMinimumKeyCount(DiskBTreeNode node)
    {
        return Math.Max(1, node.IsLeaf ? _maxKeys / 2 : (_maxKeys - 1) / 2);
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
        if (node.PageId != _rootPageId)
        {
            int minKeys = GetMinimumKeyCount(node);
            if (node.KeyCount < minKeys) return false;
        }

        if (node.KeyCount > _maxKeys) return false;

        // 1. Validate keys are sorted
        for (int i = 0; i < node.KeyCount - 1; i++)
        {
            if (node.Keys[i].CompareTo(node.Keys[i + 1]) > 0) return false;
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
            if (node.ChildrenIds.Count != node.KeyCount + 1) return false;

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



    internal DiskBTreeNode LoadNode(uint id)
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

    internal async Task<DiskBTreeNode> LoadNodeAsync(uint id, CancellationToken cancellationToken = default)
    {
        var page = await _pm.GetPageAsync(id, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (page.CachedParsedData is DiskBTreeNode node)
        {
            return node;
        }

        node = await DiskBTreeNode.LoadAsync(page, _pm, cancellationToken).ConfigureAwait(false);
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
    }
    private void ThrowIfDisposed() { if (_disposed) throw new ObjectDisposedException(nameof(DiskBTree)); }
}
