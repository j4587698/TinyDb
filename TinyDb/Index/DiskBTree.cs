using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;

namespace TinyDb.Index;

public sealed partial class DiskBTree : IDisposable
{
    private readonly PageManager _pm;
    private readonly uint _rootPageId;
    private readonly int _maxKeys;
    private bool _disposed;

    private static readonly AsyncLocal<PageLease?> _currentLease = new();

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

    internal int MaxKeys => _maxKeys;

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
    /// <remarks>
    /// 遍历遇到结构损坏（子指针指向的页已不再是索引页）时不会抛出异常，而是跳过该子树继续统计。
    /// 统计属性若因损坏抛异常，会连带让 CompactDatabase 之类的修复手段也无法执行，
    /// 使一处局部损坏放大成整库不可恢复。需要知晓损坏范围时请改用 <see cref="CountNodes(out int)"/>。
    /// </remarks>
    public int NodeCount
    {
        get
        {
            ThrowIfDisposed();
            using var lease = BeginPageLease();
            var damagedSubtrees = 0;
            return CountNodes(_rootPageId, ref damagedSubtrees);
        }
    }

    /// <summary>
    /// 统计节点总数，同时报告因结构损坏而无法遍历的子树数量。
    /// </summary>
    /// <param name="damagedSubtrees">无法加载的子树个数，0 表示结构完好。</param>
    /// <returns>可正常遍历到的节点总数。</returns>
    internal int CountNodes(out int damagedSubtrees)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var damaged = 0;
        var count = CountNodes(_rootPageId, ref damaged);
        damagedSubtrees = damaged;
        return count;
    }

    /// <summary>
    /// 获取树的高度。
    /// </summary>
    /// <remarks>
    /// 遇到损坏的子指针时返回已能确认的高度，不抛异常，理由同 <see cref="NodeCount"/>。
    /// 结构完好时最小值为 1（空树的根页本身即叶子），因此返回 0 唯一表示根页不可读。
    /// </remarks>
    public int Height
    {
        get
        {
            ThrowIfDisposed();
            using var lease = BeginPageLease();
            DiskBTreeNode node;
            try
            {
                node = LoadNode(_rootPageId);
            }
            catch (InvalidDataException)
            {
                return 0;
            }

            int height = 1;
            while (!node.IsLeaf)
            {
                if (node.ChildrenIds.Count == 0) break;
                try
                {
                    node = LoadNode(node.ChildrenIds[0]);
                }
                catch (InvalidDataException)
                {
                    break;
                }
                height++;
            }
            return height;
        }
    }

    private int CountNodes(uint pageId, ref int damagedSubtrees)
    {
        DiskBTreeNode node;
        try
        {
            node = LoadNode(pageId);
        }
        catch (InvalidDataException)
        {
            // 子指针悬空：目标页已被改作他用，这棵子树里的索引项无法再恢复。
            // 跳过并计数，而不是中断整次遍历。
            damagedSubtrees++;
            return 0;
        }

        int count = 1;
        if (!node.IsLeaf)
        {
            foreach (var childId in node.ChildrenIds)
            {
                count += CountNodes(childId, ref damagedSubtrees);
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
        var lease = new PageLease(_currentLease.Value);
        _currentLease.Value = lease;
        return lease;
    }

    private Page GetPage(uint pageId)
    {
        return _currentLease.Value?.GetPage(_pm, pageId) ?? _pm.GetPage(pageId);
    }

    private Page NewIndexPage()
    {
        return _currentLease.Value?.NewPage(_pm, PageType.Index) ?? _pm.NewPage(PageType.Index);
    }

    private void FreePage(uint pageId)
    {
        _currentLease.Value?.ReleasePage(pageId);
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

        public async Task<Page> GetPageAsync(
            PageManager pageManager,
            uint pageId,
            CancellationToken cancellationToken)
        {
            if (_pagesById.TryGetValue(pageId, out var page))
            {
                return page;
            }

            page = await pageManager.GetPagePinnedAsync(pageId, cancellationToken: cancellationToken).ConfigureAwait(false);
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
            if (_currentLease.Value == lease)
            {
                _currentLease.Value = lease._previous;
                lease._previous = null;
                return;
            }

            var current = _currentLease.Value;
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
        var lease = _currentLease.Value;
        var page = lease == null
            ? await _pm.GetPagePinnedAsync(id, cancellationToken: cancellationToken).ConfigureAwait(false)
            : await lease.GetPageAsync(_pm, id, cancellationToken).ConfigureAwait(false);

        try
        {
            if (page.CachedParsedData is DiskBTreeNode node)
            {
                return node;
            }

            node = await DiskBTreeNode.LoadAsync(page, _pm, cancellationToken).ConfigureAwait(false);
            page.CachedParsedData = node;
            return node;
        }
        finally
        {
            if (lease == null)
            {
                page.Unpin();
            }
        }
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
