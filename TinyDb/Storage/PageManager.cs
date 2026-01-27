using System.Collections.Concurrent;
using TinyDb.Utils;

namespace TinyDb.Storage;

/// <summary>
/// 页面管理器，负责页面的分配、缓存和管理
/// </summary>
public sealed class PageManager : IDisposable
{
    private readonly IDiskStream _diskStream;
    private readonly uint _pageSize;
    private readonly ConcurrentDictionary<uint, Page> _pageCache;
    private readonly object _allocationLock = new();
    private readonly LRUCache<uint, Page> _lruCache;
    private uint _nextPageID;
    private uint _firstFreePageID; // Head of free page linked list
    private long _fileSize;
    private bool _disposed;

    /// <summary>
    /// 页面大小
    /// </summary>
    public uint PageSize => _pageSize;

    /// <summary>
    /// 缓存页面数量
    /// </summary>
    public int CachedPages => _pageCache.Count;

    /// <summary>
    /// 第一个空闲页面ID
    /// </summary>
    public uint FirstFreePageID => _firstFreePageID;

    /// <summary>
    /// 总页面数量
    /// </summary>
    public uint TotalPages => (uint)(_fileSize / _pageSize);

    /// <summary>
    /// 最大缓存页面数
    /// </summary>
    public int MaxCacheSize { get; }

    /// <summary>
    /// 初始化页面管理器
    /// </summary>
    /// <param name="diskStream">磁盘流</param>
    /// <param name="pageSize">页面大小</param>
    /// <param name="maxCacheSize">最大缓存大小</param>
    public PageManager(IDiskStream diskStream, uint pageSize = 8192, int maxCacheSize = 1000)
    {
        _diskStream = diskStream ?? throw new ArgumentNullException(nameof(diskStream));
        if (pageSize <= PageHeader.Size) throw new ArgumentException($"Page size must be larger than {PageHeader.Size}", nameof(pageSize));
        if (maxCacheSize <= 0) throw new ArgumentOutOfRangeException(nameof(maxCacheSize));

        _pageSize = pageSize;
        MaxCacheSize = maxCacheSize;
        _pageCache = new ConcurrentDictionary<uint, Page>();
        _lruCache = new LRUCache<uint, Page>(maxCacheSize);
        _fileSize = _diskStream.Size;
        _nextPageID = 0;
        _firstFreePageID = 0;
    }

    /// <summary>
    /// 初始化页面状态（避免全盘扫描）
    /// </summary>
    /// <param name="totalPages">总页面数</param>
    /// <param name="firstFreePageID">第一个空闲页面ID</param>
    public void Initialize(uint totalPages, uint firstFreePageID)
    {
        lock (_allocationLock)
        {
            // 如果文件大小不匹配 TotalPages，优先信任文件大小
            var calculatedTotal = (uint)(_fileSize / _pageSize);
            _nextPageID = Math.Max(totalPages, calculatedTotal);
            _firstFreePageID = firstFreePageID;

            // 关键修复：如果 _firstFreePageID 为 0 但文件中有页面，
            // 可能是由于非正常关闭导致的空闲链表丢失，执行一次快速扫描恢复
            if (_firstFreePageID == 0 && _nextPageID > 1)
            {
                ScanFreePages();
            }
        }
    }

    private void ScanFreePages()
    {
        uint lastFreeId = 0;
        for (uint i = 2; i <= _nextPageID; i++)
        {
            try
            {
                var p = GetPage(i, false);
                if (p.Header.PageType == PageType.Empty)
                {
                    if (_firstFreePageID == 0) _firstFreePageID = i;
                    if (lastFreeId != 0)
                    {
                        var lastPage = GetPage(lastFreeId, false);
                        lastPage.Header.NextPageID = i;
                        SavePage(lastPage);
                    }
                    lastFreeId = i;
                }
            }
            catch { }
        }
    }

    /// <summary>
    /// 获取页面使用统计
    /// </summary>
    /// <returns>页面统计信息</returns>
    public PageManagerStatistics GetStatistics()
    {
        ThrowIfDisposed();

        lock (_allocationLock)
        {
            var dirtyPages = _pageCache.Values.Count(p => p.IsDirty);
            var totalPages = TotalPages;
            
            // 简单估算：扫描空闲链表长度
            uint freeCount = 0;
            uint current = _firstFreePageID;
            var visited = new HashSet<uint>();
            while (current != 0 && visited.Add(current))
            {
                freeCount++;
                try { 
                    var pOffset = CalculatePageOffset(current);
                    var pData = _diskStream.ReadPage(pOffset, (int)_pageSize);
                    var header = PageHeader.FromByteArray(pData);
                    current = header.NextPageID;
                } catch { break; }
            }

            return new PageManagerStatistics
            {
                PageSize = _pageSize,
                TotalPages = totalPages,
                UsedPages = totalPages > 0 ? (totalPages - 1 - freeCount) : 0, // 减去头部
                FreePages = freeCount,
                CachedPages = _pageCache.Count,
                DirtyPages = dirtyPages,
                MaxCacheSize = MaxCacheSize,
                CacheHitRatio = _lruCache.HitRatio,
                FileSize = _fileSize,
                NextPageID = _nextPageID
            };
        }
    }

    /// <summary>
    /// 获取页面
    /// </summary>
    /// <param name="pageID">页面ID</param>
    /// <param name="useCache">是否使用缓存</param>
    /// <returns>页面实例</returns>
    public Page GetPage(uint pageID, bool useCache = true)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));

        // 首先尝试从缓存获取
        if (useCache && _pageCache.TryGetValue(pageID, out var cachedPage))
        {
            _lruCache.Touch(pageID);
            return cachedPage;
        }

        // 从磁盘读取
        var pageOffset = CalculatePageOffset(pageID);

        // 检查是否超出文件大小
        if (pageOffset + _pageSize > _fileSize)
        {
            // 文件不存在该页面，创建新页面
            return CreateNewPage(pageID, PageType.Empty);
        }

        try
        {
            var pageData = _diskStream.ReadPage(pageOffset, (int)_pageSize);
            var page = new Page(pageID, pageData);

            // 如果是空页面，初始化为 Empty 类型
            if (page.Header.PageType == PageType.Empty && page.Header.ItemCount == 0)
            {
                page.UpdatePageType(PageType.Empty);
            }

            // 添加到缓存
            if (useCache)
            {
                AddToCache(page);
            }

            return page;
        }
        catch (Exception)
        {
            // 如果页面损坏，创建一个新的空页面
            return CreateNewPage(pageID, PageType.Empty);
        }
    }

    /// <summary>
    /// 异步获取页面
    /// </summary>
    /// <param name="pageID">页面ID</param>
    /// <param name="useCache">是否使用缓存</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>页面实例</returns>
    public async Task<Page> GetPageAsync(uint pageID, bool useCache = true, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));

        // 首先尝试从缓存获取
        if (useCache && _pageCache.TryGetValue(pageID, out var cachedPage))
        {
            _lruCache.Touch(pageID);
            return cachedPage;
        }

        // 从磁盘异步读取
        var pageOffset = CalculatePageOffset(pageID);
        var pageData = await _diskStream.ReadPageAsync(pageOffset, (int)_pageSize, cancellationToken);

        try
        {
            var page = new Page(pageID, pageData);

            // 如果是空页面，初始化为 Empty 类型
            if (page.Header.PageType == PageType.Empty && page.Header.ItemCount == 0)
            {
                page.UpdatePageType(PageType.Empty);
            }

            // 添加到缓存
            if (useCache)
            {
                AddToCache(page);
            }

            return page;
        }
        catch (Exception)
        {
            // 如果页面损坏，创建一个新的空页面
            return await Task.FromResult(CreateNewPage(pageID, PageType.Empty));
        }
    }

    /// <summary>
    /// 创建新页面
    /// </summary>
    /// <param name="pageType">页面类型</param>
    /// <returns>新页面</returns>
    public Page NewPage(PageType pageType)
    {
        ThrowIfDisposed();

        lock (_allocationLock)
        {
            uint pageID;

            // 尝试从空闲页面链表获取
            if (_firstFreePageID != 0)
            {
                pageID = _firstFreePageID;
                // 读取该页面以获取下一个空闲页面ID
                var freePage = GetPage(pageID);
                _firstFreePageID = freePage.Header.NextPageID;
                
                // 重置页面状态
                freePage.ClearData();
                freePage.UpdatePageType(pageType);
                freePage.SetLinks(0, 0); 
                
                return freePage;
            }

            // 分配新页面
            pageID = ++_nextPageID;
            return CreateNewPage(pageID, pageType);
        }
    }

    /// <summary>
    /// 创建指定ID的新页面
    /// </summary>
    /// <param name="pageID">页面ID</param>
    /// <param name="pageType">页面类型</param>
    /// <returns>新页面</returns>
    private Page CreateNewPage(uint pageID, PageType pageType)
    {
        var page = new Page(pageID, (int)_pageSize, pageType);
        page.UpdateStats((ushort)Math.Min(page.DataSize, ushort.MaxValue), 0);
        AddToCache(page);

        // 计算新的文件大小
        var newFileSize = CalculatePageOffset(pageID) + _pageSize;

        // 只有当新文件大小大于当前大小时才更新
        if (newFileSize > _fileSize)
        {
            _fileSize = newFileSize;
            // 确保磁盘流的大小也被正确设置
            _diskStream.SetLength(_fileSize);
        }

        return page;
    }

    /// <summary>
    /// 保存页面
    /// </summary>
    /// <param name="page">要保存的页面</param>
    /// <param name="forceFlush">是否强制刷新到磁盘</param>
    public void SavePage(Page page, bool forceFlush = false)
    {
        ThrowIfDisposed();
        if (page == null) throw new ArgumentNullException(nameof(page));

        lock (_allocationLock)
        {
            // 更新页面校验和
            page.UpdateChecksum();

            var pageOffset = CalculatePageOffset(page.PageID);
            _diskStream.WritePage(pageOffset, page.FullData.ToArray());

            if (forceFlush)
            {
                _diskStream.Flush();
            }

            // 标记页面为干净
            page.MarkClean();
        }
    }

    /// <summary>
    /// 异步保存页面
    /// </summary>
    /// <param name="page">要保存的页面</param>
    /// <param name="forceFlush">是否强制刷新到磁盘</param>
    /// <param name="cancellationToken">取消令牌</param>
    public async Task SavePageAsync(Page page, bool forceFlush = false, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (page == null) throw new ArgumentNullException(nameof(page));

        lock (_allocationLock)
        {
            // 更新页面校验和
            page.UpdateChecksum();
        }

        var pageOffset = CalculatePageOffset(page.PageID);
        await _diskStream.WritePageAsync(pageOffset, page.FullData.ToArray(), cancellationToken);

        if (forceFlush)
        {
            await _diskStream.FlushAsync(cancellationToken);
        }

        // 标记页面为干净
        page.MarkClean();
    }

    /// <summary>
    /// 释放页面
    /// </summary>
    /// <param name="pageID">页面ID</param>
    public void FreePage(uint pageID)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));

        lock (_allocationLock)
        {
            // 获取页面
            var page = GetPage(pageID); 
            
            // 标记为空闲并链接到链表头部
            page.ClearData();
            page.UpdatePageType(PageType.Empty);
            
            // NextPageID 指向旧的头部
            page.SetLinks(0, _firstFreePageID);
            
            // 更新头部指针
            _firstFreePageID = pageID;

            page.Header.ItemCount = 0;
            page.Header.FreeBytes = (ushort)(page.DataSize);
            page.UpdateChecksum();

            SavePage(page, forceFlush: false);

            if (_pageCache.TryRemove(pageID, out _))
            {
                _lruCache.Remove(pageID);
            }
            page.Dispose();
        }
    }

    /// <summary>
    /// 刷新所有脏页面到磁盘
    /// </summary>
    public void FlushDirtyPages()
    {
        ThrowIfDisposed();

        lock (_allocationLock)
        {
            var dirtyPages = _pageCache.Values.Where(p => p.IsDirty).ToList();

            foreach (var page in dirtyPages)
            {
                SavePage(page);
            }
        }
    }

    /// <summary>
    /// 异步刷新所有脏页面到磁盘
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    public async Task FlushDirtyPagesAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();

        List<Page> dirtyPages;
        lock (_allocationLock)
        {
            dirtyPages = _pageCache.Values.Where(p => p.IsDirty).ToList();
        }

        var saveTasks = dirtyPages.Select(page => SavePageAsync(page, false, cancellationToken));
        await Task.WhenAll(saveTasks);

        // 最后刷新磁盘流
        await _diskStream.FlushAsync(cancellationToken);
    }

    /// <summary>
    /// 从 WAL 恢复页面数据
    /// </summary>
    /// <param name="pageID">页面 ID</param>
    /// <param name="pageData">页面数据</param>
    internal void RestorePage(uint pageID, byte[] pageData)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));
        if (pageData == null) throw new ArgumentNullException(nameof(pageData));
        lock (_allocationLock)
        {
            byte[] buffer;
            if (pageData.Length == _pageSize)
            {
                buffer = pageData;
            }
            else if (pageData.Length < _pageSize)
            {
                buffer = new byte[_pageSize];
                Array.Copy(pageData, buffer, pageData.Length);
            }
            else
            {
                throw new ArgumentException($"Page data length must be less or equal to {(int)_pageSize} bytes", nameof(pageData));
            }

            var pageOffset = CalculatePageOffset(pageID);
            _diskStream.WritePage(pageOffset, buffer);

            if (_pageCache.TryRemove(pageID, out var cachedPage))
            {
                cachedPage.Dispose();
                _lruCache.Remove(pageID);
            }

            _fileSize = Math.Max(_fileSize, pageOffset + _pageSize);
        }
    }

    /// <summary>
    /// 判断是否有脏页面
    /// </summary>
    public bool HasDirtyPages()
    {
        ThrowIfDisposed();

        lock (_allocationLock)
        {
            foreach (var page in _pageCache.Values)
            {
                if (page.IsDirty)
                {
                    return true;
                }
            }
        }

        return false;
    }

    /// <summary>
    /// 清理缓存
    /// </summary>
    /// <param name="maxPagesToKeep">保留的最大页面数</param>
    public void ClearCache(int maxPagesToKeep = 0)
    {
        ThrowIfDisposed();

        lock (_allocationLock)
        {
            if (maxPagesToKeep <= 0)
            {
                // 清空所有缓存
                foreach (var page in _pageCache.Values)
                {
                    page.Dispose();
                }
                _pageCache.Clear();
                _lruCache.Clear();
            }
            else
            {
                // 保留最近使用的页面
                var pagesToRemove = _pageCache.Count - maxPagesToKeep;
                if (pagesToRemove > 0)
                {
                    var lruPages = _lruCache.GetLeastRecentlyUsed(pagesToRemove);
                    foreach (var pageID in lruPages)
                    {
                        if (_pageCache.TryRemove(pageID, out var page))
                        {
                            page.Dispose();
                        }
                    }
                }
            }
        }
    }

    /// <summary>
    /// 计算页面偏移量
    /// </summary>
    /// <param name="pageID">页面ID</param>
    /// <returns>偏移量</returns>
    private long CalculatePageOffset(uint pageID)
    {
        return (pageID - 1) * _pageSize;
    }

    /// <summary>
    /// 添加页面到缓存
    /// </summary>
    /// <param name="page">页面</param>
    private void AddToCache(Page page)
    {
        // 如果缓存已满，移除最少使用的页面
        if (_pageCache.Count >= MaxCacheSize && !_pageCache.ContainsKey(page.PageID))
        {
            EvictLeastRecentlyUsed();
        }

        _pageCache[page.PageID] = page;
        _lruCache.Put(page.PageID, page);
    }

    /// <summary>
    /// 驱逐最少使用的页面
    /// </summary>
    private void EvictLeastRecentlyUsed()
    {
        if (_lruCache.TryGetLeastRecentlyUsed(out var pageID, out var page))
        {
            if (_pageCache.TryRemove(pageID, out var removedPage))
            {
                if (removedPage.IsDirty)
                {
                    SavePage(removedPage);
                }
                removedPage.Dispose();
            }
            _lruCache.Remove(pageID);
        }
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(PageManager));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                // 刷新所有脏页面
                FlushDirtyPages();

                // 清理缓存
                ClearCache();

                _diskStream?.Dispose();
            }
            catch
            {
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}

/// <summary>
/// 页面管理器统计信息
/// </summary>
public sealed class PageManagerStatistics
{
    public uint PageSize { get; init; }
    public uint TotalPages { get; init; }
    public uint UsedPages { get; init; }
    public uint FreePages { get; init; }
    public int CachedPages { get; init; }
    public int DirtyPages { get; init; }
    public int MaxCacheSize { get; init; }
    public double CacheHitRatio { get; init; }
    public long FileSize { get; init; }
    public uint NextPageID { get; init; }

    public override string ToString()
    {
        return $"PageManager: {UsedPages}/{TotalPages} used, {CachedPages}/{MaxCacheSize} cached, {DirtyPages} dirty, HitRatio={CacheHitRatio:P1}";
    }
}
