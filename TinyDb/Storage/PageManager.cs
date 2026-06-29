using System.Collections.Concurrent;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

/// <summary>
/// 页面管理器，负责页面的分配、缓存和管理
/// </summary>
public sealed class PageManager : IDisposable
{
    private readonly IDiskStream _diskStream;
    private readonly uint _pageSize;
    private readonly uint _physicalPageSize;
    private readonly IPageCodec _pageCodec;
    private readonly ConcurrentDictionary<uint, Page> _pageCache;
    private readonly object _allocationLock = new(); // 用于分配新页面的锁
    private readonly object _freeListLock = new();
    private readonly object[] _pageLocks; // 分段锁，用于页面级别的同步
    private readonly LRUCache<uint, Page> _lruCache;
    private readonly Action<TinyDbLogLevel, string, Exception?> _log;
    private const int LockStripes = 64; // 分段锁的数量
    private uint _nextPageID;
    private uint _firstFreePageID; // Head of free page linked list
    private long _fileSize;
    private bool _disposed;

    private Action<long>? _flushLogToLsn;
    private Func<long, CancellationToken, Task>? _flushLogToLsnAsync;
    private Action<Page, byte[]?>? _appendLogPage;
    private Func<bool>? _requiresWalBeforeImage;

    /// <summary>
    /// 注册 WAL 刷新回调
    /// </summary>
    /// <param name="flushLogToLsn">根据 LSN 刷新日志的异步函数</param>
    public void RegisterWAL(Func<long, Task> flushLogToLsnAsync)
    {
        if (flushLogToLsnAsync == null) throw new ArgumentNullException(nameof(flushLogToLsnAsync));
        _flushLogToLsnAsync = (lsn, _) => flushLogToLsnAsync(lsn);
    }

    public void RegisterWAL(Func<long, CancellationToken, Task> flushLogToLsnAsync)
    {
        if (flushLogToLsnAsync == null) throw new ArgumentNullException(nameof(flushLogToLsnAsync));
        _flushLogToLsnAsync = flushLogToLsnAsync;
    }

    public void RegisterWAL(Action<long> flushLogToLsn)
    {
        if (flushLogToLsn == null) throw new ArgumentNullException(nameof(flushLogToLsn));

        _flushLogToLsn = flushLogToLsn;
    }

    public void RegisterWAL(Action<Page> appendLogPage, Action<long> flushLogToLsn)
    {
        if (appendLogPage == null) throw new ArgumentNullException(nameof(appendLogPage));
        RegisterWAL((page, _) => appendLogPage(page), flushLogToLsn, null);
    }

    public void RegisterWAL(Action<Page, byte[]?> appendLogPage, Action<long> flushLogToLsn, Func<bool>? requiresBeforeImage = null)
    {
        if (appendLogPage == null) throw new ArgumentNullException(nameof(appendLogPage));
        if (flushLogToLsn == null) throw new ArgumentNullException(nameof(flushLogToLsn));

        _appendLogPage = appendLogPage;
        _flushLogToLsn = flushLogToLsn;
        _requiresWalBeforeImage = requiresBeforeImage;
    }

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
    public uint TotalPages => (uint)(_fileSize / _physicalPageSize);

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
    public PageManager(
        IDiskStream diskStream,
        uint pageSize = 8192,
        int maxCacheSize = 1000,
        Action<TinyDbLogLevel, string, Exception?>? logger = null)
        : this(diskStream, pageSize, maxCacheSize, logger, null)
    {
    }

    internal PageManager(
        IDiskStream diskStream,
        uint pageSize,
        int maxCacheSize,
        Action<TinyDbLogLevel, string, Exception?>? logger,
        IPageCodec? pageCodec)
    {
        _diskStream = diskStream ?? throw new ArgumentNullException(nameof(diskStream));
        if (pageSize <= PageHeader.Size) throw new ArgumentException($"Page size must be larger than {PageHeader.Size}", nameof(pageSize));
        if (maxCacheSize <= 0) throw new ArgumentOutOfRangeException(nameof(maxCacheSize));

        _log = logger ?? TinyDbLogging.NoopLogger;
        _pageSize = pageSize;
        _pageCodec = pageCodec ?? new NoOpPageCodec(pageSize);
        if (_pageCodec.LogicalPageSize != pageSize)
        {
            throw new ArgumentException("Page codec logical page size must match PageManager page size.", nameof(pageCodec));
        }
        _physicalPageSize = _pageCodec.PhysicalPageSize;
        MaxCacheSize = maxCacheSize;
        _pageCache = new ConcurrentDictionary<uint, Page>();
        _lruCache = new LRUCache<uint, Page>(maxCacheSize);
        _pageLocks = new object[LockStripes];
        for (int i = 0; i < LockStripes; i++) _pageLocks[i] = new object();
        _fileSize = _diskStream.Size;
        _nextPageID = 0;
        _firstFreePageID = 0;
    }

    private void Log(TinyDbLogLevel level, string message, Exception? ex = null)
    {
        TinyDbLogging.SafeLog(_log, level, message, ex);
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
            var calculatedTotal = (uint)(_fileSize / _physicalPageSize);
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
            catch (Exception ex)
            {
                // Corrupted pages are not safe to add to the free list during recovery scanning.
                _log(TinyDbLogLevel.Warning, $"Skipping page {i} while rebuilding the free list.", ex);
            }
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
                    var pData = ReadLogicalPageData(current, pOffset);
                    var header = PageHeader.FromByteArray(pData);
                    current = header.NextPageID;
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Failed to traverse free-page list at page {current}.", ex);
                }
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
        if (pageOffset + _physicalPageSize > _fileSize)
        {
            // 文件不存在该页面，创建新页面
            return CreateNewPage(pageID, PageType.Empty);
        }

        try
        {
            var frame = _diskStream.ReadPage(pageOffset, (int)_physicalPageSize);
            if (IsZeroFilledPage(frame))
            {
                return CreateNewPage(pageID, PageType.Empty);
            }

            var pageData = _pageCodec.Decode(pageID, frame);
            var page = new Page(pageID, pageData);
            ValidateLoadedPage(page, pageData, pageOffset);

            // 如果是空页面，初始化为 Empty 类型
            if (page.Header.PageType == PageType.Empty && page.Header.ItemCount == 0)
            {
                page.UpdatePageType(PageType.Empty);
            }

            // 添加到缓存
            if (useCache)
            {
                return AddToCache(page);
            }

            return page;
        }
        catch (Exception ex)
        {
            throw new InvalidDataException($"Failed to load page {pageID} at offset {pageOffset}.", ex);
        }
    }

    private static void ValidateLoadedPage(Page page, byte[] pageData, long pageOffset)
    {
        if (!page.Header.IsValid())
        {
            throw new InvalidDataException($"Invalid page header for page {page.PageID} at offset {pageOffset}.");
        }

        if (!page.Header.VerifyChecksum(pageData))
        {
            throw new InvalidDataException($"Page checksum verification failed for page {page.PageID} at offset {pageOffset}.");
        }
    }

    internal bool TryReadLogicalPageSnapshot(uint pageID, out byte[] pageData)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));

        var pageOffset = CalculatePageOffset(pageID);
        if (pageOffset + _physicalPageSize > _fileSize)
        {
            pageData = Array.Empty<byte>();
            return false;
        }

        var frame = _diskStream.ReadPage(pageOffset, (int)_physicalPageSize);
        if (IsZeroFilledPage(frame))
        {
            pageData = Array.Empty<byte>();
            return false;
        }

        pageData = _pageCodec.Decode(pageID, frame);
        return true;
    }

    internal Page GetPagePinned(uint pageID, bool useCache = true)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));

        if (useCache)
        {
            var lockObj = _pageLocks[pageID % LockStripes];
            lock (lockObj)
            {
                if (_pageCache.TryGetValue(pageID, out var cachedPage))
                {
                    cachedPage.Pin();
                    _lruCache.Touch(pageID);
                    return cachedPage;
                }
            }
        }

        var pageOffset = CalculatePageOffset(pageID);
        if (pageOffset + _physicalPageSize > _fileSize)
        {
            return CreateNewPage(pageID, PageType.Empty, pinned: true);
        }

        try
        {
            var frame = _diskStream.ReadPage(pageOffset, (int)_physicalPageSize);
            if (IsZeroFilledPage(frame))
            {
                return CreateNewPage(pageID, PageType.Empty, pinned: true);
            }

            var pageData = _pageCodec.Decode(pageID, frame);
            var page = new Page(pageID, pageData);
            ValidateLoadedPage(page, pageData, pageOffset);

            if (page.Header.PageType == PageType.Empty && page.Header.ItemCount == 0)
            {
                page.UpdatePageType(PageType.Empty);
            }

            if (useCache)
            {
                page = AddToCache(page);
            }

            page.Pin();
            return page;
        }
        catch (Exception ex)
        {
            throw new InvalidDataException($"Failed to load page {pageID} at offset {pageOffset}.", ex);
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
        if (pageOffset + _physicalPageSize > _fileSize)
        {
            return CreateNewPage(pageID, PageType.Empty);
        }

        var frame = await _diskStream.ReadPageAsync(pageOffset, (int)_physicalPageSize, cancellationToken);

        try
        {
            if (IsZeroFilledPage(frame))
            {
                return CreateNewPage(pageID, PageType.Empty);
            }

            var pageData = _pageCodec.Decode(pageID, frame);
            var page = new Page(pageID, pageData);
            ValidateLoadedPage(page, pageData, pageOffset);

            // 如果是空页面，初始化为 Empty 类型
            if (page.Header.PageType == PageType.Empty && page.Header.ItemCount == 0)
            {
                page.UpdatePageType(PageType.Empty);
            }

            // 添加到缓存
            if (useCache)
            {
                return AddToCache(page);
            }

            return page;
        }
        catch (Exception ex)
        {
            throw new InvalidDataException($"Failed to parse page {pageID} at offset {pageOffset}.", ex);
        }
    }

    private static bool IsZeroFilledPage(byte[] pageData)
    {
        for (int i = 0; i < pageData.Length; i++)
        {
            if (pageData[i] != 0) return false;
        }

        return true;
    }

    private byte[] ReadLogicalPageData(uint pageID, long pageOffset)
    {
        var frame = _diskStream.ReadPage(pageOffset, (int)_physicalPageSize);
        if (IsZeroFilledPage(frame))
        {
            var page = new Page(pageID, (int)_pageSize, PageType.Empty);
            return page.Buffer;
        }

        return _pageCodec.Decode(pageID, frame);
    }

    /// <summary>
    /// 创建新页面
    /// </summary>
    /// <param name="pageType">页面类型</param>
    /// <returns>新页面</returns>
    public Page NewPage(PageType pageType)
    {
        ThrowIfDisposed();

        lock (_freeListLock)
        {
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
    }

    internal Page NewPagePinned(PageType pageType)
    {
        ThrowIfDisposed();

        lock (_freeListLock)
        {
            lock (_allocationLock)
            {
                uint pageID;

                if (_firstFreePageID != 0)
                {
                    pageID = _firstFreePageID;
                    var freePage = GetPagePinned(pageID);
                    _firstFreePageID = freePage.Header.NextPageID;

                    freePage.ClearData();
                    freePage.UpdatePageType(pageType);
                    freePage.SetLinks(0, 0);

                    return freePage;
                }

                pageID = ++_nextPageID;
                return CreateNewPage(pageID, pageType, pinned: true);
            }
        }
    }

    /// <summary>
    /// 创建指定ID的新页面
    /// </summary>
    /// <param name="pageID">页面ID</param>
    /// <param name="pageType">页面类型</param>
    /// <returns>新页面</returns>
    private Page CreateNewPage(uint pageID, PageType pageType, bool pinned = false)
    {
        var page = new Page(pageID, (int)_pageSize, pageType);
        page.UpdateStats((ushort)Math.Min(page.DataSize, ushort.MaxValue), 0);
        page = AddToCache(page);
        if (pinned)
        {
            page.Pin();
        }

        // 计算新的文件大小
        var newFileSize = CalculatePageOffset(pageID) + _physicalPageSize;

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

        // WAL 检查：日志必须先于数据落盘
        byte[]? beforeImage = null;
        if (_appendLogPage != null && _requiresWalBeforeImage?.Invoke() == true)
        {
            lock (_allocationLock)
            {
                beforeImage = ReadPageSnapshotForWal(page.PageID);
            }
        }

        _appendLogPage?.Invoke(page, beforeImage);
        if (_flushLogToLsn != null && page.Header.LSN >= 0)
        {
            _flushLogToLsn(page.Header.LSN);
        }

        lock (_allocationLock)
        {
            // 更新页面校验和
            page.UpdateChecksum();

            var pageOffset = CalculatePageOffset(page.PageID);
            _diskStream.WritePage(pageOffset, _pageCodec.Encode(page.PageID, page.Buffer));

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
    public Task SavePageAsync(Page page, bool forceFlush = false, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (page == null) throw new ArgumentNullException(nameof(page));
        cancellationToken.ThrowIfCancellationRequested();

        SavePage(page, forceFlush);
        return Task.CompletedTask;
    }

    private void WritePageToDisk(Page page, bool forceFlush)
    {
        page.UpdateChecksum();

        var pageOffset = CalculatePageOffset(page.PageID);
        _diskStream.WritePage(pageOffset, _pageCodec.Encode(page.PageID, page.Buffer));

        if (forceFlush)
        {
            _diskStream.Flush();
        }

        page.MarkClean();
    }

    private byte[]? ReadPageSnapshotForWal(uint pageID)
    {
        var pageOffset = CalculatePageOffset(pageID);
        if (pageOffset + _physicalPageSize > _fileSize)
        {
            return null;
        }

        var frame = _diskStream.ReadPage(pageOffset, (int)_physicalPageSize);
        if (IsZeroFilledPage(frame))
        {
            return null;
        }

        return _pageCodec.Decode(pageID, frame);
    }

    /// <summary>
    /// 释放页面
    /// </summary>
    /// <param name="pageID">页面ID</param>
    public void FreePage(uint pageID)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));

        Page page;
        byte[]? beforeImage = null;

        lock (_freeListLock)
        {
            lock (_allocationLock)
            {
                page = GetPage(pageID);
                if (_appendLogPage != null && _requiresWalBeforeImage?.Invoke() == true)
                {
                    beforeImage = ReadPageSnapshotForWal(pageID);
                }

                page.ClearData();
                page.UpdatePageType(PageType.Empty);
                page.SetLinks(0, _firstFreePageID);
                page.Header.ItemCount = 0;
                page.Header.FreeBytes = (ushort)(page.DataSize);
            }

            _appendLogPage?.Invoke(page, beforeImage);
            if (_flushLogToLsn != null && page.Header.LSN >= 0)
            {
                _flushLogToLsn(page.Header.LSN);
            }

            lock (_allocationLock)
            {
                WritePageToDisk(page, forceFlush: false);
                _firstFreePageID = pageID;
                RemoveFromCache(pageID);
            }
        }
    }

    /// <summary>
    /// 刷新所有脏页面到磁盘
    /// </summary>
    public void FlushDirtyPages()
    {
        ThrowIfDisposed();

        List<Page> dirtyPages;
        lock (_allocationLock)
        {
            dirtyPages = _pageCache.Values.Where(p => p.IsDirty).ToList();
        }

        foreach (var page in dirtyPages)
        {
            SavePage(page);
        }

        _diskStream.Flush();
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

        foreach (var page in dirtyPages)
        {
            cancellationToken.ThrowIfCancellationRequested();
            SavePage(page);
        }

        await _diskStream.FlushAsync(cancellationToken).ConfigureAwait(false);
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

            var restoredPage = new Page(pageID, buffer);
            restoredPage.UpdateChecksum();
            buffer = restoredPage.Buffer;

            var pageOffset = CalculatePageOffset(pageID);
            _diskStream.WritePage(pageOffset, _pageCodec.Encode(pageID, buffer));
            _diskStream.Flush();

            RemoveFromCache(pageID);

            _fileSize = Math.Max(_fileSize, pageOffset + _physicalPageSize);
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
        if (pageID == 0) throw new ArgumentOutOfRangeException(nameof(pageID), "Page ID cannot be zero.");
        return ((long)pageID - 1) * _physicalPageSize;
    }

    /// <summary>
    /// 添加页面到缓存
    /// </summary>
    /// <param name="page">页面</param>
    private Page AddToCache(Page page)
    {
        while (true)
        {
            if (_pageCache.TryGetValue(page.PageID, out var cachedPage))
            {
                _lruCache.Touch(page.PageID);
                return cachedPage;
            }

            // 如果缓存已满，移除最少使用的页面
            if (_pageCache.Count >= MaxCacheSize)
            {
                EvictLeastRecentlyUsed();
            }

            if (_pageCache.TryAdd(page.PageID, page))
            {
                _lruCache.Put(page.PageID, page);
                return page;
            }
        }
    }

    /// <summary>
    /// 驱逐最少使用的页面
    /// </summary>
    private void EvictLeastRecentlyUsed()
    {
        // 尝试从 LRU 获取多个候选者，以防第一个被 Pin 住
        var candidates = _lruCache.GetLeastRecentlyUsed(10);
        foreach (var pageID in candidates)
        {
            var lockObj = _pageLocks[pageID % LockStripes];
            lock (lockObj)
            {
                if (_pageCache.TryGetValue(pageID, out var page))
                {
                    // 如果页面被锁定（PinCount > 0），不能驱逐
                    if (page.PinCount > 0) continue;

                    if (_pageCache.TryRemove(pageID, out var removedPage))
                    {
                        if (removedPage.IsDirty)
                        {
                            SavePage(removedPage);
                        }
                        _lruCache.Remove(pageID);
                        return; // 成功驱逐一个，退出
                    }
                }
            }
        }
    }

    private void RemoveFromCache(uint pageID)
    {
        _pageCache.TryRemove(pageID, out _);
        _lruCache.Remove(pageID);
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
            Exception? flushException = null;
            Exception? cleanupException = null;
            try
            {
                try
                {
                    // 刷新所有脏页面
                    FlushDirtyPages();
                }
                catch (Exception ex)
                {
                    flushException = new InvalidOperationException("Flush dirty pages during PageManager dispose failed.", ex);
                }

                // 清理缓存
                ClearCache();

                _diskStream.Dispose();
            }
            catch (Exception ex)
            {
                cleanupException = new InvalidOperationException("PageManager dispose failed.", ex);
            }
            finally
            {
                _disposed = true;
            }

            if (flushException != null || cleanupException != null)
            {
                var exceptions = new List<Exception>();
                if (flushException != null) exceptions.Add(flushException);
                if (cleanupException != null) exceptions.Add(cleanupException);
                throw new AggregateException("One or more errors occurred during PageManager dispose.", exceptions);
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
        return $"PageManager: {UsedPages}/{TotalPages} used, {CachedPages}/{MaxCacheSize} cached, {DirtyPages} dirty, HitRatio={CacheHitRatio * 100:F1}%";
    }
}

