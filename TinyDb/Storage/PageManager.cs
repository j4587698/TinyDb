using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading;
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
    private readonly ConcurrentDictionary<uint, byte> _dirtyPageIds;
    private readonly object _stateLock = new();
    private readonly object _cacheLock = new();
    private readonly object _freeListLock = new();
    private readonly object _fileSizeLock = new();
    private readonly LRUCache<uint, Page> _lruCache;
    private readonly Action<TinyDbLogLevel, string, Exception?> _log;
    private const int PageLoadStripeCount = 64;
    private const int CacheOverflowSlack = 4096;
    private const int BackgroundWritebackMinBatchSize = 16;
    private const int BackgroundWritebackMaxBatchSize = 256;
    private readonly SemaphoreSlim[] _pageLoadStripes;
    private uint _nextPageID;
    private uint _firstFreePageID; // Head of free page linked list
    private uint _freePageCount;
    private long _fileSize;
    private bool _disposed;
    private Exception? _corruptionException;
    private readonly SemaphoreSlim _backgroundWritebackGate = new(1, 1);
    private readonly ManualResetEventSlim _backgroundWritebackIdle = new(true);
    private int _backgroundWritebackScheduled;

    private Action<long, WriteAheadLog.WriteLockContext?>? _flushLogToLsn;
    private Func<long, WriteAheadLog.WriteLockContext?, CancellationToken, Task>? _flushLogToLsnAsync;
    private Action<Page, byte[]?, WriteAheadLog.WriteLockContext?>? _appendLogPage;
    private Action<Page, byte[]?, WriteAheadLog.WriteLockContext?>? _appendDeferredLogPage;
    private Func<Page, byte[]?, WriteAheadLog.WriteLockContext?, CancellationToken, Task>? _appendLogPageAsync;
    private Func<bool>? _requiresWalBeforeImage;
    private readonly ConcurrentDictionary<uint, long> _deferredWalPages = new();

    /// <summary>
    /// 注册 WAL 刷新回调
    /// </summary>
    /// <param name="flushLogToLsn">根据 LSN 刷新日志的异步函数</param>
    public void RegisterWAL(Func<long, Task> flushLogToLsnAsync)
    {
        if (flushLogToLsnAsync == null) throw new ArgumentNullException(nameof(flushLogToLsnAsync));
        _flushLogToLsnAsync = (lsn, _, _) => flushLogToLsnAsync(lsn);
    }

    public void RegisterWAL(Func<long, CancellationToken, Task> flushLogToLsnAsync)
    {
        if (flushLogToLsnAsync == null) throw new ArgumentNullException(nameof(flushLogToLsnAsync));
        _flushLogToLsnAsync = (lsn, _, ct) => flushLogToLsnAsync(lsn, ct);
    }

    internal void RegisterWAL(Func<long, WriteAheadLog.WriteLockContext?, CancellationToken, Task> flushLogToLsnAsync)
    {
        _flushLogToLsnAsync = flushLogToLsnAsync ?? throw new ArgumentNullException(nameof(flushLogToLsnAsync));
    }

    public void RegisterWAL(Action<long> flushLogToLsn)
    {
        if (flushLogToLsn == null) throw new ArgumentNullException(nameof(flushLogToLsn));

        _flushLogToLsn = (lsn, _) => flushLogToLsn(lsn);
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

        _appendLogPage = (page, beforeImage, _) => appendLogPage(page, beforeImage);
        _appendDeferredLogPage = (page, beforeImage, _) => appendLogPage(page, beforeImage);
        _flushLogToLsn = (lsn, _) => flushLogToLsn(lsn);
        _requiresWalBeforeImage = requiresBeforeImage;
    }

    internal void RegisterWAL(
        Action<Page, byte[]?, WriteAheadLog.WriteLockContext?> appendLogPage,
        Action<long, WriteAheadLog.WriteLockContext?> flushLogToLsn,
        Func<bool>? requiresBeforeImage = null)
    {
        _appendLogPage = appendLogPage ?? throw new ArgumentNullException(nameof(appendLogPage));
        _appendDeferredLogPage = appendLogPage;
        _flushLogToLsn = flushLogToLsn ?? throw new ArgumentNullException(nameof(flushLogToLsn));
        _requiresWalBeforeImage = requiresBeforeImage;
    }

    internal void RegisterDeferredWAL(Action<Page, byte[]?> appendDeferredLogPage)
    {
        if (appendDeferredLogPage == null) throw new ArgumentNullException(nameof(appendDeferredLogPage));
        _appendDeferredLogPage = (page, beforeImage, _) => appendDeferredLogPage(page, beforeImage);
    }

    internal void RegisterDeferredWAL(Action<Page, byte[]?, WriteAheadLog.WriteLockContext?> appendDeferredLogPage)
    {
        _appendDeferredLogPage = appendDeferredLogPage ?? throw new ArgumentNullException(nameof(appendDeferredLogPage));
    }

    internal void MarkDeferredWalPageLogged(uint pageId, long lsn)
    {
        ThrowIfDisposed();

        var hadDeferredWrite = _deferredWalPages.TryGetValue(pageId, out _);
        if (hadDeferredWrite)
        {
            _deferredWalPages[pageId] = lsn;
        }

        if (_pageCache.TryGetValue(pageId, out var page) && (hadDeferredWrite || page.Header.LSN < 0))
        {
            page.UpdateLsnForWal(lsn);
            page.UpdateChecksum();
            if (page.IsDirty)
            {
                TrackDirtyPage(page);
            }
        }
    }

    internal void RegisterWAL(Func<Page, byte[]?, CancellationToken, Task> appendLogPageAsync)
    {
        if (appendLogPageAsync == null) throw new ArgumentNullException(nameof(appendLogPageAsync));
        _appendLogPageAsync = (page, beforeImage, _, ct) => appendLogPageAsync(page, beforeImage, ct);
    }

    internal void RegisterWAL(Func<Page, byte[]?, WriteAheadLog.WriteLockContext?, CancellationToken, Task> appendLogPageAsync)
    {
        _appendLogPageAsync = appendLogPageAsync ?? throw new ArgumentNullException(nameof(appendLogPageAsync));
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
    public uint FirstFreePageID
    {
        get
        {
            lock (_stateLock)
            {
                return _firstFreePageID;
            }
        }
    }

    /// <summary>
    /// 总页面数量
    /// </summary>
    public uint TotalPages => (uint)(Volatile.Read(ref _fileSize) / _physicalPageSize);

    /// <summary>
    /// 空闲页面数量
    /// </summary>
    public uint FreePageCount
    {
        get
        {
            lock (_stateLock)
            {
                return _freePageCount;
            }
        }
    }

    /// <summary>
    /// 最大缓存页面数
    /// </summary>
    public int MaxCacheSize { get; }

    private int CacheOverflowLimit => GetCacheOverflowLimit(MaxCacheSize);

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
        _dirtyPageIds = new ConcurrentDictionary<uint, byte>();
        _lruCache = new LRUCache<uint, Page>(GetCacheOverflowLimit(maxCacheSize));
        _pageLoadStripes = new SemaphoreSlim[PageLoadStripeCount];
        for (var i = 0; i < _pageLoadStripes.Length; i++)
        {
            _pageLoadStripes[i] = new SemaphoreSlim(1, 1);
        }
        _fileSize = _diskStream.Size;
        _nextPageID = 0;
        _firstFreePageID = 0;
        _freePageCount = 0;
    }

    private void Log(TinyDbLogLevel level, string message, Exception? ex = null)
    {
        TinyDbLogging.SafeLog(_log, level, message, ex);
    }

    private static int GetCacheOverflowLimit(int maxCacheSize)
    {
        return maxCacheSize > int.MaxValue - CacheOverflowSlack
            ? int.MaxValue
            : maxCacheSize + CacheOverflowSlack;
    }

    internal void MarkCorrupted(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        Interlocked.CompareExchange(ref _corruptionException, exception, null);
    }

    private void TrackDirtyPage(Page page)
    {
        _dirtyPageIds[page.PageID] = 0;
    }

    // 在 page._lock 内被回调，与 TrackDirtyPage 互斥，保证 IsDirty 与 _dirtyPageIds 的增删原子一致
    private void UntrackDirtyPage(Page page)
    {
        _dirtyPageIds.TryRemove(page.PageID, out _);
    }

    private void AttachDirtyTracking(Page page)
    {
        page.SetDirtyCallback(TrackDirtyPage, UntrackDirtyPage);
        if (page.IsDirty)
        {
            TrackDirtyPage(page);
        }
    }

    private bool MarkPageClean(Page page, long? dirtyGeneration = null)
    {
        // 移除动作在 page.MarkClean() 内随 IsDirty 翻转一并完成（page._lock 内），不可在此再单独 TryRemove，否则会误删并发写刚加回的条目
        return dirtyGeneration.HasValue
            ? page.MarkCleanIfGeneration(dirtyGeneration.Value)
            : MarkPageCleanWithoutGeneration(page);
    }

    private static bool MarkPageCleanWithoutGeneration(Page page)
    {
        page.MarkClean();
        return true;
    }

    private void RemoveDirtyTracking(Page page)
    {
        page.SetDirtyCallback(null, null);
        _dirtyPageIds.TryRemove(page.PageID, out _);
    }

    /// <summary>
    /// 初始化页面状态（避免全盘扫描）
    /// </summary>
    /// <param name="totalPages">总页面数</param>
    /// <param name="firstFreePageID">第一个空闲页面ID</param>
    public void Initialize(uint totalPages, uint firstFreePageID, uint freePageCount = 0, bool hasFreePageCount = false)
    {
        bool rebuildFreeList;
        bool countExistingFreeList;
        uint nextPageId;
        uint initialFirstFreePageId;
        lock (_stateLock)
        {
            // 如果文件大小不匹配 TotalPages，优先信任文件大小
            var calculatedTotal = (uint)(_fileSize / _physicalPageSize);
            _nextPageID = Math.Max(totalPages, calculatedTotal);
            _firstFreePageID = firstFreePageID;
            _freePageCount = _firstFreePageID == 0
                ? 0
                : ClampFreePageCount(freePageCount, _nextPageID);
            nextPageId = _nextPageID;
            initialFirstFreePageId = _firstFreePageID;

            // 关键修复：如果 _firstFreePageID 为 0 但文件中有页面，
            // 可能是由于非正常关闭导致的空闲链表丢失，执行一次快速扫描恢复
            rebuildFreeList = _firstFreePageID == 0 &&
                              _nextPageID > 1 &&
                              (!hasFreePageCount || freePageCount > 0);
            countExistingFreeList = !rebuildFreeList &&
                                    _firstFreePageID != 0 &&
                                    (!hasFreePageCount || _freePageCount == 0);
        }

        if (rebuildFreeList)
        {
            var (rebuiltFirstFreePageId, countedFreePages) = ScanFreePages(nextPageId);
            lock (_stateLock)
            {
                if (_firstFreePageID == initialFirstFreePageId)
                {
                    _firstFreePageID = rebuiltFirstFreePageId;
                    _freePageCount = countedFreePages;
                }
            }
        }
        else if (countExistingFreeList)
        {
            var countedFreePages = CountFreePages(initialFirstFreePageId, nextPageId);
            lock (_stateLock)
            {
                if (_firstFreePageID == initialFirstFreePageId)
                {
                    _freePageCount = countedFreePages;
                }
            }
        }
    }

    private static uint ClampFreePageCount(uint freePageCount, uint nextPageId)
    {
        var maxFreePages = nextPageId > 1 ? nextPageId - 1 : 0;
        return Math.Min(freePageCount, maxFreePages);
    }

    private (uint FirstFreePageId, uint FreePageCount) ScanFreePages(uint nextPageId)
    {
        var freePageIds = new List<uint>();
        uint skippedPages = 0;
        for (uint i = 2; i <= nextPageId; i++)
        {
            try
            {
                var pageOffset = CalculatePageOffset(i);
                var pageData = ReadLogicalPageData(i, pageOffset);
                var header = PageHeader.FromSpan(pageData);
                if (header.PageType == PageType.Empty)
                {
                    freePageIds.Add(i);
                }
            }
            catch (Exception ex)
            {
                // Corrupted pages are not safe to add to the free list during recovery scanning.
                skippedPages++;
                Log(TinyDbLogLevel.Warning, $"Skipping page {i} while rebuilding the free list.", ex);
            }
        }

        for (var index = 0; index < freePageIds.Count; index++)
        {
            var pageId = freePageIds[index];
            var nextFreePageId = index + 1 < freePageIds.Count ? freePageIds[index + 1] : 0;
            try
            {
                WriteFreePageLink(pageId, nextFreePageId);
            }
            catch (Exception ex)
            {
                skippedPages++;
                Log(TinyDbLogLevel.Warning, $"Skipping page {pageId} while rebuilding the free list.", ex);
            }
        }

        if (skippedPages > 0)
        {
            Log(
                TinyDbLogLevel.Warning,
                $"Free list rebuild skipped {skippedPages} unreadable page(s). The pages were left allocated to avoid reusing corrupted data; run CompactDatabase to reclaim space.");
        }

        var firstFreePageId = freePageIds.Count > 0 ? freePageIds[0] : 0;
        var freePageCount = (uint)freePageIds.Count;
        return (firstFreePageId, freePageCount);
    }

    private uint CountFreePages(uint firstFreePageId, uint nextPageId)
    {
        uint count = 0;
        var current = firstFreePageId;
        var visited = new HashSet<uint>();

        while (current != 0 && current <= nextPageId && visited.Add(current))
        {
            count++;
            try
            {
                var pageOffset = CalculatePageOffset(current);
                var pageData = ReadLogicalPageData(current, pageOffset);
                var header = PageHeader.FromByteArray(pageData);
                current = header.NextPageID;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to traverse free-page list at page {current}.", ex);
            }
        }

        return count;
    }

    private void WriteFreePageLink(uint pageId, uint nextPageId)
    {
        var pageOffset = CalculatePageOffset(pageId);
        var pageData = ReadLogicalPageData(pageId, pageOffset);
        var header = PageHeader.FromSpan(pageData);
        if (header.PageType != PageType.Empty || header.NextPageID == nextPageId)
        {
            return;
        }

        header.NextPageID = nextPageId;
        header.Checksum = 0;
        header.WriteTo(pageData);
        header.Checksum = TinyCrc32.HashToUInt32WithZeroedRange(pageData, 21, sizeof(uint));
        header.WriteTo(pageData);

        WriteEncodedPageToDisk(pageId, pageOffset, pageData);
        RemoveFromCache(pageId);
    }

    /// <summary>
    /// 获取页面使用统计
    /// </summary>
    /// <returns>页面统计信息</returns>
    public PageManagerStatistics GetStatistics()
    {
        ThrowIfDisposed();

        var dirtyPages = CountDirtyPages();
        uint freeCount;
        uint nextPageId;
        long fileSize;
        lock (_stateLock)
        {
            freeCount = _freePageCount;
            nextPageId = _nextPageID;
            fileSize = _fileSize;
        }

        var totalPages = (uint)(fileSize / _physicalPageSize);
        var maxFreePages = totalPages;
        freeCount = Math.Min(freeCount, maxFreePages);
        var usedPages = totalPages > freeCount ? totalPages - freeCount : 0;

        return new PageManagerStatistics
        {
            PageSize = _pageSize,
            TotalPages = totalPages,
            UsedPages = usedPages,
            FreePages = freeCount,
            CachedPages = _pageCache.Count,
            DirtyPages = dirtyPages,
            MaxCacheSize = MaxCacheSize,
            CacheHitRatio = _lruCache.HitRatio,
            FileSize = fileSize,
            NextPageID = nextPageId
        };
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
        if (useCache && TryGetCachedPage(pageID, pin: false, out var cachedPage))
        {
            return cachedPage;
        }

        var loadGate = GetPageLoadGate(pageID);
        loadGate.Wait();
        try
        {
            if (useCache && TryGetCachedPage(pageID, pin: false, out cachedPage))
            {
                return cachedPage;
            }

            // 从磁盘读取
            var pageOffset = CalculatePageOffset(pageID);

            // 检查是否超出文件大小
            if (pageOffset + _physicalPageSize > Volatile.Read(ref _fileSize))
            {
                // 文件不存在该页面，创建新页面
                return CreateNewPage(pageID, PageType.Empty);
            }

            try
            {
                var read = ReadLogicalPageDataOrNull(pageID, pageOffset);
                if (read == null)
                {
                    return CreateNewPage(pageID, PageType.Empty);
                }

                var pageData = read.Value.Data;
                var page = CreateLoadedPage(pageID, read.Value);
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
        finally
        {
            loadGate.Release();
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

    private bool TryGetCachedPage(uint pageID, bool pin, out Page page)
    {
        while (_pageCache.TryGetValue(pageID, out var candidate))
        {
            if (pin)
            {
                candidate.Pin();
            }

            if (_pageCache.TryGetValue(pageID, out var current) &&
                ReferenceEquals(current, candidate))
            {
                _lruCache.TryTouch(pageID);
                page = candidate;
                return true;
            }

            if (pin)
            {
                candidate.Unpin();
            }
        }

        page = null!;
        return false;
    }

    private SemaphoreSlim GetPageLoadGate(uint pageID)
    {
        return _pageLoadStripes[(int)(pageID % PageLoadStripeCount)];
    }

    internal bool TryReadLogicalPageSnapshot(uint pageID, out byte[] pageData)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));

        var pageOffset = CalculatePageOffset(pageID);
        if (pageOffset + _physicalPageSize > Volatile.Read(ref _fileSize))
        {
            pageData = Array.Empty<byte>();
            return false;
        }

        var read = ReadLogicalPageDataOrNull(pageID, pageOffset);
        if (read == null)
        {
            pageData = Array.Empty<byte>();
            return false;
        }

        pageData = read.Value.Data;
        return true;
    }

    internal Page GetPagePinned(uint pageID, bool useCache = true)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));

        if (useCache && TryGetCachedPage(pageID, pin: true, out var cachedPage))
        {
            return cachedPage;
        }

        var loadGate = GetPageLoadGate(pageID);
        loadGate.Wait();
        try
        {
            if (useCache && TryGetCachedPage(pageID, pin: true, out cachedPage))
            {
                return cachedPage;
            }

            var pageOffset = CalculatePageOffset(pageID);
            if (pageOffset + _physicalPageSize > Volatile.Read(ref _fileSize))
            {
                return CreateNewPage(pageID, PageType.Empty, pinned: true);
            }

            try
            {
                var read = ReadLogicalPageDataOrNull(pageID, pageOffset);
                if (read == null)
                {
                    return CreateNewPage(pageID, PageType.Empty, pinned: true);
                }

                var pageData = read.Value.Data;
                var page = CreateLoadedPage(pageID, read.Value);
                ValidateLoadedPage(page, pageData, pageOffset);

                if (page.Header.PageType == PageType.Empty && page.Header.ItemCount == 0)
                {
                    page.UpdatePageType(PageType.Empty);
                }

                if (useCache)
                {
                    return AddToCache(page, pinned: true);
                }

                page.Pin();
                return page;
            }
            catch (Exception ex)
            {
                throw new InvalidDataException($"Failed to load page {pageID} at offset {pageOffset}.", ex);
            }
        }
        finally
        {
            loadGate.Release();
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
        if (useCache && TryGetCachedPage(pageID, pin: false, out var cachedPage))
        {
            return cachedPage;
        }

        var loadGate = GetPageLoadGate(pageID);
        await loadGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (useCache && TryGetCachedPage(pageID, pin: false, out cachedPage))
            {
                return cachedPage;
            }

            // 从磁盘异步读取
            var pageOffset = CalculatePageOffset(pageID);
            if (pageOffset + _physicalPageSize > Volatile.Read(ref _fileSize))
            {
                return CreateNewPage(pageID, PageType.Empty);
            }

            try
            {
                var read = await ReadLogicalPageDataOrNullAsync(pageID, pageOffset, cancellationToken).ConfigureAwait(false);
                if (read == null)
                {
                    return CreateNewPage(pageID, PageType.Empty);
                }

                var pageData = read.Value.Data;
                var page = CreateLoadedPage(pageID, read.Value);
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
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new InvalidDataException($"Failed to parse page {pageID} at offset {pageOffset}.", ex);
            }
        }
        finally
        {
            loadGate.Release();
        }
    }

    internal async Task<Page> GetPagePinnedAsync(uint pageID, bool useCache = true, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));

        if (useCache && TryGetCachedPage(pageID, pin: true, out var cachedPage))
        {
            return cachedPage;
        }

        var loadGate = GetPageLoadGate(pageID);
        await loadGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (useCache && TryGetCachedPage(pageID, pin: true, out cachedPage))
            {
                return cachedPage;
            }

            var pageOffset = CalculatePageOffset(pageID);
            if (pageOffset + _physicalPageSize > Volatile.Read(ref _fileSize))
            {
                return CreateNewPage(pageID, PageType.Empty, pinned: true);
            }

            try
            {
                var read = await ReadLogicalPageDataOrNullAsync(pageID, pageOffset, cancellationToken).ConfigureAwait(false);
                if (read == null)
                {
                    return CreateNewPage(pageID, PageType.Empty, pinned: true);
                }

                var pageData = read.Value.Data;
                var page = CreateLoadedPage(pageID, read.Value);
                ValidateLoadedPage(page, pageData, pageOffset);

                if (page.Header.PageType == PageType.Empty && page.Header.ItemCount == 0)
                {
                    page.UpdatePageType(PageType.Empty);
                }

                if (useCache)
                {
                    return AddToCache(page, pinned: true);
                }

                page.Pin();
                return page;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                throw new InvalidDataException($"Failed to parse page {pageID} at offset {pageOffset}.", ex);
            }
        }
        finally
        {
            loadGate.Release();
        }
    }

    private static bool IsZeroFilledPage(ReadOnlySpan<byte> pageData)
    {
        for (int i = 0; i < pageData.Length; i++)
        {
            if (pageData[i] != 0) return false;
        }

        return true;
    }

    private readonly struct LogicalPageRead
    {
        public LogicalPageRead(byte[] data, bool ownsBuffer)
        {
            Data = data;
            OwnsBuffer = ownsBuffer;
        }

        public byte[] Data { get; }
        public bool OwnsBuffer { get; }
    }

    private static Page CreateLoadedPage(uint pageID, LogicalPageRead read)
    {
        return read.OwnsBuffer
            ? Page.FromOwnedBuffer(pageID, read.Data)
            : new Page(pageID, read.Data);
    }

    private LogicalPageRead? ReadLogicalPageDataOrNull(uint pageID, long pageOffset)
    {
        var logicalLength = (int)_pageSize;

        if (CanWriteLogicalPageDirectly() && _diskStream is DiskStream directDiskStream)
        {
            var logicalPage = new byte[logicalLength];
            directDiskStream.ReadPage(pageOffset, logicalPage);
            return IsZeroFilledPage(logicalPage) ? null : new LogicalPageRead(logicalPage, ownsBuffer: true);
        }

        if (_diskStream is DiskStream diskStream)
        {
            var frameLength = (int)_physicalPageSize;
            var buffer = ArrayPool<byte>.Shared.Rent(frameLength);
            var frame = buffer.AsSpan(0, frameLength);
            try
            {
                diskStream.ReadPage(pageOffset, frame);
                if (IsZeroFilledPage(frame))
                {
                    return null;
                }

                var logicalPage = new byte[logicalLength];
                _pageCodec.DecodeTo(pageID, frame, logicalPage);
                return new LogicalPageRead(logicalPage, ownsBuffer: true);
            }
            finally
            {
                if (_pageCodec.IsEncrypted)
                {
                    CryptographicOperations.ZeroMemory(frame);
                }

                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        var fallbackFrame = _diskStream.ReadPage(pageOffset, (int)_physicalPageSize);
        if (IsZeroFilledPage(fallbackFrame))
        {
            return null;
        }

        return new LogicalPageRead(_pageCodec.Decode(pageID, fallbackFrame), ownsBuffer: false);
    }

    private async Task<LogicalPageRead?> ReadLogicalPageDataOrNullAsync(
        uint pageID,
        long pageOffset,
        CancellationToken cancellationToken)
    {
        var logicalLength = (int)_pageSize;

        if (CanWriteLogicalPageDirectly() && _diskStream is DiskStream directDiskStream)
        {
            var logicalPage = new byte[logicalLength];
            await directDiskStream.ReadPageAsync(pageOffset, logicalPage, cancellationToken).ConfigureAwait(false);
            return IsZeroFilledPage(logicalPage) ? null : new LogicalPageRead(logicalPage, ownsBuffer: true);
        }

        if (_diskStream is DiskStream diskStream)
        {
            var frameLength = (int)_physicalPageSize;
            var buffer = ArrayPool<byte>.Shared.Rent(frameLength);
            var frame = buffer.AsMemory(0, frameLength);
            try
            {
                await diskStream.ReadPageAsync(pageOffset, frame, cancellationToken).ConfigureAwait(false);
                if (IsZeroFilledPage(frame.Span))
                {
                    return null;
                }

                var logicalPage = new byte[logicalLength];
                _pageCodec.DecodeTo(pageID, frame.Span, logicalPage);
                return new LogicalPageRead(logicalPage, ownsBuffer: true);
            }
            finally
            {
                if (_pageCodec.IsEncrypted)
                {
                    CryptographicOperations.ZeroMemory(frame.Span);
                }

                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        var fallbackFrame = await _diskStream.ReadPageAsync(pageOffset, (int)_physicalPageSize, cancellationToken).ConfigureAwait(false);
        if (IsZeroFilledPage(fallbackFrame))
        {
            return null;
        }

        return new LogicalPageRead(_pageCodec.Decode(pageID, fallbackFrame), ownsBuffer: false);
    }

    private byte[] ReadLogicalPageData(uint pageID, long pageOffset)
    {
        var read = ReadLogicalPageDataOrNull(pageID, pageOffset);
        if (read == null)
        {
            var page = new Page(pageID, (int)_pageSize, PageType.Empty);
            return page.Buffer;
        }

        return read.Value.Data;
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
            var freePageId = _firstFreePageID;
            if (freePageId != 0)
            {
                var freePage = GetPage(freePageId);
                lock (_stateLock)
                {
                    _firstFreePageID = freePage.Header.NextPageID;
                    if (_freePageCount > 0)
                    {
                        _freePageCount--;
                    }
                }

                freePage.ClearData();
                freePage.UpdatePageType(pageType);
                freePage.SetLinks(0, 0);

                return freePage;
            }
        }

        uint pageID;
        lock (_stateLock)
        {
            pageID = ++_nextPageID;
        }

        return CreateNewPage(pageID, pageType);
    }

    internal Page NewPagePinned(PageType pageType)
    {
        ThrowIfDisposed();

        lock (_freeListLock)
        {
            var freePageId = _firstFreePageID;
            if (freePageId != 0)
            {
                var freePage = GetPagePinned(freePageId);
                lock (_stateLock)
                {
                    _firstFreePageID = freePage.Header.NextPageID;
                    if (_freePageCount > 0)
                    {
                        _freePageCount--;
                    }
                }

                freePage.ClearData();
                freePage.UpdatePageType(pageType);
                freePage.SetLinks(0, 0);

                return freePage;
            }
        }

        uint pageID;
        lock (_stateLock)
        {
            pageID = ++_nextPageID;
        }

        return CreateNewPage(pageID, pageType, pinned: true);
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
        page = AddToCache(page, pinned);

        // 计算新的文件大小
        var newFileSize = CalculatePageOffset(pageID) + _physicalPageSize;

        EnsureFileLength(newFileSize);

        return page;
    }

    private void EnsureFileLength(long newFileSize)
    {
        lock (_fileSizeLock)
        {
            if (newFileSize > Volatile.Read(ref _fileSize))
            {
                _diskStream.SetLength(newFileSize);
                Volatile.Write(ref _fileSize, newFileSize);
            }
        }
    }

    /// <summary>
    /// 保存页面
    /// </summary>
    /// <param name="page">要保存的页面</param>
    /// <param name="forceFlush">是否强制刷新到磁盘</param>
    public void SavePage(Page page, bool forceFlush = false)
    {
        SavePage(page, forceFlush, walContext: null);
    }

    internal void SavePage(Page page, bool forceFlush, WriteAheadLog.WriteLockContext? walContext)
    {
        ThrowIfDisposed();
        if (page == null) throw new ArgumentNullException(nameof(page));

        // WAL 检查：日志必须先于数据落盘
        byte[]? beforeImage = null;
        if (_appendLogPage != null && _requiresWalBeforeImage?.Invoke() == true)
        {
            beforeImage = ReadPageSnapshotForWal(page.PageID);
        }

        _appendLogPage?.Invoke(page, beforeImage, walContext);
        if (_flushLogToLsn != null && page.Header.LSN >= 0)
        {
            _flushLogToLsn(page.Header.LSN, walContext);
        }

        WritePageToDisk(page, forceFlush);
    }

    internal byte[]? CaptureBeforeImageForWal(Page page)
    {
        ThrowIfDisposed();
        if (page == null) throw new ArgumentNullException(nameof(page));

        if ((_appendLogPage == null && _appendDeferredLogPage == null) || _requiresWalBeforeImage?.Invoke() != true)
        {
            return null;
        }

        return page.Snapshot(includeUnusedTail: true);
    }

    internal void SavePageDeferred(Page page, byte[]? beforeImage)
    {
        ThrowIfDisposed();
        if (page == null) throw new ArgumentNullException(nameof(page));

        var appendDeferredLogPage = _appendDeferredLogPage ?? _appendLogPage;
        if (appendDeferredLogPage == null)
        {
            SavePage(page, forceFlush: false);
            return;
        }

        appendDeferredLogPage(page, beforeImage, null);
        _deferredWalPages[page.PageID] = page.Header.LSN;

        if (page.IsDirty)
        {
            TrackDirtyPage(page);
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
        await SavePageAsync(page, forceFlush, walContext: null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task SavePageAsync(
        Page page,
        bool forceFlush,
        WriteAheadLog.WriteLockContext? walContext,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (page == null) throw new ArgumentNullException(nameof(page));
        cancellationToken.ThrowIfCancellationRequested();

        byte[]? beforeImage = null;
        if ((_appendLogPage != null || _appendLogPageAsync != null) && _requiresWalBeforeImage?.Invoke() == true)
        {
            beforeImage = await ReadPageSnapshotForWalAsync(page.PageID, cancellationToken).ConfigureAwait(false);
        }

        if (_appendLogPageAsync != null)
        {
            await _appendLogPageAsync(page, beforeImage, walContext, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            _appendLogPage?.Invoke(page, beforeImage, walContext);
        }

        if (_flushLogToLsnAsync != null && page.Header.LSN >= 0)
        {
            await _flushLogToLsnAsync(page.Header.LSN, walContext, cancellationToken).ConfigureAwait(false);
        }
        else if (_flushLogToLsn != null && page.Header.LSN >= 0)
        {
            _flushLogToLsn(page.Header.LSN, walContext);
        }

        page.Pin();
        try
        {
            var snapshot = page.SnapshotForDiskWrite(out var dirtyGeneration);
            var pageOffset = CalculatePageOffset(page.PageID);

            await WriteEncodedPageToDiskAsync(page.PageID, pageOffset, snapshot, cancellationToken).ConfigureAwait(false);

            if (forceFlush)
            {
                await _diskStream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }

            if (MarkPageClean(page, dirtyGeneration))
            {
                _deferredWalPages.TryRemove(page.PageID, out _);
            }
        }
        finally
        {
            page.Unpin();
        }
    }

    private void WritePageToDisk(Page page, bool forceFlush)
    {
        var pageOffset = CalculatePageOffset(page.PageID);
        var wroteCleanPage = page.WriteForDiskWithoutSnapshot(
            logicalPage => WriteEncodedPageToDisk(page.PageID, pageOffset, logicalPage));

        if (forceFlush)
        {
            _diskStream.Flush();
        }

        if (wroteCleanPage)
        {
            _deferredWalPages.TryRemove(page.PageID, out _);
        }
    }

    private bool CanWriteLogicalPageDirectly()
    {
        return !_pageCodec.IsEncrypted && _pageCodec.PhysicalPageSize == _pageCodec.LogicalPageSize;
    }

    private void WriteEncodedPageToDisk(uint pageId, long pageOffset, byte[] logicalPage)
    {
        if (CanWriteLogicalPageDirectly())
        {
            _diskStream.WritePage(pageOffset, logicalPage);
            return;
        }

        if (_diskStream is DiskStream diskStream)
        {
            var frameLength = (int)_pageCodec.PhysicalPageSize;
            var buffer = ArrayPool<byte>.Shared.Rent(frameLength);
            var frame = buffer.AsSpan(0, frameLength);
            try
            {
                _pageCodec.EncodeTo(pageId, logicalPage, frame);
                diskStream.WritePage(pageOffset, frame);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(frame);
                ArrayPool<byte>.Shared.Return(buffer);
            }

            return;
        }

        _diskStream.WritePage(pageOffset, _pageCodec.Encode(pageId, logicalPage));
    }

    private async Task WriteEncodedPageToDiskAsync(
        uint pageId,
        long pageOffset,
        byte[] logicalPage,
        CancellationToken cancellationToken)
    {
        if (CanWriteLogicalPageDirectly())
        {
            await _diskStream.WritePageAsync(pageOffset, logicalPage, cancellationToken).ConfigureAwait(false);
            return;
        }

        if (_diskStream is DiskStream diskStream)
        {
            var frameLength = (int)_pageCodec.PhysicalPageSize;
            var buffer = ArrayPool<byte>.Shared.Rent(frameLength);
            var frame = buffer.AsMemory(0, frameLength);
            try
            {
                _pageCodec.EncodeTo(pageId, logicalPage, frame.Span);
                await diskStream.WritePageAsync(pageOffset, frame, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                CryptographicOperations.ZeroMemory(frame.Span);
                ArrayPool<byte>.Shared.Return(buffer);
            }

            return;
        }

        await _diskStream.WritePageAsync(pageOffset, _pageCodec.Encode(pageId, logicalPage), cancellationToken).ConfigureAwait(false);
    }

    private bool TryWriteDeferredWalPage(Page page)
    {
        if (!_deferredWalPages.TryGetValue(page.PageID, out var loggedLsn))
        {
            return false;
        }

        var currentLsn = page.Header.LSN;
        if (loggedLsn < 0 || currentLsn != loggedLsn)
        {
            return false;
        }

        WritePageToDisk(page, forceFlush: false);

        return true;
    }

    private async Task<bool> TryWriteDeferredWalPageAsync(Page page, CancellationToken cancellationToken)
    {
        if (!_deferredWalPages.TryGetValue(page.PageID, out var loggedLsn))
        {
            return false;
        }

        var currentLsn = page.Header.LSN;
        if (loggedLsn < 0 || currentLsn != loggedLsn)
        {
            return false;
        }

        page.Pin();
        try
        {
            var snapshot = page.SnapshotForDiskWrite(out var dirtyGeneration);
            var pageOffset = CalculatePageOffset(page.PageID);

            await WriteEncodedPageToDiskAsync(page.PageID, pageOffset, snapshot, cancellationToken).ConfigureAwait(false);
            if (MarkPageClean(page, dirtyGeneration))
            {
                _deferredWalPages.TryRemove(page.PageID, out _);
            }
        }
        finally
        {
            page.Unpin();
        }
        return true;
    }

    private byte[]? ReadPageSnapshotForWal(uint pageID)
    {
        var pageOffset = CalculatePageOffset(pageID);
        if (pageOffset + _physicalPageSize > Volatile.Read(ref _fileSize))
        {
            return null;
        }

        return ReadLogicalPageDataOrNull(pageID, pageOffset)?.Data;
    }

    private async Task<byte[]?> ReadPageSnapshotForWalAsync(uint pageID, CancellationToken cancellationToken)
    {
        var pageOffset = CalculatePageOffset(pageID);
        if (pageOffset + _physicalPageSize > Volatile.Read(ref _fileSize))
        {
            return null;
        }

        return (await ReadLogicalPageDataOrNullAsync(pageID, pageOffset, cancellationToken).ConfigureAwait(false))?.Data;
    }

    /// <summary>
    /// 释放页面
    /// </summary>
    /// <param name="pageID">页面ID</param>
    public void FreePage(uint pageID)
    {
        ThrowIfDisposed();
        if (pageID == 0) throw new ArgumentException("Page ID cannot be zero", nameof(pageID));

        lock (_freeListLock)
        {
            var page = GetPage(pageID);
            byte[]? beforeImage = null;
            if (_appendLogPage != null && _requiresWalBeforeImage?.Invoke() == true)
            {
                beforeImage = ReadPageSnapshotForWal(pageID);
            }

            uint nextFreePageId;
            lock (_stateLock)
            {
                nextFreePageId = _firstFreePageID;
            }

            page.ClearData();
            page.UpdatePageType(PageType.Empty);
            page.SetLinks(0, nextFreePageId);
            page.Header.ItemCount = 0;
            page.Header.FreeBytes = (ushort)(page.DataSize);

            _appendLogPage?.Invoke(page, beforeImage, null);
            if (_flushLogToLsn != null && page.Header.LSN >= 0)
            {
                _flushLogToLsn(page.Header.LSN, null);
            }

            WritePageToDisk(page, forceFlush: false);
            lock (_stateLock)
            {
                _firstFreePageID = pageID;
                _freePageCount++;
            }
        }
    }

    /// <summary>
    /// 刷新所有脏页面到磁盘
    /// </summary>
    public void FlushDirtyPages()
    {
        ThrowIfDisposed();
        FlushDirtyPagesCore(walContext: null);
    }

    internal void FlushDirtyPages(WriteAheadLog.WriteLockContext walContext)
    {
        ThrowIfDisposed();
        FlushDirtyPagesCore(walContext);
    }

    private void FlushDirtyPagesCore(WriteAheadLog.WriteLockContext? walContext)
    {
        var dirtyPageIds = _dirtyPageIds.Keys.ToArray();
        foreach (var pageId in dirtyPageIds)
        {
            if (!TryGetDirtyCachedPage(pageId, out var page))
            {
                continue;
            }

            if (TryWriteDeferredWalPage(page))
            {
                continue;
            }

            if (IsDeferredWalPagePending(page))
            {
                continue;
            }

            SavePage(page, forceFlush: false, walContext);
        }

        _diskStream.Flush();
    }

    private bool IsDeferredWalPagePending(Page page)
    {
        if (!_deferredWalPages.TryGetValue(page.PageID, out var loggedLsn))
        {
            return false;
        }

        var currentLsn = page.Header.LSN;
        return loggedLsn < 0 || currentLsn != loggedLsn;
    }

    /// <summary>
    /// 异步刷新所有脏页面到磁盘
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    public async Task FlushDirtyPagesAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await FlushDirtyPagesAsyncCore(walContext: null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task FlushDirtyPagesAsync(
        WriteAheadLog.WriteLockContext walContext,
        CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await FlushDirtyPagesAsyncCore(walContext, cancellationToken).ConfigureAwait(false);
    }

    private async Task FlushDirtyPagesAsyncCore(
        WriteAheadLog.WriteLockContext? walContext,
        CancellationToken cancellationToken)
    {
        var dirtyPageIds = _dirtyPageIds.Keys.ToArray();
        foreach (var pageId in dirtyPageIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!TryGetDirtyCachedPage(pageId, out var page))
            {
                continue;
            }

            if (await TryWriteDeferredWalPageAsync(page, cancellationToken).ConfigureAwait(false))
            {
                continue;
            }

            if (IsDeferredWalPagePending(page))
            {
                continue;
            }

            await SavePageAsync(page, forceFlush: false, walContext, cancellationToken).ConfigureAwait(false);
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
        lock (_stateLock)
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
            WriteEncodedPageToDisk(pageID, pageOffset, buffer);
            _diskStream.Flush();

            RemoveFromCache(pageID);

            Volatile.Write(ref _fileSize, Math.Max(_fileSize, pageOffset + _physicalPageSize));
        }
    }

    /// <summary>
    /// 判断是否有脏页面
    /// </summary>
    public bool HasDirtyPages()
    {
        ThrowIfDisposed();

        foreach (var pageId in _dirtyPageIds.Keys)
        {
            if (TryGetDirtyCachedPage(pageId, out _))
            {
                return true;
            }
        }

        return false;
    }

    private bool TryGetDirtyCachedPage(uint pageId, out Page page)
    {
        if (_pageCache.TryGetValue(pageId, out page!))
        {
            if (page.IsDirty)
            {
                return true;
            }

            // 页仍在缓存但已不脏：移除交给 page.MarkClean()（page._lock 内）完成，此处不主动 TryRemove，
            // 否则可能误删并发 MarkDirty 刚加回的条目，重新引入脏页丢失竞态。
            return false;
        }

        // 页已不在缓存（被驱逐/移除），不可能再被并发置脏，安全地清理残留 id。
        _dirtyPageIds.TryRemove(pageId, out _);
        return false;
    }

    private int CountDirtyPages()
    {
        var count = 0;
        foreach (var pageId in _dirtyPageIds.Keys)
        {
            if (TryGetDirtyCachedPage(pageId, out _))
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>
    /// 清理缓存
    /// </summary>
    /// <param name="maxPagesToKeep">保留的最大页面数</param>
    public void ClearCache(int maxPagesToKeep = 0)
    {
        ThrowIfDisposed();
        ClearCacheCore(maxPagesToKeep);
    }

    private void ClearCacheCore(int maxPagesToKeep = 0)
    {
        lock (_cacheLock)
        {
            if (maxPagesToKeep <= 0)
            {
                // 清空所有缓存
                foreach (var pageID in _pageCache.Keys)
                {
                    TryRemoveCachedPageForClear(pageID);
                }
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
                        TryRemoveCachedPageForClear(pageID);
                    }
                }
            }
        }
    }

    private bool TryRemoveCachedPageForClear(uint pageID)
    {
        if (!_pageCache.TryGetValue(pageID, out var page))
        {
            return false;
        }

        if (page.PinCount > 0)
        {
            return false;
        }

        if (!_pageCache.TryRemove(pageID, out var removedPage))
        {
            return false;
        }

        RemoveDirtyTracking(removedPage);
        _lruCache.Remove(pageID);
        removedPage.Dispose();
        return true;
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
    private Page AddToCache(Page page, bool pinned = false)
    {
        while (true)
        {
            lock (_cacheLock)
            {
                if (_pageCache.TryGetValue(page.PageID, out var cachedPage))
                {
                    if (pinned)
                    {
                        cachedPage.Pin();
                    }

                    _lruCache.TryTouch(page.PageID);
                    return cachedPage;
                }
            }

            // 如果缓存已满，移除最少使用的页面
            if (_pageCache.Count >= MaxCacheSize && !EvictLeastRecentlyUsed())
            {
                ScheduleBackgroundWriteback();
                if (_pageCache.Count >= CacheOverflowLimit)
                {
                    _backgroundWritebackIdle.Wait(TimeSpan.FromSeconds(5));
                    var evicted = EvictLeastRecentlyUsed(_pageCache.Count);
                    if (!evicted && _pageCache.Count >= CacheOverflowLimit)
                    {
                        throw new InvalidOperationException(
                            $"Page cache is full ({MaxCacheSize} pages, overflow limit {CacheOverflowLimit}) and no clean unpinned page is available for eviction.");
                    }
                }
            }

            lock (_cacheLock)
            {
                if (_pageCache.TryAdd(page.PageID, page))
                {
                    AttachDirtyTracking(page);
                    if (pinned)
                    {
                        page.Pin();
                    }

                    _lruCache.Put(page.PageID, page);
                    return page;
                }
            }
        }
    }

    /// <summary>
    /// 驱逐最少使用的页面
    /// </summary>
    private bool EvictLeastRecentlyUsed(int candidateCount = 0)
    {
        // 尝试从 LRU 获取多个候选者，以防第一个被 Pin 住。
        var candidates = _lruCache.GetLeastRecentlyUsed(candidateCount > 0 ? candidateCount : Math.Max(10, MaxCacheSize));
        var sawDirtyPage = false;
        foreach (var pageID in candidates)
        {
            var lockObj = _cacheLock;
            Page? evictionCandidate = null;
            lock (lockObj)
            {
                if (_pageCache.TryGetValue(pageID, out var page))
                {
                    // 如果页面被锁定（PinCount > 0），不能驱逐
                    if (page.PinCount > 0) continue;
                    if (page.IsDirty)
                    {
                        sawDirtyPage = true;
                        continue;
                    }

                    page.Pin();
                    evictionCandidate = page;
                }
                else
                {
                    _lruCache.Remove(pageID);
                }
            }

            if (evictionCandidate == null)
            {
                continue;
            }

            try
            {
                lock (lockObj)
                {
                    if (!_pageCache.TryGetValue(pageID, out var currentPage) ||
                        !ReferenceEquals(currentPage, evictionCandidate))
                    {
                        continue;
                    }

                    if (currentPage.PinCount > 1 || currentPage.IsDirty)
                    {
                        if (currentPage.IsDirty)
                        {
                            sawDirtyPage = true;
                        }

                        continue;
                    }

                    if (!_pageCache.TryRemove(pageID, out var removedPage))
                    {
                        continue;
                    }

                    RemoveDirtyTracking(removedPage);
                    _lruCache.Remove(pageID);
                    return true; // 成功驱逐一个，退出
                }
            }
            finally
            {
                evictionCandidate.Unpin();
            }
        }

        if (sawDirtyPage)
        {
            ScheduleBackgroundWriteback();
        }

        return false;
    }

    private void ScheduleBackgroundWriteback()
    {
        if (_disposed)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref _backgroundWritebackScheduled, 1, 0) != 0)
        {
            return;
        }

        _backgroundWritebackIdle.Reset();
        _ = RunBackgroundWritebackAsync();
    }

    private async Task RunBackgroundWritebackAsync()
    {
        try
        {
            await _backgroundWritebackGate.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_disposed)
                {
                    return;
                }

                await WritebackDirtyPagesForEvictionAsync(CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                _backgroundWritebackGate.Release();
            }
        }
        catch (ObjectDisposedException) when (_disposed)
        {
        }
        catch (Exception ex)
        {
            Log(TinyDbLogLevel.Warning, "Background page writeback failed.", ex);
        }
        finally
        {
            Interlocked.Exchange(ref _backgroundWritebackScheduled, 0);
            _backgroundWritebackIdle.Set();
        }
    }

    private async Task WritebackDirtyPagesForEvictionAsync(CancellationToken cancellationToken)
    {
        var targetWrites = GetBackgroundWritebackTarget();
        var candidateCount = GetBackgroundWritebackCandidateCount(targetWrites);
        if (candidateCount <= 0)
        {
            return;
        }

        var candidates = _lruCache.GetLeastRecentlyUsed(candidateCount);
        var written = 0;
        foreach (var pageID in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (written >= targetWrites)
            {
                break;
            }

            if (!TryPinDirtyPageForBackgroundWriteback(pageID, out var page))
            {
                continue;
            }

            try
            {
                if (await WriteDirtyPageForBackgroundWritebackAsync(page, cancellationToken).ConfigureAwait(false))
                {
                    written++;
                }
            }
            finally
            {
                page.Unpin();
            }
        }

        if (written > 0)
        {
            await _diskStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private int GetBackgroundWritebackTarget()
    {
        return Math.Clamp(MaxCacheSize / 4, BackgroundWritebackMinBatchSize, BackgroundWritebackMaxBatchSize);
    }

    private int GetBackgroundWritebackCandidateCount(int targetWrites)
    {
        var cachedPages = _pageCache.Count;
        if (cachedPages <= 0)
        {
            return 0;
        }

        var desired = Math.Max(targetWrites * 4, Math.Min(MaxCacheSize, BackgroundWritebackMaxBatchSize * 4));
        return Math.Min(cachedPages, desired);
    }

    private bool TryPinDirtyPageForBackgroundWriteback(uint pageID, out Page page)
    {
        page = null!;

        if (!_pageCache.TryGetValue(pageID, out var candidate))
        {
            _lruCache.Remove(pageID);
            return false;
        }

        if (candidate.PinCount > 0 || !candidate.IsDirty)
        {
            return false;
        }

        candidate.Pin();
        if (!_pageCache.TryGetValue(pageID, out var current) ||
            !ReferenceEquals(current, candidate) ||
            candidate.PinCount != 1 ||
            !candidate.IsDirty)
        {
            candidate.Unpin();
            return false;
        }

        page = candidate;
        return true;
    }

    private async Task<bool> WriteDirtyPageForBackgroundWritebackAsync(Page page, CancellationToken cancellationToken)
    {
        if (!page.IsDirty)
        {
            return false;
        }

        if (await TryWriteDeferredWalPageAsync(page, cancellationToken).ConfigureAwait(false))
        {
            return true;
        }

        if (IsDeferredWalPagePending(page) || !page.IsDirty)
        {
            return false;
        }

        await SavePageAsync(page, cancellationToken: cancellationToken).ConfigureAwait(false);
        return true;
    }

    private void RemoveFromCache(uint pageID)
    {
        lock (_cacheLock)
        {
            if (_pageCache.TryRemove(pageID, out var page))
            {
                RemoveDirtyTracking(page);
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

        if (Volatile.Read(ref _corruptionException) is { } corruptionException)
        {
            throw new InvalidOperationException(
                "PageManager is corrupted after a failed compensation rollback. Dispose and reopen the database to recover from WAL.",
                corruptionException);
        }
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
                _backgroundWritebackGate.Wait();
                try
                {
                    try
                    {
                        try
                        {
                            // 刷新所有脏页面
                            if (Volatile.Read(ref _corruptionException) == null)
                            {
                                FlushDirtyPages();
                            }
                        }
                        catch (Exception ex)
                        {
                            flushException = new InvalidOperationException("Flush dirty pages during PageManager dispose failed.", ex);
                        }

                        // 清理缓存
                        ClearCacheCore();

                        foreach (var gate in _pageLoadStripes)
                        {
                            gate.Dispose();
                        }

                        _diskStream.Dispose();
                    }
                    finally
                    {
                        _disposed = true;
                    }
                }
                finally
                {
                    _backgroundWritebackGate.Release();
                }

                _backgroundWritebackIdle.Wait();
            }
            catch (Exception ex)
            {
                cleanupException = new InvalidOperationException("PageManager dispose failed.", ex);
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

