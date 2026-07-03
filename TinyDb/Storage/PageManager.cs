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
    private readonly LRUCache<uint, Page> _lruCache;
    private readonly Action<TinyDbLogLevel, string, Exception?> _log;
    private const int PageLoadStripeCount = 64;
    private readonly SemaphoreSlim[] _pageLoadStripes;
    private uint _nextPageID;
    private uint _firstFreePageID; // Head of free page linked list
    private long _fileSize;
    private bool _disposed;
    private readonly SemaphoreSlim _backgroundWritebackGate = new(1, 1);
    private readonly ManualResetEventSlim _backgroundWritebackIdle = new(true);
    private int _backgroundWritebackScheduled;

    private Action<long>? _flushLogToLsn;
    private Func<long, CancellationToken, Task>? _flushLogToLsnAsync;
    private Action<Page, byte[]?>? _appendLogPage;
    private Action<Page, byte[]?>? _appendDeferredLogPage;
    private Func<Page, byte[]?, CancellationToken, Task>? _appendLogPageAsync;
    private Func<bool>? _requiresWalBeforeImage;
    private readonly ConcurrentDictionary<uint, long> _deferredWalPages = new();

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
        _appendDeferredLogPage = appendLogPage;
        _flushLogToLsn = flushLogToLsn;
        _requiresWalBeforeImage = requiresBeforeImage;
    }

    internal void RegisterDeferredWAL(Action<Page, byte[]?> appendDeferredLogPage)
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
        _dirtyPageIds = new ConcurrentDictionary<uint, byte>();
        _lruCache = new LRUCache<uint, Page>(maxCacheSize);
        _pageLoadStripes = new SemaphoreSlim[PageLoadStripeCount];
        for (var i = 0; i < _pageLoadStripes.Length; i++)
        {
            _pageLoadStripes[i] = new SemaphoreSlim(1, 1);
        }
        _fileSize = _diskStream.Size;
        _nextPageID = 0;
        _firstFreePageID = 0;
    }

    private void Log(TinyDbLogLevel level, string message, Exception? ex = null)
    {
        TinyDbLogging.SafeLog(_log, level, message, ex);
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
    public void Initialize(uint totalPages, uint firstFreePageID)
    {
        bool rebuildFreeList;
        uint nextPageId;
        lock (_stateLock)
        {
            // 如果文件大小不匹配 TotalPages，优先信任文件大小
            var calculatedTotal = (uint)(_fileSize / _physicalPageSize);
            _nextPageID = Math.Max(totalPages, calculatedTotal);
            _firstFreePageID = firstFreePageID;
            nextPageId = _nextPageID;

            // 关键修复：如果 _firstFreePageID 为 0 但文件中有页面，
            // 可能是由于非正常关闭导致的空闲链表丢失，执行一次快速扫描恢复
            rebuildFreeList = _firstFreePageID == 0 && _nextPageID > 1;
        }

        if (rebuildFreeList)
        {
            var rebuiltFirstFreePageId = ScanFreePages(nextPageId);
            lock (_stateLock)
            {
                _firstFreePageID = rebuiltFirstFreePageId;
            }
        }
    }

    private uint ScanFreePages(uint nextPageId)
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

        return freePageIds.Count > 0 ? freePageIds[0] : 0;
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
        uint current;
        uint nextPageId;
        long fileSize;
        lock (_stateLock)
        {
            current = _firstFreePageID;
            nextPageId = _nextPageID;
            fileSize = _fileSize;
        }

        var totalPages = (uint)(fileSize / _physicalPageSize);
            
            // 简单估算：扫描空闲链表长度
            uint freeCount = 0;
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
            if (pageOffset + _physicalPageSize > _fileSize)
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
        lock (_cacheLock)
        {
            if (_pageCache.TryGetValue(pageID, out page!))
            {
                if (pin)
                {
                    page.Pin();
                }

                _lruCache.TryTouch(pageID);
                return true;
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
        if (pageOffset + _physicalPageSize > _fileSize)
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
            if (pageOffset + _physicalPageSize > _fileSize)
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
            if (pageOffset + _physicalPageSize > _fileSize)
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

        lock (_stateLock)
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

    internal Page NewPagePinned(PageType pageType)
    {
        ThrowIfDisposed();

        lock (_stateLock)
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

        // 只有当新文件大小大于当前大小时才更新
        lock (_stateLock)
        {
            if (newFileSize > _fileSize)
            {
                _fileSize = newFileSize;
            // 确保磁盘流的大小也被正确设置
                _diskStream.SetLength(_fileSize);
            }
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
            beforeImage = ReadPageSnapshotForWal(page.PageID);
        }

        _appendLogPage?.Invoke(page, beforeImage);
        if (_flushLogToLsn != null && page.Header.LSN >= 0)
        {
            _flushLogToLsn(page.Header.LSN);
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

        appendDeferredLogPage(page, beforeImage);
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
            await _appendLogPageAsync(page, beforeImage, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            _appendLogPage?.Invoke(page, beforeImage);
        }

        if (_flushLogToLsnAsync != null && page.Header.LSN >= 0)
        {
            await _flushLogToLsnAsync(page.Header.LSN, cancellationToken).ConfigureAwait(false);
        }
        else if (_flushLogToLsn != null && page.Header.LSN >= 0)
        {
            _flushLogToLsn(page.Header.LSN);
        }

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

    private void WritePageToDisk(Page page, bool forceFlush)
    {
        var snapshot = page.SnapshotForDiskWrite(out var dirtyGeneration);

        var pageOffset = CalculatePageOffset(page.PageID);
        WriteEncodedPageToDisk(page.PageID, pageOffset, snapshot);

        if (forceFlush)
        {
            _diskStream.Flush();
        }

        if (MarkPageClean(page, dirtyGeneration))
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
        if (loggedLsn < 0)
        {
            if (currentLsn < 0)
            {
                return false;
            }
        }
        else if (currentLsn != loggedLsn)
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
        if (loggedLsn < 0)
        {
            if (currentLsn < 0)
            {
                return false;
            }
        }
        else if (currentLsn != loggedLsn)
        {
            return false;
        }

        var snapshot = page.SnapshotForDiskWrite(out var dirtyGeneration);
        var pageOffset = CalculatePageOffset(page.PageID);

        await WriteEncodedPageToDiskAsync(page.PageID, pageOffset, snapshot, cancellationToken).ConfigureAwait(false);
        if (MarkPageClean(page, dirtyGeneration))
        {
            _deferredWalPages.TryRemove(page.PageID, out _);
        }
        return true;
    }

    private byte[]? ReadPageSnapshotForWal(uint pageID)
    {
        var pageOffset = CalculatePageOffset(pageID);
        if (pageOffset + _physicalPageSize > _fileSize)
        {
            return null;
        }

        return ReadLogicalPageDataOrNull(pageID, pageOffset)?.Data;
    }

    private async Task<byte[]?> ReadPageSnapshotForWalAsync(uint pageID, CancellationToken cancellationToken)
    {
        var pageOffset = CalculatePageOffset(pageID);
        if (pageOffset + _physicalPageSize > _fileSize)
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

        var page = GetPage(pageID);
        byte[]? beforeImage = null;
        if (_appendLogPage != null && _requiresWalBeforeImage?.Invoke() == true)
        {
            beforeImage = ReadPageSnapshotForWal(pageID);
        }

        while (true)
        {
            uint expectedFirstFreePageId;
            lock (_stateLock)
            {
                expectedFirstFreePageId = _firstFreePageID;
                page.ClearData();
                page.UpdatePageType(PageType.Empty);
                page.SetLinks(0, expectedFirstFreePageId);
                page.Header.ItemCount = 0;
                page.Header.FreeBytes = (ushort)(page.DataSize);
            }

            _appendLogPage?.Invoke(page, beforeImage);
            if (_flushLogToLsn != null && page.Header.LSN >= 0)
            {
                _flushLogToLsn(page.Header.LSN);
            }

            lock (_stateLock)
            {
                if (_firstFreePageID != expectedFirstFreePageId)
                {
                    continue;
                }

                WritePageToDisk(page, forceFlush: false);
                _firstFreePageID = pageID;
                return;
            }
        }
    }

    /// <summary>
    /// 刷新所有脏页面到磁盘
    /// </summary>
    public void FlushDirtyPages()
    {
        ThrowIfDisposed();
        FlushDirtyPagesCore(skipUnsafeDeferredWalPages: false);
    }

    private void FlushDirtyPagesCore(bool skipUnsafeDeferredWalPages)
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

            if (skipUnsafeDeferredWalPages && IsDeferredWalPagePending(page))
            {
                continue;
            }

            SavePage(page);
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
        return loggedLsn < 0
            ? currentLsn < 0
            : currentLsn != loggedLsn;
    }

    /// <summary>
    /// 异步刷新所有脏页面到磁盘
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    public async Task FlushDirtyPagesAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await FlushDirtyPagesAsyncCore(skipUnsafeDeferredWalPages: false, cancellationToken).ConfigureAwait(false);
    }

    private async Task FlushDirtyPagesAsyncCore(bool skipUnsafeDeferredWalPages, CancellationToken cancellationToken)
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

            if (skipUnsafeDeferredWalPages && IsDeferredWalPagePending(page))
            {
                continue;
            }

            await SavePageAsync(page, cancellationToken: cancellationToken).ConfigureAwait(false);
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

            _fileSize = Math.Max(_fileSize, pageOffset + _physicalPageSize);
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

        lock (_cacheLock)
        {
            if (maxPagesToKeep <= 0)
            {
                // 清空所有缓存
                foreach (var page in _pageCache.Values)
                {
                    RemoveDirtyTracking(page);
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
                            RemoveDirtyTracking(page);
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
            if (_pageCache.Count >= MaxCacheSize)
            {
                EvictLeastRecentlyUsed();
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
    private bool EvictLeastRecentlyUsed()
    {
        // 尝试从 LRU 获取多个候选者，以防第一个被 Pin 住。
        var candidates = _lruCache.GetLeastRecentlyUsed(10);
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

                    page.Pin();
                    evictionCandidate = page;
                }
            }

            if (evictionCandidate == null)
            {
                continue;
            }

            try
            {
                if (evictionCandidate.IsDirty)
                {
                    ScheduleBackgroundWriteback();
                    SavePage(evictionCandidate);
                }

                lock (lockObj)
                {
                    if (!_pageCache.TryGetValue(pageID, out var currentPage) ||
                        !ReferenceEquals(currentPage, evictionCandidate))
                    {
                        continue;
                    }

                    if (currentPage.PinCount > 1 || currentPage.IsDirty)
                    {
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

                await FlushDirtyPagesAsyncCore(skipUnsafeDeferredWalPages: true, CancellationToken.None).ConfigureAwait(false);
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
                            FlushDirtyPages();
                        }
                        catch (Exception ex)
                        {
                            flushException = new InvalidOperationException("Flush dirty pages during PageManager dispose failed.", ex);
                        }

                        // 清理缓存
                        ClearCache();

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

