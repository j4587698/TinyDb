using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class PageManager
{
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
    public void Initialize(
        uint totalPages,
        uint firstFreePageID,
        uint freePageCount = 0,
        bool hasFreePageCount = false,
        bool readOnly = false)
    {
        Volatile.Write(ref _deferredFreePageScanMode, DeferredFreePageScanNone);
        bool rebuildFreeList;
        bool countExistingFreeList;
        uint nextPageId;
        uint initialFirstFreePageId;
        lock (_stateLock)
        {
            // 如果文件大小不匹配 TotalPages，优先信任文件大小
            var calculatedTotal = (uint)(ReadFileSize() / _physicalPageSize);
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

        if (readOnly && (rebuildFreeList || countExistingFreeList))
        {
            Volatile.Write(
                ref _deferredFreePageScanMode,
                rebuildFreeList ? DeferredFreePageScanRebuild : DeferredFreePageScanCount);
            return;
        }

        if (rebuildFreeList)
        {
            var (rebuiltFirstFreePageId, countedFreePages) = ScanFreePages(nextPageId, rewriteLinks: !readOnly);
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

    private (uint FirstFreePageId, uint FreePageCount) ScanFreePages(uint nextPageId, bool rewriteLinks)
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

        if (rewriteLinks)
        {
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

    private void EnsureDeferredFreePageScanCompleted()
    {
        if (Volatile.Read(ref _deferredFreePageScanMode) == DeferredFreePageScanNone)
        {
            return;
        }

        lock (_freeListLock)
        {
            var scanMode = Volatile.Read(ref _deferredFreePageScanMode);
            if (scanMode == DeferredFreePageScanNone)
            {
                return;
            }

            uint nextPageId;
            uint firstFreePageId;
            lock (_stateLock)
            {
                nextPageId = _nextPageID;
                firstFreePageId = _firstFreePageID;
            }

            uint resolvedFirstFreePageId;
            uint resolvedFreePageCount;
            if (scanMode == DeferredFreePageScanRebuild)
            {
                (resolvedFirstFreePageId, resolvedFreePageCount) = ScanFreePages(nextPageId, rewriteLinks: false);
            }
            else
            {
                resolvedFirstFreePageId = firstFreePageId;
                resolvedFreePageCount = CountFreePages(firstFreePageId, nextPageId);
            }

            lock (_stateLock)
            {
                _firstFreePageID = resolvedFirstFreePageId;
                _freePageCount = resolvedFreePageCount;
                Volatile.Write(ref _deferredFreePageScanMode, DeferredFreePageScanNone);
            }
        }
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
    /// 读取 allocator state page 中记录的空闲链表状态。
    /// </summary>
    /// <param name="allocatorStatePageId">allocator state page 页号，0 表示旧库未迁移。</param>
    /// <param name="firstFreePageId">读到的空闲链表头。</param>
    /// <param name="freePageCount">读到的空闲页数量。</param>
    /// <returns>成功读到可信状态则为 true；调用方应在返回 false 时沿用 header 中的值。</returns>
    /// <remarks>
    /// 由 <see cref="TinyDbEngine"/> 在 <see cref="Initialize"/> 之前调用，好让扫描/重建逻辑
    /// 一开始就拿到正确的链表头，避免先做一次全盘 <c>ScanFreePages</c> 再被覆盖。
    /// <para>
    /// 页读取失败的处理分两种情况：未加密库按普通磁盘损坏处理，降级到 header 值并让上层重建
    /// ——allocator state 的内容完全可以由空闲链表扫描重算，纯属缓存，绝不能因为它坏了就让整个库
    /// 打不开（那正是本轮改动要消灭的"一处局部损坏放大成整库不可恢复"）。加密库则一律外抛：
    /// 解密认证失败意味着文件被篡改，把它降级成警告等于给篡改开了一个后门。
    /// </para>
    /// </remarks>
    internal bool TryReadAllocatorState(uint allocatorStatePageId, out uint firstFreePageId, out uint freePageCount)
    {
        firstFreePageId = 0;
        freePageCount = 0;

        if (allocatorStatePageId == 0) return false;

        if (IsBeyondFileSize(CalculatePageOffset(allocatorStatePageId)))
        {
            // GetPage 对越界页号会静默新建一个空白页，那样会把空闲链表悄悄清零。
            Log(TinyDbLogLevel.Warning,
                $"Allocator state page {allocatorStatePageId} is beyond the end of the database file. Falling back to header values.");
            return false;
        }

        Page page;
        try
        {
            page = GetPage(allocatorStatePageId);
        }
        catch (InvalidDataException ex) when (!_pageCodec.IsEncrypted)
        {
            Log(TinyDbLogLevel.Warning,
                $"Allocator state page {allocatorStatePageId} could not be read. Falling back to header values; the page will be rebuilt.", ex);
            return false;
        }

        if (page.PageType != PageType.Extension)
        {
            Log(TinyDbLogLevel.Warning,
                $"Allocator state page {allocatorStatePageId} has page type {page.PageType} instead of {PageType.Extension}. Falling back to header values.");
            return false;
        }

        var data = page.ReadBytes(0, AllocatorStateDataSize);
        firstFreePageId = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(0, 4));
        freePageCount = BinaryPrimitives.ReadUInt32LittleEndian(data.AsSpan(4, 4));
        return true;
    }

    /// <summary>
    /// 绑定 allocator state page，使后续 <see cref="FreePage"/>/<see cref="NewPage"/> 自动持久化空闲链表状态。
    /// </summary>
    internal void SetAllocatorStatePage(uint allocatorStatePageId)
    {
        _allocatorStatePageId = allocatorStatePageId;
    }

    /// <summary>
    /// 为旧库创建 allocator state page 并写入当前空闲链表状态。
    /// </summary>
    /// <remarks>
    /// 由 <see cref="TinyDbEngine"/> 在检测到旧库（<c>AllocatorStatePageId == 0</c>）时调用。
    /// 调用前 PageManager 应已完成 Initialize，_firstFreePageID/_freePageCount 是正确的。
    /// </remarks>
    internal uint CreateAllocatorStatePage()
    {
        // 分配一个新页作为 allocator state page
        // 此时 _allocatorStatePageId 仍为 0，PersistAllocatorState 是 no-op，不会递归
        var page = NewPage(PageType.Extension);

        // 写入当前空闲链表状态
        uint firstFreePageId;
        uint freePageCount;
        lock (_stateLock)
        {
            firstFreePageId = _firstFreePageID;
            freePageCount = _freePageCount;
        }

        WriteAllocatorStateToPage(page, firstFreePageId, freePageCount);

        // 设置 _allocatorStatePageId，后续 FreePage/NewPage 会自动持久化
        _allocatorStatePageId = page.PageID;

        // 立即写盘 + WAL
        SavePage(page, forceFlush: false);

        return page.PageID;
    }

    /// <summary>
    /// 把当前空闲链表状态持久化到 allocator state page。
    /// </summary>
    /// <remarks>
    /// 在 <see cref="FreePage"/> 和 <see cref="NewPage"/>/TryTakeFreePage 修改
    /// <c>_firstFreePageID</c>/<c>_freePageCount</c> 后调用。
    /// 调用方必须已持有 <c>_freeListLock</c>。
    /// 若 <c>_allocatorStatePageId == 0</c>（旧库未迁移），此方法是 no-op。
    /// 这里只写 WAL 并把页标脏，不做同步刷盘：分配/回收是热路径，每次都刷 WAL + 写盘
    /// 会让批量删除和 B 树分裂合并的开销成倍上升。allocator state 落后最坏只是少回收一些空间，
    /// 而 TryTakeFreePage 的页型守卫保证落后的链表头不会被误用。
    /// </remarks>
    private void PersistAllocatorState()
    {
        var statePageId = _allocatorStatePageId;
        if (statePageId == 0) return;

        uint firstFreePageId;
        uint freePageCount;
        lock (_stateLock)
        {
            firstFreePageId = _firstFreePageID;
            freePageCount = _freePageCount;
        }

        var page = GetPage(statePageId);
        byte[]? beforeImage = null;
        if (_appendLogPage != null && _requiresWalBeforeImage?.Invoke() == true)
        {
            beforeImage = ReadPageSnapshotForWal(statePageId);
        }

        WriteAllocatorStateToPage(page, firstFreePageId, freePageCount);

        _appendLogPage?.Invoke(page, beforeImage, null);
    }

    private void WriteAllocatorStateToPage(Page page, uint firstFreePageId, uint freePageCount)
    {
        // 分配/回收是热路径，用栈上缓冲避免每次调用产生一次堆分配。
        Span<byte> data = stackalloc byte[AllocatorStateDataSize];
        BinaryPrimitives.WriteUInt32LittleEndian(data.Slice(0, 4), firstFreePageId);
        BinaryPrimitives.WriteUInt32LittleEndian(data.Slice(4, 4), freePageCount);
        page.WriteData(0, data);
    }

    private const int AllocatorStateDataSize = 8;

    /// <summary>
    /// 获取页面使用统计
    /// </summary>
    /// <returns>页面统计信息</returns>
    public PageManagerStatistics GetStatistics()
    {
        ThrowIfDisposed();
        EnsureDeferredFreePageScanCompleted();

        var dirtyPages = CountDirtyPages();
        uint freeCount;
        uint nextPageId;
        long fileSize;
        lock (_stateLock)
        {
            freeCount = _freePageCount;
            nextPageId = _nextPageID;
            fileSize = ReadFileSize();
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
            CachedPages = GetCachedPageCount(),
            DirtyPages = dirtyPages,
            MaxCacheSize = MaxCacheSize,
            CacheHitRatio = _lruCache.HitRatio,
            FileSize = fileSize,
            NextPageID = nextPageId
        };
    }
}
