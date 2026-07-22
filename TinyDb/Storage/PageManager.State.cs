using System.Buffers;
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
