using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class PageManager
{
    public void ClearCache(int maxPagesToKeep = 0, bool flushDirtyPages = true)
    {
        ThrowIfDisposed();

        if (flushDirtyPages)
        {
            FlushDirtyPages();
        }

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

        if (page.IsDisposed)
        {
            if (!_pageCache.TryRemove(pageID, out var removedPage))
            {
                return false;
            }

            RemoveDirtyTracking(removedPage);
            _lruCache.Remove(pageID);
            return true;
        }

        if (page.PinCount > 0 || page.IsDirty)
        {
            return false;
        }

        Page? pageToDispose = null;
        page.Pin();

        try
        {
            if (!_pageCache.TryGetValue(pageID, out var currentPage) ||
                !ReferenceEquals(currentPage, page) ||
                currentPage.PinCount != 1 ||
                currentPage.IsDirty)
            {
                return false;
            }

            if (!_pageCache.TryRemove(pageID, out var removedPage))
            {
                return false;
            }

            RemoveDirtyTracking(removedPage);
            _lruCache.Remove(pageID);
            pageToDispose = removedPage;
            return true;
        }
        finally
        {
            page.Unpin();
            pageToDispose?.Dispose();
        }
    }

    internal void DiscardCachedPage(uint pageID)
    {
        ThrowIfDisposed();
        RemoveFromCache(pageID);
    }

    /// <summary>
    /// 计算页面偏移量
    /// </summary>
    /// <param name="pageID">页面ID</param>
    /// <returns>偏移量</returns>
    internal long CalculatePageOffset(uint pageID)
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
                if (_pageCache.TryGetValue(page.PageID, out var cachedPage))
                {
                    if (pinned)
                    {
                        cachedPage.Pin();
                    }

                    _lruCache.TryTouch(page.PageID);
                    return cachedPage;
                }

                if (_pageCache.Count >= CacheOverflowLimit)
                {
                    continue;
                }

                if (_pageCache.TryAdd(page.PageID, page))
                {
                    AttachDirtyTracking(page);
                    var pinnedForCache = false;
                    if (pinned)
                    {
                        page.Pin();
                        pinnedForCache = true;
                    }

                    try
                    {
                        _lruCache.Put(page.PageID, page);
                        return page;
                    }
                    catch
                    {
                        _pageCache.TryRemove(page.PageID, out _);
                        RemoveDirtyTracking(page);
                        if (pinnedForCache)
                        {
                            page.Unpin();
                        }

                        throw;
                    }
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
        Page? removedPage = null;
        lock (_cacheLock)
        {
            if (_pageCache.TryRemove(pageID, out var page))
            {
                RemoveDirtyTracking(page);
                removedPage = page;
            }

            _lruCache.Remove(pageID);
        }

        removedPage?.Dispose();
    }
}
