using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class PageManager
{
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

            EnsureRecordedFileSizeAtLeast(pageOffset + _physicalPageSize);
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
}
