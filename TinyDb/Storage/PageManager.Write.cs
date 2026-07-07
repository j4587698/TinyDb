using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class PageManager
{
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
        if (IsBeyondFileSize(pageOffset))
        {
            return null;
        }

        return ReadLogicalPageDataOrNull(pageID, pageOffset)?.Data;
    }

    private async Task<byte[]?> ReadPageSnapshotForWalAsync(uint pageID, CancellationToken cancellationToken)
    {
        var pageOffset = CalculatePageOffset(pageID);
        if (IsBeyondFileSize(pageOffset))
        {
            return null;
        }

        return (await ReadLogicalPageDataOrNullAsync(pageID, pageOffset, cancellationToken).ConfigureAwait(false))?.Data;
    }
}
