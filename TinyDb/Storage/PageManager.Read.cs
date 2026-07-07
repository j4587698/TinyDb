using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class PageManager
{
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
            if (IsBeyondFileSize(pageOffset))
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
        if (IsBeyondFileSize(pageOffset))
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
            if (IsBeyondFileSize(pageOffset))
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
            if (IsBeyondFileSize(pageOffset))
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
            if (IsBeyondFileSize(pageOffset))
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
}
