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
public sealed partial class PageManager : IDisposable
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
    private const int DeferredFreePageScanNone = 0;
    private const int DeferredFreePageScanRebuild = 1;
    private const int DeferredFreePageScanCount = 2;
    private readonly SemaphoreSlim[] _pageLoadStripes;
    private int _cachedPageCount;
    private uint _nextPageID;
    private uint _firstFreePageID; // Head of free page linked list
    private uint _freePageCount;
    private int _deferredFreePageScanMode;
    private long _fileSize;
    private bool _disposed;
    private int _disposeStarted;
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
    /// 页面大小
    /// </summary>
    public uint PageSize => _pageSize;

    /// <summary>
    /// 缓存页面数量
    /// </summary>
    public int CachedPages => GetCachedPageCount();

    /// <summary>
    /// 第一个空闲页面ID
    /// </summary>
    public uint FirstFreePageID
    {
        get
        {
            EnsureDeferredFreePageScanCompleted();
            lock (_stateLock)
            {
                return _firstFreePageID;
            }
        }
    }

    /// <summary>
    /// 总页面数量
    /// </summary>
    public uint TotalPages => (uint)(ReadFileSize() / _physicalPageSize);

    private long ReadFileSize() => Interlocked.Read(ref _fileSize);

    private void SetFileSize(long fileSize) => Interlocked.Exchange(ref _fileSize, fileSize);

    private bool IsBeyondFileSize(long pageOffset)
    {
        return pageOffset + _physicalPageSize > ReadFileSize();
    }

    private void EnsureRecordedFileSizeAtLeast(long fileSize)
    {
        while (true)
        {
            var current = ReadFileSize();
            if (fileSize <= current) return;

            if (Interlocked.CompareExchange(ref _fileSize, fileSize, current) == current)
            {
                return;
            }
        }
    }

    /// <summary>
    /// 空闲页面数量
    /// </summary>
    public uint FreePageCount
    {
        get
        {
            EnsureDeferredFreePageScanCompleted();
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
        _lruCache = new LRUCache<uint, Page>(GetCacheOverflowLimit(maxCacheSize), evictOnCapacity: false);
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

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed))
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
        if (Interlocked.Exchange(ref _disposeStarted, 1) == 0)
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
                        try
                        {
                            if (flushException == null)
                            {
                                ClearCacheCore();
                            }

                            foreach (var gate in _pageLoadStripes)
                            {
                                gate.Dispose();
                            }

                            _diskStream.Dispose();
                        }
                        catch (Exception ex)
                        {
                            cleanupException = new InvalidOperationException("PageManager dispose cleanup failed.", ex);
                        }
                    }
                    finally
                    {
                        Volatile.Write(ref _disposed, true);
                    }
                }
                finally
                {
                    _backgroundWritebackGate.Release();
                }

                if (!_backgroundWritebackIdle.Wait(TimeSpan.FromSeconds(5)))
                {
                    Log(TinyDbLogLevel.Warning, "Background page writeback did not stop before PageManager dispose timeout.");
                }
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
