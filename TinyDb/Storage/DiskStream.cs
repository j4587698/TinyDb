using System.Collections.Concurrent;
using System.IO;
using System.Threading;

namespace TinyDb.Storage;

/// <summary>
/// 磁盘流管理器，负责数据库文件的 I/O 操作
/// </summary>
public sealed class DiskStream : IDiskStream
{
    private readonly string _filePath;
    private readonly FileStream _fileStream;
    private bool _disposed;
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    /// <summary>
    /// 跨平台文件区域锁管理器（进程内）
    /// </summary>
    private static readonly ConcurrentDictionary<string, RegionLockManager> _regionLocks = new();

    /// <summary>
    /// 文件路径
    /// </summary>
    public string FilePath => _filePath;

    /// <summary>
    /// 文件大小
    /// </summary>
    public long Size
    {
        get
        {
            ThrowIfDisposed();
            _semaphore.Wait();
            try
            {
                return _fileStream.Length;
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    /// <summary>
    /// 当前位置
    /// </summary>
    public long Position
    {
        get
        {
            ThrowIfDisposed();
            _semaphore.Wait();
            try
            {
                return _fileStream.Position;
            }
            finally
            {
                _semaphore.Release();
            }
        }
    }

    /// <summary>
    /// 是否可读
    /// </summary>
    public bool IsReadable => _fileStream.CanRead;

    /// <summary>
    /// 是否可写
    /// </summary>
    public bool IsWritable => _fileStream.CanWrite;

    /// <summary>
    /// 是否可定位
    /// </summary>
    public bool IsSeekable => _fileStream.CanSeek;

    /// <summary>
    /// 初始化磁盘流
    /// </summary>
    /// <param name="filePath">文件路径</param>
    /// <param name="access">访问模式</param>
    /// <param name="share">共享模式</param>
    public DiskStream(string filePath, FileAccess access = FileAccess.ReadWrite, FileShare share = FileShare.None)
    {
        _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        _fileStream = new FileStream(filePath, FileMode.OpenOrCreate, access, share);
    }

    // ... Read, Write, Flush, Seek, SetLength ... (keeping them as is, just need to match surrounding code for context in replace)
    
    public int Read(byte[] buffer, int offset, int count)
    {
        ThrowIfDisposed();
        _semaphore.Wait();
        try
        {
            return _fileStream.Read(buffer, offset, count);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public void Write(byte[] buffer, int offset, int count)
    {
        ThrowIfDisposed();
        _semaphore.Wait();
        try
        {
            _fileStream.Write(buffer, offset, count);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public void Flush()
    {
        ThrowIfDisposed();
        _semaphore.Wait();
        try
        {
            _fileStream.Flush(true);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public long Seek(long offset, SeekOrigin origin)
    {
        ThrowIfDisposed();
        _semaphore.Wait();
        try
        {
            return _fileStream.Seek(offset, origin);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public void SetLength(long length)
    {
        ThrowIfDisposed();
        _semaphore.Wait();
        try
        {
            _fileStream.SetLength(length);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// 锁定文件区域（跨平台实现）
    /// </summary>
    /// <param name="position">起始位置</param>
    /// <param name="length">锁定长度</param>
    /// <returns>锁定句柄</returns>
    public object LockRegion(long position, long length)
    {
        ThrowIfDisposed();

        // 获取或创建该文件的区域锁管理器
        var fullPath = Path.GetFullPath(_filePath);
        var lockManager = _regionLocks.GetOrAdd(fullPath, _ => new RegionLockManager());

        // 获取区域锁
        var regionLock = lockManager.AcquireLock(position, length);
        return regionLock;
    }

    /// <summary>
    /// 解锁文件区域
    /// </summary>
    /// <param name="lockHandle">锁定句柄</param>
    public void UnlockRegion(object lockHandle)
    {
        ThrowIfDisposed();

        if (lockHandle is RegionLock regionLock)
        {
            regionLock.Release();
        }
        else
        {
            throw new ArgumentException("Invalid lock handle", nameof(lockHandle));
        }
    }

    /// <summary>
    /// 跨平台区域锁管理器（进程内）
    /// </summary>
    private sealed class RegionLockManager
    {
        private readonly object _syncRoot = new();
        private readonly List<ActiveRegionLock> _lockedRegions = new();

        public RegionLock AcquireLock(long position, long length)
        {
            var end = position + length;

            while (true)
            {
                ActiveRegionLock? conflictingLock = null;

                lock (_syncRoot)
                {
                    // 检查是否有重叠的区域
                    foreach (var region in _lockedRegions)
                    {
                        if (position < region.End && end > region.Start)
                        {
                            // 有重叠
                            conflictingLock = region;
                            break;
                        }
                    }

                    if (conflictingLock == null)
                    {
                        // 没有重叠，创建新的锁并立即添加到列表
                        var newLock = new ActiveRegionLock(position, end);
                        _lockedRegions.Add(newLock);
                        return new RegionLock(this, newLock);
                    }
                }

                // 有冲突，在锁外等待
                conflictingLock.WaitForRelease();
                // 等待后重新检查，因为可能有新的冲突
            }
        }

        public void ReleaseLock(ActiveRegionLock activeLock)
        {
            lock (_syncRoot)
            {
                _lockedRegions.Remove(activeLock);
            }
            activeLock.SignalRelease();
        }

        /// <summary>
        /// 活动区域锁
        /// </summary>
        internal sealed class ActiveRegionLock
        {
            private readonly ManualResetEventSlim _releaseEvent = new(false);

            public long Start { get; }
            public long End { get; }

            public ActiveRegionLock(long start, long end)
            {
                Start = start;
                End = end;
            }

            public void WaitForRelease()
            {
                _releaseEvent.Wait();
            }

            public void SignalRelease()
            {
                _releaseEvent.Set();
            }
        }
    }

    /// <summary>
    /// 区域锁句柄
    /// </summary>
    private sealed class RegionLock
    {
        private readonly RegionLockManager _manager;
        private readonly RegionLockManager.ActiveRegionLock _activeLock;
        private bool _released;

        public RegionLock(RegionLockManager manager, RegionLockManager.ActiveRegionLock activeLock)
        {
            _manager = manager;
            _activeLock = activeLock;
        }

        public void Release()
        {
            if (!_released)
            {
                _released = true;
                _manager.ReleaseLock(_activeLock);
            }
        }
    }


    /// <summary>
    /// 获取文件统计信息
    /// </summary>
    /// <returns>统计信息</returns>
    public DiskStreamStatistics GetStatistics()
    {
        ThrowIfDisposed();
        _semaphore.Wait();
        try
        {
            return new DiskStreamStatistics
            {
                FilePath = _filePath,
                Size = _fileStream.Length,
                Position = _fileStream.Position,
                IsReadable = _fileStream.CanRead,
                IsWritable = _fileStream.CanWrite,
                IsSeekable = _fileStream.CanSeek
            };
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(DiskStream));
    }

    /// <summary>
    /// 异步刷新缓冲区到磁盘
    /// </summary>
    /// <param name="cancellationToken">取消令牌</param>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            _fileStream.Flush(true);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// 读取页面数据
    /// </summary>
    /// <param name="pageOffset">页面偏移量</param>
    /// <param name="pageSize">页面大小</param>
    /// <returns>页面数据</returns>
    public byte[] ReadPage(long pageOffset, int pageSize)
    {
        ThrowIfDisposed();
        _semaphore.Wait();
        try
        {
            _fileStream.Seek(pageOffset, SeekOrigin.Begin);
            var buffer = new byte[pageSize];
            var bytesRead = 0;
            while (bytesRead < pageSize)
            {
                var read = _fileStream.Read(buffer, bytesRead, pageSize - bytesRead);
                if (read == 0) break;
                bytesRead += read;
            }
            
            return buffer;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// 写入页面数据
    /// </summary>
    /// <param name="pageOffset">页面偏移量</param>
    /// <param name="pageData">页面数据</param>
    public void WritePage(long pageOffset, byte[] pageData)
    {
        ThrowIfDisposed();
        _semaphore.Wait();
        try
        {
            _fileStream.Seek(pageOffset, SeekOrigin.Begin);
            _fileStream.Write(pageData, 0, pageData.Length);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// 异步读取页面数据
    /// </summary>
    /// <param name="pageOffset">页面偏移量</param>
    /// <param name="pageSize">页面大小</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>页面数据</returns>
    public async Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            _fileStream.Seek(pageOffset, SeekOrigin.Begin);
            var buffer = new byte[pageSize];
            var bytesRead = 0;
            while (bytesRead < pageSize)
            {
                var read = await _fileStream.ReadAsync(buffer, bytesRead, pageSize - bytesRead, cancellationToken);
                if (read == 0) break;
                bytesRead += read;
            }
            return buffer;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// 异步写入页面数据
    /// </summary>
    /// <param name="pageOffset">页面偏移量</param>
    /// <param name="pageData">页面数据</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>任务</returns>
    public async Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            _fileStream.Seek(pageOffset, SeekOrigin.Begin);
            await _fileStream.WriteAsync(pageData, 0, pageData.Length, cancellationToken);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _semaphore.Dispose();
            _fileStream.Dispose();
            _disposed = true;
        }
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"DiskStream[{_filePath}]: Size={Size}, Position={Position}";
    }
}

/// <summary>
/// 磁盘流统计信息
/// </summary>
public sealed class DiskStreamStatistics
{
    public string FilePath { get; init; } = string.Empty;
    public long Size { get; init; }
    public long Position { get; init; }
    public bool IsReadable { get; init; }
    public bool IsWritable { get; init; }
    public bool IsSeekable { get; init; }

    public override string ToString()
    {
        return $"DiskStream[{FilePath}]: {Position}/{Size} bytes, R={IsReadable} W={IsWritable} S={IsSeekable}";
    }
}
