using System.IO;

namespace SimpleDb.Storage;

/// <summary>
/// 磁盘流管理器，负责数据库文件的 I/O 操作
/// </summary>
public sealed class DiskStream : IDisposable
{
    private readonly string _filePath;
    private readonly FileStream _fileStream;
    private bool _disposed;

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
            return _fileStream.Length;
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
            return _fileStream.Position;
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

    /// <summary>
    /// 读取数据
    /// </summary>
    /// <param name="buffer">缓冲区</param>
    /// <param name="offset">偏移量</param>
    /// <param name="count">读取字节数</param>
    /// <returns>实际读取的字节数</returns>
    public int Read(byte[] buffer, int offset, int count)
    {
        ThrowIfDisposed();
        return _fileStream.Read(buffer, offset, count);
    }

    /// <summary>
    /// 写入数据
    /// </summary>
    /// <param name="buffer">缓冲区</param>
    /// <param name="offset">偏移量</param>
    /// <param name="count">写入字节数</param>
    public void Write(byte[] buffer, int offset, int count)
    {
        ThrowIfDisposed();
        _fileStream.Write(buffer, offset, count);
    }

    /// <summary>
    /// 刷新缓冲区到磁盘
    /// </summary>
    public void Flush()
    {
        ThrowIfDisposed();
        _fileStream.Flush(true);
    }

    /// <summary>
    /// 设置流位置
    /// </summary>
    /// <param name="offset">偏移量</param>
    /// <param name="origin">起始位置</param>
    /// <returns>新位置</returns>
    public long Seek(long offset, SeekOrigin origin)
    {
        ThrowIfDisposed();
        return _fileStream.Seek(offset, origin);
    }

    /// <summary>
    /// 设置文件长度
    /// </summary>
    /// <param name="length">文件长度</param>
    public void SetLength(long length)
    {
        ThrowIfDisposed();
        _fileStream.SetLength(length);
    }

    /// <summary>
    /// 锁定文件区域
    /// </summary>
    /// <param name="position">起始位置</param>
    /// <param name="length">锁定长度</param>
    /// <returns>锁定句柄</returns>
    public object LockRegion(long position, long length)
    {
        ThrowIfDisposed();
        return new object(); // 简化实现
    }

    /// <summary>
    /// 解锁文件区域
    /// </summary>
    /// <param name="lockHandle">锁定句柄</param>
    public void UnlockRegion(object lockHandle)
    {
        ThrowIfDisposed();
        // 简化实现
    }

    /// <summary>
    /// 获取文件统计信息
    /// </summary>
    /// <returns>统计信息</returns>
    public DiskStreamStatistics GetStatistics()
    {
        ThrowIfDisposed();
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
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        _fileStream.Flush(true);
        return Task.CompletedTask;
    }

    /// <summary>
    /// 读取页面数据
    /// </summary>
    /// <param name="pageOffset">页面偏移量</param>
    /// <param name="pageSize">页面大小</param>
    /// <returns>页面数据</returns>
    public byte[] ReadPage(long pageOffset, int pageSize)
    {
        Seek(pageOffset, SeekOrigin.Begin);
        var buffer = new byte[pageSize];
        Read(buffer, 0, pageSize);
        return buffer;
    }

    /// <summary>
    /// 写入页面数据
    /// </summary>
    /// <param name="pageOffset">页面偏移量</param>
    /// <param name="pageData">页面数据</param>
    public void WritePage(long pageOffset, byte[] pageData)
    {
        Seek(pageOffset, SeekOrigin.Begin);
        Write(pageData, 0, pageData.Length);
    }

    /// <summary>
    /// 异步读取页面数据
    /// </summary>
    /// <param name="pageOffset">页面偏移量</param>
    /// <param name="pageSize">页面大小</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>页面数据</returns>
    public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(ReadPage(pageOffset, pageSize));
    }

    /// <summary>
    /// 异步写入页面数据
    /// </summary>
    /// <param name="pageOffset">页面偏移量</param>
    /// <param name="pageData">页面数据</param>
    /// <param name="cancellationToken">取消令牌</param>
    /// <returns>任务</returns>
    public Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
    {
        WritePage(pageOffset, pageData);
        return Task.CompletedTask;
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _fileStream?.Dispose();
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