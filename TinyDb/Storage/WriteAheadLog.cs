using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

/// <summary>
/// 简化版写前日志实现，用于保证崩溃恢复能力。
/// </summary>
public sealed partial class WriteAheadLog : IDisposable
{
    private const byte EntryTypePage = 0x1;
    private const byte EntryTypeTransactionBegin = 0x2;
    private const byte EntryTypeTransactionPage = 0x3;
    private const byte EntryTypeTransactionCommit = 0x4;
    private const int HeaderSize = 9;
    private const int TransactionIdSize = 16;
    private const int BeforeLengthSize = sizeof(int);
    private const int PageChecksumOffset = 21;
    private const int PageLsnOffset = 41;
    private const long DeferredTruncateThresholdBytes = 4L * 1024 * 1024;
    private const long PendingDeferredTransactionLsn = -1;

    private readonly string _logFilePath;
    private readonly FileStream? _stream;
    private readonly Action<TinyDbLogLevel, string, Exception?> _log;
    private readonly IWalCodec _walCodec;
    private readonly SemaphoreSlim _mutex = new(1, 1);
    private static readonly AsyncLocal<WriteLockContext?> s_currentWriteContext = new();
    private readonly AsyncLocal<TransactionContext?> _currentTransactionContext = new();
    private readonly int _maxRecordSize;
    private int _disposed;
    private int _hasPendingEntries;
    private long _flushedLSN;

    /// <summary>
    /// 是否启用 WAL
    /// </summary>
    public bool IsEnabled { get; }

    /// <summary>
    /// 当前已刷盘的 LSN
    /// </summary>
    public long FlushedLSN => ReadFlushedLSN();

    private long ReadFlushedLSN() => Interlocked.Read(ref _flushedLSN);

    private void SetFlushedLSN(long value) => Interlocked.Exchange(ref _flushedLSN, value);

    /// <summary>
    /// 是否有未提交的日志记录
    /// </summary>
    public bool HasPendingEntries => IsEnabled && HasPendingEntriesCore;

    private bool HasPendingEntriesCore => Volatile.Read(ref _hasPendingEntries) != 0;

    private void SetHasPendingEntries(bool value)
    {
        Volatile.Write(ref _hasPendingEntries, value ? 1 : 0);
    }

    internal sealed class WriteLockContext
    {
        private readonly WriteAheadLog _owner;
        private int _active = 1;

        internal WriteLockContext(WriteAheadLog owner)
        {
            _owner = owner;
        }

        internal bool IsActiveFor(WriteAheadLog owner)
        {
            return ReferenceEquals(_owner, owner) && Volatile.Read(ref _active) != 0;
        }

        internal void Deactivate()
        {
            Volatile.Write(ref _active, 0);
        }
    }

    public WriteAheadLog(
        string databaseFilePath,
        int pageSize,
        bool enabled,
        string? walFileNameFormat = null,
        Action<TinyDbLogLevel, string, Exception?>? logger = null)
        : this(databaseFilePath, pageSize, enabled, walFileNameFormat, logger, null)
    {
    }

    internal WriteAheadLog(
        string databaseFilePath,
        int pageSize,
        bool enabled,
        string? walFileNameFormat,
        Action<TinyDbLogLevel, string, Exception?>? logger,
        IWalCodec? walCodec,
        bool readOnly = false)
    {
        if (string.IsNullOrWhiteSpace(databaseFilePath))
            throw new ArgumentException("Database file path cannot be null or empty", nameof(databaseFilePath));

        _log = logger ?? TinyDbLogging.NoopLogger;
        _walCodec = walCodec ?? new NoOpWalCodec();
        _maxRecordSize = Math.Max(pageSize * 2 + TransactionIdSize + BeforeLengthSize, pageSize) + _walCodec.MaxOverhead;
        _logFilePath = GenerateWalFilePath(databaseFilePath, walFileNameFormat ?? "{name}-wal.{ext}");

        if (readOnly)
        {
            if (File.Exists(_logFilePath) && new FileInfo(_logFilePath).Length > 0)
            {
                throw new InvalidOperationException(
                    "Cannot open the database in read-only mode while WAL recovery is pending.");
            }

            IsEnabled = false;
            return;
        }

        IsEnabled = enabled;

        if (!IsEnabled)
        {
            TryDeleteExistingLog();
            return;
        }

        var directory = Path.GetDirectoryName(_logFilePath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        // 同步句柄：WAL 绝大多数调用是同步的（Write + Flush(true)），在异步句柄上做同步 IO
        // 每次都要额外分配 overlapped 并等待事件；.NET 也没有异步落盘 API，异步句柄没有收益。
        // 同步句柄还使 TinyDb 不再持有任何绑定 IOCP 的句柄。
        _stream = new FileStream(
            _logFilePath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.Read,
            bufferSize: Math.Max(pageSize, 4096),
            FileOptions.SequentialScan);

        _stream.Seek(0, SeekOrigin.End);
        SetHasPendingEntries(_stream.Length > 0);
    }

    /// <summary>
    /// 生成WAL文件路径，支持格式化占位符
    /// </summary>
    /// <param name="databaseFilePath">数据库文件路径</param>
    /// <param name="format">文件名格式，支持占位符：{name} = 数据库名称，{ext} = 原扩展名</param>
    /// <returns>WAL文件路径</returns>
    private static string GenerateWalFilePath(string databaseFilePath, string format)
    {
        if (string.IsNullOrWhiteSpace(format))
        {
            // 如果格式为空，使用默认行为
            return Path.ChangeExtension(databaseFilePath, ".wal");
        }

        var directory = Path.GetDirectoryName(databaseFilePath) ?? string.Empty;
        var fileNameWithoutExt = Path.GetFileNameWithoutExtension(databaseFilePath);
        var extension = Path.GetExtension(databaseFilePath).TrimStart('.');

        // 替换占位符
        var formattedFileName = format
            .Replace("{name}", fileNameWithoutExt)
            .Replace("{ext}", extension);

        if (Path.IsPathRooted(formattedFileName) ||
            !string.Equals(Path.GetFileName(formattedFileName), formattedFileName, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "WAL file name format must produce a file name in the database directory.",
                nameof(format));
        }

        // 确保文件名以.db结尾（如果格式中没有包含扩展名）
        if (!Path.HasExtension(formattedFileName) && !string.IsNullOrEmpty(extension))
        {
            formattedFileName += $".{extension}";
        }

        return Path.Combine(directory, formattedFileName);
    }

    private void Log(TinyDbLogLevel level, string message, Exception? ex = null)
    {
        TinyDbLogging.SafeLog(_log, level, message, ex);
    }

    private long _diskFlushCount;

    /// <summary>
    /// 真正刷到磁盘（FlushFileBuffers）的次数，仅供测试断言持久化语义。
    /// </summary>
    internal long DiskFlushCount => Interlocked.Read(ref _diskFlushCount);

    /// <summary>
    /// WAL 唯一的“刷到磁盘”入口。所有把日志标记为已持久化（SetFlushedLSN）之前的刷盘都必须经过这里。
    /// 注意：FileStream.FlushAsync 只会把托管缓冲区写给操作系统，不会调用 FlushFileBuffers，
    /// 不能用于持久化；.NET 也没有异步落盘 API，所以异步路径同样调用本方法。
    /// </summary>
    private void FlushStreamToDisk(FileStream stream)
    {
        stream.Flush(flushToDisk: true);
        Interlocked.Increment(ref _diskFlushCount);
    }

    private void TryDeleteExistingLog()
    {
        try
        {
            if (File.Exists(_logFilePath))
            {
                File.Delete(_logFilePath);
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Failed to delete existing log file '{_logFilePath}'.", ex);
        }
    }

    private static readonly TimeSpan DisposeLockTimeout = TimeSpan.FromSeconds(5);

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        // 先拿写锁，等进行中的持锁者（刷盘、截断、事务提交）完成当前 IO 再关闭文件。
        // 当前线程已在写锁内（例如在 Synchronize 回调里触发关闭）时不能再等，否则自锁。
        var holdsLockAlready = HasActiveWriteContext(null);
        var acquired = !holdsLockAlready && _mutex.Wait(DisposeLockTimeout);
        if (!acquired && !holdsLockAlready)
        {
            Log(TinyDbLogLevel.Warning, "WAL dispose timed out waiting for an in-flight write; closing the log anyway.");
        }

        try
        {
            _stream?.Dispose();
        }
        finally
        {
            if (acquired)
            {
                _mutex.Release();
            }

            // 有意不释放 _mutex：SemaphoreSlim.Dispose 不会唤醒已排队的等待者，它们会永远阻塞；
            // 未使用 AvailableWaitHandle 时它也不持有非托管资源。关闭后的等待者拿到锁后
            // 访问已释放的流，会得到明确的 ObjectDisposedException。
        }
    }

}
