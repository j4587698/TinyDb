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
    private readonly AsyncLocal<Guid?> _currentTransactionId = new();
    private readonly AsyncLocal<TransactionPageBuffer?> _currentTransactionPages = new();
    private readonly int _maxRecordSize;
    private bool _disposed;
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
        IWalCodec? walCodec)
    {
        if (string.IsNullOrWhiteSpace(databaseFilePath))
            throw new ArgumentException("Database file path cannot be null or empty", nameof(databaseFilePath));

        _log = logger ?? TinyDbLogging.NoopLogger;
        _walCodec = walCodec ?? new NoOpWalCodec();
        _maxRecordSize = Math.Max(pageSize * 2 + TransactionIdSize + BeforeLengthSize, pageSize) + _walCodec.MaxOverhead;
        IsEnabled = enabled;
        _logFilePath = GenerateWalFilePath(databaseFilePath, walFileNameFormat ?? "{name}-wal.{ext}");

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

        _stream = new FileStream(
            _logFilePath,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.Read,
            bufferSize: Math.Max(pageSize, 4096),
            FileOptions.Asynchronous | FileOptions.SequentialScan);

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

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try
        {
            _stream?.Dispose();
        }
        finally
        {
            _mutex.Dispose();
        }
    }

}
