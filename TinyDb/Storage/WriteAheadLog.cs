using System;
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
public sealed class WriteAheadLog : IDisposable
{
    private const byte EntryTypePage = 0x1;
    private const byte EntryTypeTransactionBegin = 0x2;
    private const byte EntryTypeTransactionPage = 0x3;
    private const byte EntryTypeTransactionCommit = 0x4;
    private const int HeaderSize = 9;
    private const int TransactionIdSize = 16;
    private const int BeforeLengthSize = sizeof(int);

    private readonly string _logFilePath;
    private readonly FileStream? _stream;
    private readonly Action<TinyDbLogLevel, string, Exception?> _log;
    private readonly IWalCodec _walCodec;
    private readonly SemaphoreSlim _mutex = new(1, 1);
    private readonly AsyncLocal<int> _synchronizationDepth = new();
    private readonly AsyncLocal<Guid?> _currentTransactionId = new();
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
    public long FlushedLSN => _flushedLSN;

    /// <summary>
    /// 是否有未提交的日志记录
    /// </summary>
    public bool HasPendingEntries => IsEnabled && HasPendingEntriesCore;

    private bool HasPendingEntriesCore => Volatile.Read(ref _hasPendingEntries) != 0;

    private void SetHasPendingEntries(bool value)
    {
        Volatile.Write(ref _hasPendingEntries, value ? 1 : 0);
    }

    internal bool RequiresBeforeImage => IsEnabled && _currentTransactionId.Value.HasValue;

    internal bool IsInTransactionScope => IsEnabled && _currentTransactionId.Value.HasValue;

    internal Action? BeforeTransactionCommitForTesting { get; set; }

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

    public async Task AppendPageAsync(Page page, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;

        if (_synchronizationDepth.Value > 0)
        {
            var data = PreparePageRecord(page);
            await WriteEntryAsync(EntryTypePage, page.PageID, data, cancellationToken).ConfigureAwait(false);
            SetHasPendingEntries(true);
            return;
        }

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var data = PreparePageRecord(page);
            await WriteEntryAsync(EntryTypePage, page.PageID, data, cancellationToken).ConfigureAwait(false);
            SetHasPendingEntries(true);
        }
        finally
        {
            _mutex.Release();
        }
    }

    public void AppendPage(Page page)
    {
        AppendPage(page, beforeImage: null);
    }

    public void AppendPage(Page page, byte[]? beforeImage)
    {
        if (!IsEnabled) return;

        if (_synchronizationDepth.Value > 0)
        {
            if (_currentTransactionId.Value is Guid transactionId)
            {
                var data = PrepareTransactionPageRecord(transactionId, page, beforeImage);
                WriteEntry(EntryTypeTransactionPage, page.PageID, data);
            }
            else
            {
                var data = PreparePageRecord(page);
                WriteEntry(EntryTypePage, page.PageID, data);
            }
            SetHasPendingEntries(true);
            return;
        }

        _mutex.Wait();
        try
        {
            var data = PreparePageRecord(page);
            WriteEntry(EntryTypePage, page.PageID, data);
            SetHasPendingEntries(true);
        }
        finally
        {
            _mutex.Release();
        }
    }

    internal WalTransactionScope BeginTransaction(Guid transactionId)
    {
        if (!IsEnabled)
        {
            return new WalTransactionScope(this, transactionId, ownsMutex: false);
        }

        _mutex.Wait();
        try
        {
            _synchronizationDepth.Value++;
            _currentTransactionId.Value = transactionId;
            WriteEntry(EntryTypeTransactionBegin, 0, CreateTransactionControlData(transactionId));
            SetHasPendingEntries(true);
            return new WalTransactionScope(this, transactionId, ownsMutex: true);
        }
        catch
        {
            _currentTransactionId.Value = null;
            _synchronizationDepth.Value--;
            _mutex.Release();
            throw;
        }
    }

    internal sealed class WalTransactionScope : IDisposable
    {
        private readonly WriteAheadLog _wal;
        private readonly Guid _transactionId;
        private readonly bool _ownsMutex;
        private bool _completed;
        private bool _disposed;

        internal WalTransactionScope(WriteAheadLog wal, Guid transactionId, bool ownsMutex)
        {
            _wal = wal;
            _transactionId = transactionId;
            _ownsMutex = ownsMutex;
        }

        public void Commit()
        {
            if (_completed) return;
            if (_wal.IsEnabled)
            {
                if (_wal._currentTransactionId.Value != _transactionId)
                {
                    throw new InvalidOperationException("WAL transaction scope mismatch.");
                }

                _wal.BeforeTransactionCommitForTesting?.Invoke();
                _wal.WriteEntry(EntryTypeTransactionCommit, 0, CreateTransactionControlData(_transactionId));
                _wal.SetHasPendingEntries(true);
                _wal.FlushLogCore();
            }

            _completed = true;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (!_ownsMutex) return;

            _wal._currentTransactionId.Value = null;
            _wal._synchronizationDepth.Value--;
            _wal._mutex.Release();
        }
    }

    private byte[] PreparePageRecord(Page page)
    {
        var stream = _stream!;
        long lsn = stream.Position;

        var header = page.Header;
        header.LSN = lsn;
        page.UpdateHeader(header);
        page.UpdateChecksum();

        return page.Snapshot(includeUnusedTail: true);
    }

    private byte[] PrepareTransactionPageRecord(Guid transactionId, Page page, byte[]? beforeImage)
    {
        var afterImage = PreparePageRecord(page);
        var beforeLength = beforeImage?.Length ?? -1;
        var data = new byte[TransactionIdSize + BeforeLengthSize + Math.Max(beforeLength, 0) + afterImage.Length];
        transactionId.ToByteArray().CopyTo(data, 0);
        BinaryPrimitives.WriteInt32LittleEndian(data.AsSpan(TransactionIdSize, BeforeLengthSize), beforeLength);

        var offset = TransactionIdSize + BeforeLengthSize;
        if (beforeImage != null)
        {
            beforeImage.CopyTo(data.AsSpan(offset));
            offset += beforeImage.Length;
        }

        afterImage.CopyTo(data.AsSpan(offset));
        return data;
    }

    private static byte[] CreateTransactionControlData(Guid transactionId)
    {
        return transactionId.ToByteArray();
    }

    private void WriteEntry(byte entryType, uint pageId, byte[] data)
    {
        var stream = _stream!;
        var recordOffset = stream.Position;
        var payload = _walCodec.Encode(entryType, pageId, recordOffset, data);
        
        // 头部格式: [Type(1)] [PageId(4)] [Length(4)] [CRC32(4)]
        Span<byte> header = stackalloc byte[HeaderSize + 4]; // HeaderSize is 9, +4 for CRC = 13
        header[0] = entryType;
        BinaryPrimitives.WriteUInt32LittleEndian(header[1..5], pageId);
        BinaryPrimitives.WriteInt32LittleEndian(header[5..9], payload.Length);
        var crc32 = TinyCrc32.HashToUInt32(header[..HeaderSize], payload);
        BinaryPrimitives.WriteUInt32LittleEndian(header[9..13], crc32);
        
        stream.Write(header);
        stream.Write(payload, 0, payload.Length);
    }

    private async Task WriteEntryAsync(byte entryType, uint pageId, byte[] data, CancellationToken cancellationToken)
    {
        var stream = _stream!;
        var recordOffset = stream.Position;
        var payload = _walCodec.Encode(entryType, pageId, recordOffset, data);
        
        byte[] header = new byte[HeaderSize + 4];
        header[0] = entryType;
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(1, 4), pageId);
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(5, 4), payload.Length);
        var crc32 = TinyCrc32.HashToUInt32(header.AsSpan(0, HeaderSize), payload);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(9, 4), crc32);
        
        await stream.WriteAsync(header, 0, header.Length, cancellationToken).ConfigureAwait(false);
        await stream.WriteAsync(payload, 0, payload.Length, cancellationToken).ConfigureAwait(false);
    }

    public async Task FlushToLSNAsync(long targetLSN, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || targetLSN < _flushedLSN) return;

        if (_synchronizationDepth.Value > 0)
        {
            FlushToLSNCore(targetLSN);
            return;
        }

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            FlushToLSNCore(targetLSN);
        }
        finally
        {
            _mutex.Release();
        }
    }

    public void FlushToLSN(long targetLSN)
    {
        if (!IsEnabled || targetLSN < _flushedLSN) return;

        if (_synchronizationDepth.Value > 0)
        {
            FlushToLSNCore(targetLSN);
            return;
        }

        _mutex.Wait();
        try
        {
            FlushToLSNCore(targetLSN);
        }
        finally
        {
            _mutex.Release();
        }
    }

    private void FlushToLSNCore(long targetLSN)
    {
        if (targetLSN >= _flushedLSN)
        {
            _stream!.Flush(true);
            _flushedLSN = _stream.Position;
        }
    }

    public async Task FlushLogAsync(CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || !HasPendingEntriesCore) return;

        if (_synchronizationDepth.Value > 0)
        {
            FlushLogCore();
            return;
        }

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            FlushLogCore();
        }
        finally
        {
            _mutex.Release();
        }
    }

    public void FlushLog()
    {
        if (!IsEnabled || !HasPendingEntriesCore) return;

        if (_synchronizationDepth.Value > 0)
        {
            FlushLogCore();
            return;
        }

        _mutex.Wait();
        try
        {
            FlushLogCore();
        }
        finally
        {
            _mutex.Release();
        }
    }

    private void FlushLogCore()
    {
        if (HasPendingEntriesCore)
        {
            _stream!.Flush(true);
            _flushedLSN = _stream.Position;
        }
    }

    public void Synchronize(Action flushData)
    {
        if (flushData == null) throw new ArgumentNullException(nameof(flushData));

        if (!IsEnabled)
        {
            _synchronizationDepth.Value++;
            try
            {
                flushData();
            }
            finally
            {
                _synchronizationDepth.Value--;
            }
            return;
        }

        _mutex.Wait();
        try
        {
            var stream = _stream!;

            if (HasPendingEntriesCore)
            {
                stream.Flush(true);
                _flushedLSN = stream.Position;
            }

            _synchronizationDepth.Value++;
            try
            {
                flushData();
            }
            finally
            {
                _synchronizationDepth.Value--;
            }

            if (HasPendingEntriesCore)
            {
                stream.SetLength(0);
                stream.Seek(0, SeekOrigin.End);
                stream.Flush(true);
                SetHasPendingEntries(false);
                _flushedLSN = stream.Position;
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    public async Task SynchronizeAsync(Func<CancellationToken, Task> flushDataAsync, CancellationToken cancellationToken = default)
    {
        if (flushDataAsync == null) throw new ArgumentNullException(nameof(flushDataAsync));

        if (!IsEnabled)
        {
            _synchronizationDepth.Value++;
            try
            {
                await flushDataAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _synchronizationDepth.Value--;
            }
            return;
        }

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var stream = _stream!;

            if (HasPendingEntriesCore)
            {
                stream.Flush(true);
                _flushedLSN = stream.Position;
            }

            _synchronizationDepth.Value++;
            try
            {
                await flushDataAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _synchronizationDepth.Value--;
            }

            if (HasPendingEntriesCore)
            {
                stream.SetLength(0);
                stream.Seek(0, SeekOrigin.End);
                stream.Flush(true);
                SetHasPendingEntries(false);
                _flushedLSN = stream.Position;
            }
        }
        finally
        {
            _mutex.Release();
        }
    }

    private static bool TryReadTransactionId(byte[] data, out Guid transactionId)
    {
        transactionId = default;
        if (data.Length != TransactionIdSize) return false;

        transactionId = new Guid(data);
        return true;
    }

    private static bool TryReadTransactionPage(
        byte[] data,
        out Guid transactionId,
        out byte[]? beforeImage,
        out byte[] afterImage)
    {
        transactionId = default;
        beforeImage = null;
        afterImage = Array.Empty<byte>();

        if (data.Length < TransactionIdSize + BeforeLengthSize) return false;

        transactionId = new Guid(data.AsSpan(0, TransactionIdSize));
        var beforeLength = BinaryPrimitives.ReadInt32LittleEndian(data.AsSpan(TransactionIdSize, BeforeLengthSize));
        if (beforeLength < -1) return false;

        var offset = TransactionIdSize + BeforeLengthSize;
        if (beforeLength >= 0)
        {
            if (beforeLength > data.Length - offset) return false;
            beforeImage = data.AsSpan(offset, beforeLength).ToArray();
            offset += beforeLength;
        }

        if (offset >= data.Length) return false;

        afterImage = data.AsSpan(offset).ToArray();
        return true;
    }

    public void Replay(Action<uint, byte[]> apply)
    {
        Replay(apply, restore: null);
    }

    public void Replay(Action<uint, byte[]> apply, Action<uint, byte[]>? restore)
    {
        if (!IsEnabled) return;
        if (apply == null) throw new ArgumentNullException(nameof(apply));

        var stream = _stream!;
        _mutex.Wait();
        long lastSuccessfulPosition = 0;
        bool replayStoppedAtInvalidRecord = false;
        try
        {
            stream.Flush(true);
            stream.Seek(0, SeekOrigin.Begin);

            const int FullHeaderSize = HeaderSize + 4; // 13 bytes
            var headerBuffer = new byte[FullHeaderSize];
            var pendingTransactions = new Dictionary<Guid, List<(uint PageId, byte[]? BeforeImage, byte[] AfterImage)>>();

            while (stream.Position < stream.Length)
            {
                long currentEntryStart = stream.Position;

                var read = stream.Read(headerBuffer, 0, FullHeaderSize);
                if (read < HeaderSize)
                {
                    replayStoppedAtInvalidRecord = true;
                    break;
                }

                var entryType = headerBuffer[0];
                if (entryType is not (EntryTypePage or EntryTypeTransactionBegin or EntryTypeTransactionPage or EntryTypeTransactionCommit))
                {
                    Log(TinyDbLogLevel.Warning, $"Invalid entry type 0x{entryType:X} at {currentEntryStart}.");
                    replayStoppedAtInvalidRecord = true;
                    break;
                }

                var pageId = BinaryPrimitives.ReadUInt32LittleEndian(headerBuffer.AsSpan(1, 4));
                var length = BinaryPrimitives.ReadInt32LittleEndian(headerBuffer.AsSpan(5, 4));

                uint? expectedCrc = null;
                if (read >= FullHeaderSize)
                {
                    expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(headerBuffer.AsSpan(9, 4));
                }
                else
                {
                    if (read > HeaderSize)
                    {
                        Log(TinyDbLogLevel.Warning, $"Incomplete header at {currentEntryStart}.");
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }
                }

                if (length <= 0 || length > _maxRecordSize)
                {
                    Log(TinyDbLogLevel.Warning, $"Invalid record length {length} at {currentEntryStart}.");
                    replayStoppedAtInvalidRecord = true;
                    break;
                }

                var buffer = new byte[length];
                var offset = 0;
                while (offset < length)
                {
                    var chunk = stream.Read(buffer, offset, length - offset);
                    if (chunk == 0)
                    {
                        break;
                    }
                    offset += chunk;
                }

                if (offset != length)
                {
                    Log(TinyDbLogLevel.Warning, $"Incomplete data record at {currentEntryStart}.");
                    replayStoppedAtInvalidRecord = true;
                    break;
                }

                if (expectedCrc.HasValue)
                {
                    var actualCrc = TinyCrc32.HashToUInt32(headerBuffer.AsSpan(0, HeaderSize), buffer);
                    var legacyDataOnlyCrc = TinyCrc32.HashToUInt32(buffer);
                    if (actualCrc != expectedCrc.Value && legacyDataOnlyCrc != expectedCrc.Value)
                    {
                        if (_walCodec.IsEncrypted)
                        {
                            Log(TinyDbLogLevel.Warning, $"Encrypted WAL CRC mismatch at {currentEntryStart}.");
                            replayStoppedAtInvalidRecord = true;
                            break;
                        }

                        Log(TinyDbLogLevel.Warning, $"CRC mismatch at {currentEntryStart}.");
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }
                }

                try
                {
                    buffer = _walCodec.Decode(entryType, pageId, currentEntryStart, buffer);
                }
                catch (InvalidDataException ex) when (_walCodec.IsEncrypted)
                {
                    Log(TinyDbLogLevel.Warning, $"Encrypted WAL record is invalid at {currentEntryStart}.", ex);
                    replayStoppedAtInvalidRecord = true;
                    break;
                }

                if (entryType == EntryTypePage)
                {
                    apply(pageId, buffer);
                }
                else if (entryType == EntryTypeTransactionBegin)
                {
                    if (!TryReadTransactionId(buffer, out var transactionId))
                    {
                        Log(TinyDbLogLevel.Warning, $"Invalid transaction begin record at {currentEntryStart}.");
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }

                    pendingTransactions.TryAdd(transactionId, new List<(uint, byte[]?, byte[])>());
                }
                else if (entryType == EntryTypeTransactionPage)
                {
                    if (!TryReadTransactionPage(buffer, out var transactionId, out var beforeImage, out var afterImage))
                    {
                        Log(TinyDbLogLevel.Warning, $"Invalid transaction page record at {currentEntryStart}.");
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }

                    if (!pendingTransactions.TryGetValue(transactionId, out var records))
                    {
                        records = new List<(uint, byte[]?, byte[])>();
                        pendingTransactions[transactionId] = records;
                    }

                    records.Add((pageId, beforeImage, afterImage));
                }
                else if (entryType == EntryTypeTransactionCommit)
                {
                    if (!TryReadTransactionId(buffer, out var transactionId))
                    {
                        Log(TinyDbLogLevel.Warning, $"Invalid transaction commit record at {currentEntryStart}.");
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }

                    if (pendingTransactions.TryGetValue(transactionId, out var records))
                    {
                        foreach (var record in records)
                        {
                            apply(record.PageId, record.AfterImage);
                        }

                        pendingTransactions.Remove(transactionId);
                    }
                }

                lastSuccessfulPosition = stream.Position;
            }

            foreach (var records in pendingTransactions.Values)
            {
                for (int i = records.Count - 1; i >= 0; i--)
                {
                    var record = records[i];
                    if (record.BeforeImage != null)
                    {
                        (restore ?? apply)(record.PageId, record.BeforeImage);
                    }
                }
            }

            if (replayStoppedAtInvalidRecord)
            {
                stream.SetLength(lastSuccessfulPosition);
                stream.Flush(true);
            }
            else if (lastSuccessfulPosition > 0 || stream.Length > 0)
            {
                stream.SetLength(0);
                stream.Flush(true);
            }

            stream.Seek(0, SeekOrigin.End);
            SetHasPendingEntries(stream.Length > 0);
        }
        catch (Exception ex)
        {
            Exception? recoveryException = null;
            try
            {
                stream.SetLength(lastSuccessfulPosition);
            }
            catch (Exception recoverEx)
            {
                recoveryException = new InvalidOperationException(
                    "Failed to truncate WAL to last successful position during replay recovery.",
                    recoverEx);
            }

            if (recoveryException != null)
            {
                throw new AggregateException("Fatal error during WAL replay and replay recovery truncation failed.", ex, recoveryException);
            }

            throw;
        }
        finally
        {
            _mutex.Release();
        }
    }

    public Task ReplayAsync(Func<uint, byte[], Task> applyAsync, CancellationToken cancellationToken = default)
    {
        return ReplayAsync(applyAsync, restoreAsync: null, cancellationToken);
    }

    public async Task ReplayAsync(
        Func<uint, byte[], Task> applyAsync,
        Func<uint, byte[], Task>? restoreAsync,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;
        if (applyAsync == null) throw new ArgumentNullException(nameof(applyAsync));

        var stream = _stream!;
        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        long lastSuccessfulPosition = 0;
        bool replayStoppedAtInvalidRecord = false;
        try
        {
            stream.Flush(true);
            stream.Seek(0, SeekOrigin.Begin);

            const int FullHeaderSize = HeaderSize + 4; // 13 bytes
            var headerBuffer = new byte[FullHeaderSize];
            var pendingTransactions = new Dictionary<Guid, List<(uint PageId, byte[]? BeforeImage, byte[] AfterImage)>>();
            
            while (stream.Position < stream.Length)
            {
                long currentEntryStart = stream.Position;
                
                // 尝试读取完整头部
                var read = await stream.ReadAsync(headerBuffer, 0, FullHeaderSize, cancellationToken).ConfigureAwait(false);
                
                // 如果连最小头部 (HeaderSize = 9) 都读不够，说明文件结束或损坏
                if (read < HeaderSize)
                {
                    replayStoppedAtInvalidRecord = true;
                    break;
                }

                var entryType = headerBuffer[0];
                if (entryType is not (EntryTypePage or EntryTypeTransactionBegin or EntryTypeTransactionPage or EntryTypeTransactionCommit))
                {
                    Log(TinyDbLogLevel.Warning, $"Invalid entry type 0x{entryType:X} at {currentEntryStart}.");
                    replayStoppedAtInvalidRecord = true;
                    break; // 无效类型，停止重放
                }

                var pageId = BinaryPrimitives.ReadUInt32LittleEndian(headerBuffer.AsSpan(1, 4));
                var length = BinaryPrimitives.ReadInt32LittleEndian(headerBuffer.AsSpan(5, 4));
                
                // 读取 CRC32 (如果有)
                uint? expectedCrc = null;
                if (read >= FullHeaderSize)
                {
                    expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(headerBuffer.AsSpan(9, 4));
                }
                else
                {
                    if (read > HeaderSize) 
                    {
                        Log(TinyDbLogLevel.Warning, $"Incomplete header at {currentEntryStart}.");
                        replayStoppedAtInvalidRecord = true;
                        break; 
                    }
                }

                if (length <= 0 || length > _maxRecordSize)
                {
                    Log(TinyDbLogLevel.Warning, $"Invalid record length {length} at {currentEntryStart}.");
                    replayStoppedAtInvalidRecord = true;
                    break; // 无效长度
                }

                var buffer = new byte[length];
                var offset = 0;
                while (offset < length)
                {
                    var chunk = await stream.ReadAsync(buffer, offset, length - offset, cancellationToken).ConfigureAwait(false);
                    if (chunk == 0)
                    {
                        break;
                    }
                    offset += chunk;
                }

                if (offset != length)
                {
                    Log(TinyDbLogLevel.Warning, $"Incomplete data record at {currentEntryStart}.");
                    replayStoppedAtInvalidRecord = true;
                    break; // 数据不完整
                }

                // 验证校验和 (如果存在)
                if (expectedCrc.HasValue)
                {
                    var actualCrc = TinyCrc32.HashToUInt32(headerBuffer.AsSpan(0, HeaderSize), buffer);
                    var legacyDataOnlyCrc = TinyCrc32.HashToUInt32(buffer);
                    if (actualCrc != expectedCrc.Value && legacyDataOnlyCrc != expectedCrc.Value)
                    {
                        if (_walCodec.IsEncrypted)
                        {
                            Log(TinyDbLogLevel.Warning, $"Encrypted WAL CRC mismatch at {currentEntryStart}.");
                            replayStoppedAtInvalidRecord = true;
                            break;
                        }

                        Log(TinyDbLogLevel.Warning, $"CRC mismatch at {currentEntryStart}.");
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }
                }

                try
                {
                    buffer = _walCodec.Decode(entryType, pageId, currentEntryStart, buffer);
                }
                catch (InvalidDataException ex) when (_walCodec.IsEncrypted)
                {
                    Log(TinyDbLogLevel.Warning, $"Encrypted WAL record is invalid at {currentEntryStart}.", ex);
                    replayStoppedAtInvalidRecord = true;
                    break;
                }

                if (entryType == EntryTypePage)
                {
                    await applyAsync(pageId, buffer).ConfigureAwait(false);
                }
                else if (entryType == EntryTypeTransactionBegin)
                {
                    if (!TryReadTransactionId(buffer, out var transactionId))
                    {
                        Log(TinyDbLogLevel.Warning, $"Invalid transaction begin record at {currentEntryStart}.");
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }

                    pendingTransactions.TryAdd(transactionId, new List<(uint, byte[]?, byte[])>());
                }
                else if (entryType == EntryTypeTransactionPage)
                {
                    if (!TryReadTransactionPage(buffer, out var transactionId, out var beforeImage, out var afterImage))
                    {
                        Log(TinyDbLogLevel.Warning, $"Invalid transaction page record at {currentEntryStart}.");
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }

                    if (!pendingTransactions.TryGetValue(transactionId, out var records))
                    {
                        records = new List<(uint, byte[]?, byte[])>();
                        pendingTransactions[transactionId] = records;
                    }

                    records.Add((pageId, beforeImage, afterImage));
                }
                else if (entryType == EntryTypeTransactionCommit)
                {
                    if (!TryReadTransactionId(buffer, out var transactionId))
                    {
                        Log(TinyDbLogLevel.Warning, $"Invalid transaction commit record at {currentEntryStart}.");
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }

                    if (pendingTransactions.TryGetValue(transactionId, out var records))
                    {
                        foreach (var record in records)
                        {
                            await applyAsync(record.PageId, record.AfterImage).ConfigureAwait(false);
                        }

                        pendingTransactions.Remove(transactionId);
                    }
                }

                lastSuccessfulPosition = stream.Position;
            }

            // 清理或截断日志
            foreach (var records in pendingTransactions.Values)
            {
                for (int i = records.Count - 1; i >= 0; i--)
                {
                    var record = records[i];
                    if (record.BeforeImage != null)
                    {
                        await (restoreAsync ?? applyAsync)(record.PageId, record.BeforeImage).ConfigureAwait(false);
                    }
                }
            }

            if (replayStoppedAtInvalidRecord)
            {
                stream.SetLength(lastSuccessfulPosition);
                stream.Flush(true);
            }
            else if (lastSuccessfulPosition > 0 || stream.Length > 0)
            {
                // 如果没有处理完全部文件（因为损坏或截断），则将文件截断到最后一个有效的记录处
                stream.SetLength(0);
                stream.Flush(true);
            }
            else if (lastSuccessfulPosition > 0)
            {
                // 全部重放成功，清空日志
                stream.SetLength(0);
                stream.Flush(true);
            }

            stream.Seek(0, SeekOrigin.End);
            SetHasPendingEntries(stream.Length > 0);
        }
        catch (Exception ex)
        {
            Exception? recoveryException = null;
            // 尽力而为：截断到已知正确的位置
            try
            {
                stream.SetLength(lastSuccessfulPosition);
            }
            catch (Exception recoverEx)
            {
                recoveryException = new InvalidOperationException(
                    "Failed to truncate WAL to last successful position during replay recovery.",
                    recoverEx);
            }

            if (recoveryException != null)
            {
                throw new AggregateException("Fatal error during WAL replay and replay recovery truncation failed.", ex, recoveryException);
            }

            throw;
        }
        finally
        {
            _mutex.Release();
        }
    }

    public async Task TruncateAsync(CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var stream = _stream!;
            stream.SetLength(0);
            stream.Seek(0, SeekOrigin.End);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            SetHasPendingEntries(false);
        }
        finally
        {
            _mutex.Release();
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
