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
public sealed class WriteAheadLog : IDisposable
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

    private bool HasActiveWriteContext(WriteLockContext? context)
    {
        return context?.IsActiveFor(this) == true ||
               s_currentWriteContext.Value?.IsActiveFor(this) == true;
    }

    private static void RunWithCurrentThreadWriteContext(WriteLockContext context, Action action)
    {
        var previousContext = s_currentWriteContext.Value;
        s_currentWriteContext.Value = context;
        try
        {
            action();
        }
        finally
        {
            s_currentWriteContext.Value = previousContext;
        }
    }

    private static async Task RunWithCurrentThreadWriteContextAsync(WriteLockContext context, Func<Task> action)
    {
        var previousContext = s_currentWriteContext.Value;
        s_currentWriteContext.Value = context;
        try
        {
            await action().ConfigureAwait(false);
        }
        finally
        {
            s_currentWriteContext.Value = previousContext;
        }
    }

    private void RunWithWriteLock(Action<WriteLockContext> action)
    {
        _mutex.Wait();
        var context = new WriteLockContext(this);
        try
        {
            RunWithCurrentThreadWriteContext(context, () => action(context));
        }
        finally
        {
            context.Deactivate();
            _mutex.Release();
        }
    }

    private async Task RunWithWriteLockAsync(Func<WriteLockContext, Task> action, CancellationToken cancellationToken)
    {
        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        var context = new WriteLockContext(this);
        try
        {
            await RunWithCurrentThreadWriteContextAsync(context, () => action(context)).ConfigureAwait(false);
        }
        finally
        {
            context.Deactivate();
            _mutex.Release();
        }
    }

    internal bool RequiresBeforeImage => IsEnabled && _currentTransactionId.Value.HasValue;

    internal bool IsInTransactionScope => IsEnabled && _currentTransactionId.Value.HasValue;

    internal Action? BeforeTransactionCommitForTesting { get; set; }
    internal Action<uint, long>? DeferredTransactionPageLogged { get; set; }

    private sealed class PendingTransactionPage
    {
        public PendingTransactionPage(uint pageId, byte[]? beforeImage, byte[] afterImage, bool needsWalWrite)
        {
            PageId = pageId;
            BeforeImage = beforeImage;
            AfterImage = afterImage;
            NeedsWalWrite = needsWalWrite;
        }

        public uint PageId { get; }
        public byte[]? BeforeImage { get; }
        public byte[] AfterImage { get; set; }
        public bool NeedsWalWrite { get; set; }
    }

    private sealed class TransactionPageBuffer
    {
        private readonly Dictionary<uint, PendingTransactionPage> _pages = new();
        private readonly List<uint> _order = new();

        public TransactionPageBuffer(Guid transactionId)
        {
            TransactionId = transactionId;
        }

        public Guid TransactionId { get; }

        public void AddOrReplace(uint pageId, byte[]? beforeImage, byte[] afterImage, bool needsWalWrite)
        {
            if (!_pages.TryGetValue(pageId, out var pending))
            {
                _pages.Add(pageId, new PendingTransactionPage(pageId, beforeImage, afterImage, needsWalWrite));
                _order.Add(pageId);
                return;
            }

            pending.AfterImage = afterImage;
            pending.NeedsWalWrite = needsWalWrite;
        }

        public IEnumerable<(uint PageId, byte[] AfterImage, byte[]? BeforeImage)> GetPagesPendingWalWriteInFirstTouchOrder()
        {
            foreach (var pageId in _order)
            {
                var pending = _pages[pageId];
                if (!pending.NeedsWalWrite) continue;
                yield return (pending.PageId, pending.AfterImage, pending.BeforeImage);
            }
        }

        public IEnumerable<(uint PageId, byte[]? BeforeImage)> GetPagesInReverseFirstTouchOrder()
        {
            for (var i = _order.Count - 1; i >= 0; i--)
            {
                var pending = _pages[_order[i]];
                yield return (pending.PageId, pending.BeforeImage);
            }
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

    public async Task AppendPageAsync(Page page, CancellationToken cancellationToken = default)
    {
        await AppendPageAsync(page, beforeImage: null, writeContext: null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task AppendPageAsync(Page page, byte[]? beforeImage, CancellationToken cancellationToken = default)
    {
        await AppendPageAsync(page, beforeImage, writeContext: null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task AppendPageAsync(
        Page page,
        byte[]? beforeImage,
        WriteLockContext? writeContext,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;
        if (page == null) throw new ArgumentNullException(nameof(page));

        if (HasActiveWriteContext(writeContext))
        {
            await AppendPageCoreAsync(page, beforeImage, cancellationToken).ConfigureAwait(false);
            return;
        }

        await RunWithWriteLockAsync(
            _ => AppendPageCoreAsync(page, beforeImage, cancellationToken),
            cancellationToken).ConfigureAwait(false);
    }

    private async Task AppendPageCoreAsync(Page page, byte[]? beforeImage, CancellationToken cancellationToken)
    {
        if (_currentTransactionId.Value is Guid transactionId)
        {
            var afterImage = PreparePageRecord(page);
            TrackTransactionPage(transactionId, page.PageID, beforeImage, afterImage, needsWalWrite: false);
            var transactionData = CreateTransactionPageRecord(transactionId, beforeImage, afterImage);
            await WriteEntryAsync(EntryTypeTransactionPage, page.PageID, transactionData, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var data = PreparePageRecord(page);
            await WriteEntryAsync(EntryTypePage, page.PageID, data, cancellationToken).ConfigureAwait(false);
        }

        SetHasPendingEntries(true);
    }

    public void AppendPage(Page page)
    {
        AppendPage(page, beforeImage: null);
    }

    internal void AppendPageDeferred(Page page, byte[]? beforeImage)
    {
        if (!IsEnabled) return;
        if (page == null) throw new ArgumentNullException(nameof(page));

        if (_currentTransactionId.Value is Guid transactionId)
        {
            BufferDeferredTransactionPage(transactionId, page, beforeImage);
            SetHasPendingEntries(true);
            return;
        }

        AppendPage(page, beforeImage);
    }

    public void AppendPage(Page page, byte[]? beforeImage)
    {
        AppendPage(page, beforeImage, writeContext: null);
    }

    internal void AppendPage(Page page, byte[]? beforeImage, WriteLockContext? writeContext)
    {
        if (!IsEnabled) return;
        if (page == null) throw new ArgumentNullException(nameof(page));

        if (HasActiveWriteContext(writeContext))
        {
            AppendPageCore(page, beforeImage);
            return;
        }

        RunWithWriteLock(_ => AppendPageCore(page, beforeImage));
    }

    private void AppendPageCore(Page page, byte[]? beforeImage)
    {
        if (_currentTransactionId.Value is Guid transactionId)
        {
            var afterImage = PreparePageRecord(page);
            TrackTransactionPage(transactionId, page.PageID, beforeImage, afterImage, needsWalWrite: false);
            var data = CreateTransactionPageRecord(transactionId, beforeImage, afterImage);
            WriteEntry(EntryTypeTransactionPage, page.PageID, data);
        }
        else
        {
            var data = PreparePageRecord(page);
            WriteEntry(EntryTypePage, page.PageID, data);
        }

        SetHasPendingEntries(true);
    }

    private void BufferDeferredTransactionPage(Guid transactionId, Page page, byte[]? beforeImage)
    {
        var transactionPages = _currentTransactionPages.Value;
        if (transactionPages == null || transactionPages.TransactionId != transactionId)
        {
            throw new InvalidOperationException("WAL transaction page buffer mismatch.");
        }

        page.UpdateLsnForWal(PendingDeferredTransactionLsn);
        transactionPages.AddOrReplace(page.PageID, beforeImage, page.Snapshot(includeUnusedTail: true), needsWalWrite: true);
    }

    private void TrackTransactionPage(Guid transactionId, uint pageId, byte[]? beforeImage, byte[] afterImage, bool needsWalWrite)
    {
        var transactionPages = _currentTransactionPages.Value;
        if (transactionPages == null || transactionPages.TransactionId != transactionId)
        {
            throw new InvalidOperationException("WAL transaction page buffer mismatch.");
        }

        transactionPages.AddOrReplace(pageId, beforeImage, afterImage, needsWalWrite);
    }

    internal WalTransactionScope BeginTransaction(Guid transactionId) => BeginTransaction(transactionId, flushOnCommit: true);

    internal WalTransactionScope BeginTransaction(Guid transactionId, bool flushOnCommit)
    {
        if (!IsEnabled)
        {
            return new WalTransactionScope(this, transactionId, ownsContext: false, flushOnCommit);
        }

        try
        {
            _currentTransactionId.Value = transactionId;
            _currentTransactionPages.Value = new TransactionPageBuffer(transactionId);
            RunWithWriteLock(_ =>
            {
                WriteEntry(EntryTypeTransactionBegin, 0, CreateTransactionControlData(transactionId));
                SetHasPendingEntries(true);
            });
            return new WalTransactionScope(this, transactionId, ownsContext: true, flushOnCommit);
        }
        catch
        {
            _currentTransactionPages.Value = null;
            _currentTransactionId.Value = null;
            throw;
        }
    }

    internal Task WriteTransactionBeginAsync(Guid transactionId, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled)
        {
            return Task.CompletedTask;
        }

        return RunWithWriteLockAsync(async _ =>
        {
            await WriteEntryAsync(
                EntryTypeTransactionBegin,
                0,
                CreateTransactionControlData(transactionId),
                cancellationToken).ConfigureAwait(false);
            SetHasPendingEntries(true);
        }, cancellationToken);
    }

    internal WalTransactionScope EnterTransactionContext(Guid transactionId, bool flushOnCommit)
    {
        if (!IsEnabled)
        {
            return new WalTransactionScope(this, transactionId, ownsContext: false, flushOnCommit);
        }

        _currentTransactionId.Value = transactionId;
        _currentTransactionPages.Value = new TransactionPageBuffer(transactionId);
        return new WalTransactionScope(this, transactionId, ownsContext: true, flushOnCommit);
    }

    internal sealed class WalTransactionScope : IDisposable
    {
        private readonly WriteAheadLog _wal;
        private readonly Guid _transactionId;
        private readonly bool _ownsContext;
        private readonly bool _flushOnCommit;
        private bool _completed;
        private bool _disposed;

        internal WalTransactionScope(WriteAheadLog wal, Guid transactionId, bool ownsContext, bool flushOnCommit)
        {
            _wal = wal;
            _transactionId = transactionId;
            _ownsContext = ownsContext;
            _flushOnCommit = flushOnCommit;
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

                _wal.RunWithWriteLock(_ =>
                {
                    _wal.WriteDeferredTransactionPages(_transactionId);
                    _wal.BeforeTransactionCommitForTesting?.Invoke();
                    _wal.WriteEntry(EntryTypeTransactionCommit, 0, CreateTransactionControlData(_transactionId));
                    _wal.SetHasPendingEntries(true);
                    if (_flushOnCommit)
                    {
                        _wal.FlushLogCore();
                    }
                });
            }

            _completed = true;
        }

        public async Task CommitAsync(CancellationToken cancellationToken = default)
        {
            if (_completed) return;
            if (_wal.IsEnabled)
            {
                if (_wal._currentTransactionId.Value != _transactionId)
                {
                    throw new InvalidOperationException("WAL transaction scope mismatch.");
                }

                await _wal.RunWithWriteLockAsync(async _ =>
                {
                    await _wal.WriteDeferredTransactionPagesAsync(_transactionId, cancellationToken).ConfigureAwait(false);
                    _wal.BeforeTransactionCommitForTesting?.Invoke();
                    await _wal.WriteEntryAsync(
                        EntryTypeTransactionCommit,
                        0,
                        CreateTransactionControlData(_transactionId),
                        cancellationToken).ConfigureAwait(false);
                    _wal.SetHasPendingEntries(true);
                    if (_flushOnCommit)
                    {
                        _wal.FlushLogCore();
                    }
                }, cancellationToken).ConfigureAwait(false);
            }

            _completed = true;
        }

        public void Rollback(Action<uint, byte[]> restore)
        {
            if (_completed) return;
            if (restore == null) throw new ArgumentNullException(nameof(restore));

            if (_wal.IsEnabled)
            {
                if (_wal._currentTransactionId.Value != _transactionId)
                {
                    throw new InvalidOperationException("WAL transaction scope mismatch.");
                }

                _wal.RestoreTransactionBeforeImages(_transactionId, restore);
            }

            _completed = true;
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            if (!_ownsContext) return;

            _wal._currentTransactionPages.Value = null;
            _wal._currentTransactionId.Value = null;
        }
    }

    private void WriteDeferredTransactionPages(Guid transactionId)
    {
        var transactionPages = _currentTransactionPages.Value;
        if (transactionPages == null)
        {
            return;
        }

        if (transactionPages.TransactionId != transactionId)
        {
            throw new InvalidOperationException("WAL transaction page buffer mismatch.");
        }

        foreach (var record in transactionPages.GetPagesPendingWalWriteInFirstTouchOrder())
        {
            var data = PrepareTransactionPageRecord(transactionId, record.AfterImage, record.BeforeImage, out var lsn);
            WriteEntry(EntryTypeTransactionPage, record.PageId, data);
            DeferredTransactionPageLogged?.Invoke(record.PageId, lsn);
        }
    }

    private async Task WriteDeferredTransactionPagesAsync(Guid transactionId, CancellationToken cancellationToken)
    {
        var transactionPages = _currentTransactionPages.Value;
        if (transactionPages == null)
        {
            return;
        }

        if (transactionPages.TransactionId != transactionId)
        {
            throw new InvalidOperationException("WAL transaction page buffer mismatch.");
        }

        foreach (var record in transactionPages.GetPagesPendingWalWriteInFirstTouchOrder())
        {
            var data = PrepareTransactionPageRecord(transactionId, record.AfterImage, record.BeforeImage, out var lsn);
            await WriteEntryAsync(EntryTypeTransactionPage, record.PageId, data, cancellationToken).ConfigureAwait(false);
            DeferredTransactionPageLogged?.Invoke(record.PageId, lsn);
        }
    }

    private void RestoreTransactionBeforeImages(Guid transactionId, Action<uint, byte[]> restore)
    {
        var transactionPages = _currentTransactionPages.Value;
        if (transactionPages == null)
        {
            return;
        }

        if (transactionPages.TransactionId != transactionId)
        {
            throw new InvalidOperationException("WAL transaction page buffer mismatch.");
        }

        foreach (var record in transactionPages.GetPagesInReverseFirstTouchOrder())
        {
            if (record.BeforeImage != null)
            {
                restore(record.PageId, record.BeforeImage);
            }
        }
    }

    private byte[] PreparePageRecord(Page page)
    {
        var stream = _stream!;
        long lsn = stream.Position;

        page.UpdateLsnForWal(lsn);
        page.UpdateChecksum();

        return page.Snapshot(includeUnusedTail: true);
    }

    private byte[] PreparePageRecord(byte[] afterImageSnapshot, long lsn)
    {
        if (afterImageSnapshot == null) throw new ArgumentNullException(nameof(afterImageSnapshot));
        if (afterImageSnapshot.Length < PageHeader.Size)
        {
            throw new InvalidDataException("Deferred WAL page snapshot is too small.");
        }

        var afterImage = new byte[afterImageSnapshot.Length];
        Array.Copy(afterImageSnapshot, afterImage, afterImage.Length);

        BinaryPrimitives.WriteInt64LittleEndian(afterImage.AsSpan(PageLsnOffset, sizeof(long)), lsn);
        BinaryPrimitives.WriteUInt32LittleEndian(afterImage.AsSpan(PageChecksumOffset, sizeof(uint)), 0);
        var checksum = TinyCrc32.HashToUInt32WithZeroedRange(afterImage, PageChecksumOffset, sizeof(uint));
        BinaryPrimitives.WriteUInt32LittleEndian(afterImage.AsSpan(PageChecksumOffset, sizeof(uint)), checksum);

        return afterImage;
    }

    private byte[] PrepareTransactionPageRecord(Guid transactionId, Page page, byte[]? beforeImage)
    {
        var afterImage = PreparePageRecord(page);
        return CreateTransactionPageRecord(transactionId, beforeImage, afterImage);
    }

    private byte[] PrepareTransactionPageRecord(Guid transactionId, byte[] afterImageSnapshot, byte[]? beforeImage, out long lsn)
    {
        lsn = _stream!.Position;
        var afterImage = PreparePageRecord(afterImageSnapshot, lsn);
        return CreateTransactionPageRecord(transactionId, beforeImage, afterImage);
    }

    private static byte[] CreateTransactionPageRecord(Guid transactionId, byte[]? beforeImage, byte[] afterImage)
    {
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

        const int headerLength = HeaderSize + 4;
        var recordLength = headerLength + payload.Length;
        var record = ArrayPool<byte>.Shared.Rent(recordLength);
        try
        {
            var recordSpan = record.AsSpan(0, recordLength);
            var headerSpan = recordSpan[..headerLength];
            headerSpan[0] = entryType;
            BinaryPrimitives.WriteUInt32LittleEndian(headerSpan[1..5], pageId);
            BinaryPrimitives.WriteInt32LittleEndian(headerSpan[5..9], payload.Length);
            var crc32 = TinyCrc32.HashToUInt32(headerSpan[..HeaderSize], payload);
            BinaryPrimitives.WriteUInt32LittleEndian(headerSpan[9..13], crc32);
            payload.CopyTo(recordSpan[headerLength..]);

            await stream.WriteAsync(record, 0, recordLength, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(record, clearArray: _walCodec.IsEncrypted);
        }
    }

    public async Task FlushToLSNAsync(long targetLSN, CancellationToken cancellationToken = default)
    {
        await FlushToLSNAsync(targetLSN, writeContext: null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task FlushToLSNAsync(
        long targetLSN,
        WriteLockContext? writeContext,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || targetLSN < _flushedLSN) return;

        if (HasActiveWriteContext(writeContext))
        {
            FlushToLSNCore(targetLSN);
            return;
        }

        await RunWithWriteLockAsync(_ =>
        {
            FlushToLSNCore(targetLSN);
            return Task.CompletedTask;
        }, cancellationToken).ConfigureAwait(false);
    }

    public void FlushToLSN(long targetLSN)
    {
        FlushToLSN(targetLSN, writeContext: null);
    }

    internal void FlushToLSN(long targetLSN, WriteLockContext? writeContext)
    {
        if (!IsEnabled || targetLSN < _flushedLSN) return;

        if (HasActiveWriteContext(writeContext))
        {
            FlushToLSNCore(targetLSN);
            return;
        }

        RunWithWriteLock(_ => FlushToLSNCore(targetLSN));
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
        await FlushLogAsync(writeContext: null, cancellationToken).ConfigureAwait(false);
    }

    internal async Task FlushLogAsync(
        WriteLockContext? writeContext,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || !HasPendingEntriesCore) return;

        if (HasActiveWriteContext(writeContext))
        {
            FlushLogCore();
            return;
        }

        await RunWithWriteLockAsync(_ =>
        {
            FlushLogCore();
            return Task.CompletedTask;
        }, cancellationToken).ConfigureAwait(false);
    }

    public void FlushLog()
    {
        FlushLog(writeContext: null);
    }

    internal void FlushLog(WriteLockContext? writeContext)
    {
        if (!IsEnabled || !HasPendingEntriesCore) return;

        if (HasActiveWriteContext(writeContext))
        {
            FlushLogCore();
            return;
        }

        RunWithWriteLock(_ => FlushLogCore());
    }

    private void FlushLogCore()
    {
        if (HasPendingEntriesCore)
        {
            _stream!.Flush(true);
            _flushedLSN = _stream.Position;
        }
    }

    public void Synchronize(Action flushData) => Synchronize(flushData, truncateLog: true);

    internal void Synchronize(Action flushData, bool truncateLog)
    {
        if (flushData == null) throw new ArgumentNullException(nameof(flushData));
        Synchronize(_ => flushData(), truncateLog);
    }

    internal void Synchronize(Action<WriteLockContext> flushData, bool truncateLog)
    {
        if (flushData == null) throw new ArgumentNullException(nameof(flushData));

        if (!IsEnabled)
        {
            var disabledContext = new WriteLockContext(this);
            try
            {
                RunWithCurrentThreadWriteContext(disabledContext, () => flushData(disabledContext));
            }
            finally
            {
                disabledContext.Deactivate();
            }
            return;
        }

        _mutex.Wait();
        var context = new WriteLockContext(this);
        try
        {
            var stream = _stream!;

            if (HasPendingEntriesCore)
            {
                stream.Flush(true);
                _flushedLSN = stream.Position;
            }

            RunWithCurrentThreadWriteContext(context, () => flushData(context));

            if (HasPendingEntriesCore)
            {
                if (truncateLog || stream.Length >= DeferredTruncateThresholdBytes)
                {
                    stream.SetLength(0);
                    stream.Seek(0, SeekOrigin.End);
                    stream.Flush(true);
                }

                SetHasPendingEntries(false);
                _flushedLSN = stream.Position;
            }
        }
        finally
        {
            context.Deactivate();
            _mutex.Release();
        }
    }

    public Task SynchronizeAsync(Func<CancellationToken, Task> flushDataAsync, CancellationToken cancellationToken = default)
    {
        return SynchronizeAsync(flushDataAsync, truncateLog: true, cancellationToken);
    }

    internal async Task SynchronizeAsync(
        Func<CancellationToken, Task> flushDataAsync,
        bool truncateLog,
        CancellationToken cancellationToken = default)
    {
        if (flushDataAsync == null) throw new ArgumentNullException(nameof(flushDataAsync));
        await SynchronizeAsync((_, ct) => flushDataAsync(ct), truncateLog, cancellationToken).ConfigureAwait(false);
    }

    internal async Task SynchronizeAsync(
        Func<WriteLockContext, CancellationToken, Task> flushDataAsync,
        bool truncateLog,
        CancellationToken cancellationToken = default)
    {
        if (flushDataAsync == null) throw new ArgumentNullException(nameof(flushDataAsync));

        if (!IsEnabled)
        {
            var disabledContext = new WriteLockContext(this);
            try
            {
                await flushDataAsync(disabledContext, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                disabledContext.Deactivate();
            }
            return;
        }

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        var context = new WriteLockContext(this);
        try
        {
            var stream = _stream!;

            if (HasPendingEntriesCore)
            {
                stream.Flush(true);
                _flushedLSN = stream.Position;
            }

            await flushDataAsync(context, cancellationToken).ConfigureAwait(false);

            if (HasPendingEntriesCore)
            {
                if (truncateLog || stream.Length >= DeferredTruncateThresholdBytes)
                {
                    stream.SetLength(0);
                    stream.Seek(0, SeekOrigin.End);
                    stream.Flush(true);
                }

                SetHasPendingEntries(false);
                _flushedLSN = stream.Position;
            }
        }
        finally
        {
            context.Deactivate();
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

                var rentedBuffer = ArrayPool<byte>.Shared.Rent(length);
                byte[]? decodedBuffer = null;
                try
                {
                    var offset = 0;
                    while (offset < length)
                    {
                        var chunk = stream.Read(rentedBuffer, offset, length - offset);
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
                        if (!ValidateRecordCrc(headerBuffer, rentedBuffer.AsSpan(0, length), expectedCrc.Value, currentEntryStart))
                        {
                            replayStoppedAtInvalidRecord = true;
                            break;
                        }
                    }

                    try
                    {
                        decodedBuffer = _walCodec.Decode(entryType, pageId, currentEntryStart, rentedBuffer, length);
                    }
                    catch (InvalidDataException ex) when (_walCodec.IsEncrypted)
                    {
                        Log(TinyDbLogLevel.Warning, $"Encrypted WAL record is invalid at {currentEntryStart}.", ex);
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }

                    if (ReferenceEquals(decodedBuffer, rentedBuffer))
                    {
                        decodedBuffer = rentedBuffer.AsSpan(0, length).ToArray();
                    }

                    var buffer = decodedBuffer;
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
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer, clearArray: _walCodec.IsEncrypted);
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

                var rentedBuffer = ArrayPool<byte>.Shared.Rent(length);
                byte[]? decodedBuffer = null;
                try
                {
                    var offset = 0;
                    while (offset < length)
                    {
                        var chunk = await stream.ReadAsync(rentedBuffer, offset, length - offset, cancellationToken).ConfigureAwait(false);
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
                        if (!ValidateRecordCrc(headerBuffer, rentedBuffer.AsSpan(0, length), expectedCrc.Value, currentEntryStart))
                        {
                            replayStoppedAtInvalidRecord = true;
                            break;
                        }
                    }

                    try
                    {
                        decodedBuffer = _walCodec.Decode(entryType, pageId, currentEntryStart, rentedBuffer, length);
                    }
                    catch (InvalidDataException ex) when (_walCodec.IsEncrypted)
                    {
                        Log(TinyDbLogLevel.Warning, $"Encrypted WAL record is invalid at {currentEntryStart}.", ex);
                        replayStoppedAtInvalidRecord = true;
                        break;
                    }

                    if (ReferenceEquals(decodedBuffer, rentedBuffer))
                    {
                        decodedBuffer = rentedBuffer.AsSpan(0, length).ToArray();
                    }

                    var buffer = decodedBuffer;
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
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(rentedBuffer, clearArray: _walCodec.IsEncrypted);
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

    private bool ValidateRecordCrc(byte[] headerBuffer, ReadOnlySpan<byte> buffer, uint expectedCrc, long recordOffset)
    {
        var actualCrc = TinyCrc32.HashToUInt32(headerBuffer.AsSpan(0, HeaderSize), buffer);
        if (actualCrc == expectedCrc)
        {
            return true;
        }

        var legacyDataOnlyCrc = TinyCrc32.HashToUInt32(buffer);
        if (legacyDataOnlyCrc == expectedCrc)
        {
            Log(TinyDbLogLevel.Warning, $"WAL record at {recordOffset} matched legacy data-only CRC; header-inclusive CRC mismatch.");
            return true;
        }

        if (_walCodec.IsEncrypted)
        {
            Log(TinyDbLogLevel.Warning, $"Encrypted WAL CRC mismatch at {recordOffset}.");
            return false;
        }

        Log(TinyDbLogLevel.Warning, $"CRC mismatch at {recordOffset}.");
        return false;
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
            _flushedLSN = stream.Position;
        }
        finally
        {
            _mutex.Release();
        }
    }

    public void Truncate()
    {
        if (!IsEnabled) return;

        _mutex.Wait();
        try
        {
            var stream = _stream!;
            stream.SetLength(0);
            stream.Seek(0, SeekOrigin.End);
            stream.Flush(true);
            SetHasPendingEntries(false);
            _flushedLSN = stream.Position;
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
