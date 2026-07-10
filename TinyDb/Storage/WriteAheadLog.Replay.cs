using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class WriteAheadLog
{

    private static bool TryReadTransactionId(ReadOnlySpan<byte> data, out Guid transactionId)
    {
        transactionId = default;
        if (data.Length != TransactionIdSize) return false;

        transactionId = new Guid(data);
        return true;
    }

    private static int ReadFullHeader(Stream stream, byte[] buffer, int length)
    {
        var totalRead = 0;
        while (totalRead < length)
        {
            var read = stream.Read(buffer, totalRead, length - totalRead);
            if (read == 0)
            {
                break;
            }

            totalRead += read;
        }

        return totalRead;
    }

    private static async Task<int> ReadFullHeaderAsync(
        Stream stream,
        byte[] buffer,
        int length,
        CancellationToken cancellationToken)
    {
        var totalRead = 0;
        while (totalRead < length)
        {
            var read = await stream.ReadAsync(buffer, totalRead, length - totalRead, cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                break;
            }

            totalRead += read;
        }

        return totalRead;
    }


    private static bool TryReadTransactionPage(
        ReadOnlySpan<byte> data,
        out Guid transactionId,
        out byte[]? beforeImage,
        out byte[] afterImage)
    {
        transactionId = default;
        beforeImage = null;
        afterImage = Array.Empty<byte>();

        if (data.Length < TransactionIdSize + BeforeLengthSize) return false;

        transactionId = new Guid(data.Slice(0, TransactionIdSize));
        var beforeLength = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(TransactionIdSize, BeforeLengthSize));
        if (beforeLength < -1) return false;

        var offset = TransactionIdSize + BeforeLengthSize;
        if (beforeLength >= 0)
        {
            if (beforeLength > data.Length - offset) return false;
            beforeImage = data.Slice(offset, beforeLength).ToArray();
            offset += beforeLength;
        }

        if (offset >= data.Length) return false;

        afterImage = data.Slice(offset).ToArray();
        return true;
    }


    public void Replay(Action<uint, ReadOnlyMemory<byte>> apply)
    {
        Replay(apply, restore: null);
    }


    public void Replay(Action<uint, ReadOnlyMemory<byte>> apply, Action<uint, ReadOnlyMemory<byte>>? restore)
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
            var pendingUndoRecords = new List<(Guid TransactionId, uint PageId, byte[]? BeforeImage)>();

            while (stream.Position < stream.Length)
            {
                long currentEntryStart = stream.Position;

                var read = ReadFullHeader(stream, headerBuffer, FullHeaderSize);
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
                else if (read > HeaderSize)
                {
                    Log(TinyDbLogLevel.Warning, $"Incomplete header at {currentEntryStart}.");
                    replayStoppedAtInvalidRecord = true;
                    break;
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

                    var buffer = new ReadOnlyMemory<byte>(
                        decodedBuffer!,
                        0,
                        ReferenceEquals(decodedBuffer, rentedBuffer) ? length : decodedBuffer!.Length);
                    if (entryType == EntryTypePage)
                    {
                        apply(pageId, buffer);
                    }
                    else if (entryType == EntryTypeTransactionBegin)
                    {
                        if (!TryReadTransactionId(buffer.Span, out var transactionId))
                        {
                            Log(TinyDbLogLevel.Warning, $"Invalid transaction begin record at {currentEntryStart}.");
                            replayStoppedAtInvalidRecord = true;
                            break;
                        }

                        pendingTransactions.TryAdd(transactionId, new List<(uint, byte[]?, byte[])>());
                    }
                    else if (entryType == EntryTypeTransactionPage)
                    {
                        if (!TryReadTransactionPage(buffer.Span, out var transactionId, out var beforeImage, out var afterImage))
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
                        pendingUndoRecords.Add((transactionId, pageId, beforeImage));
                    }
                    else if (entryType == EntryTypeTransactionCommit)
                    {
                        if (!TryReadTransactionId(buffer.Span, out var transactionId))
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

            for (var i = pendingUndoRecords.Count - 1; i >= 0; i--)
            {
                var record = pendingUndoRecords[i];
                if (!pendingTransactions.ContainsKey(record.TransactionId) || record.BeforeImage == null)
                {
                    continue;
                }

                (restore ?? apply)(record.PageId, record.BeforeImage);
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


    public Task ReplayAsync(Func<uint, ReadOnlyMemory<byte>, Task> applyAsync, CancellationToken cancellationToken = default)
    {
        return ReplayAsync(applyAsync, restoreAsync: null, cancellationToken);
    }


    public async Task ReplayAsync(
        Func<uint, ReadOnlyMemory<byte>, Task> applyAsync,
        Func<uint, ReadOnlyMemory<byte>, Task>? restoreAsync,
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
            var pendingUndoRecords = new List<(Guid TransactionId, uint PageId, byte[]? BeforeImage)>();

            while (stream.Position < stream.Length)
            {
                long currentEntryStart = stream.Position;

                // 尝试读取完整头部
                var read = await ReadFullHeaderAsync(stream, headerBuffer, FullHeaderSize, cancellationToken).ConfigureAwait(false);

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
                else if (read > HeaderSize)
                {
                    Log(TinyDbLogLevel.Warning, $"Incomplete header at {currentEntryStart}.");
                    replayStoppedAtInvalidRecord = true;
                    break;
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

                    var buffer = new ReadOnlyMemory<byte>(
                        decodedBuffer!,
                        0,
                        ReferenceEquals(decodedBuffer, rentedBuffer) ? length : decodedBuffer!.Length);
                    if (entryType == EntryTypePage)
                    {
                        await applyAsync(pageId, buffer).ConfigureAwait(false);
                    }
                    else if (entryType == EntryTypeTransactionBegin)
                    {
                        if (!TryReadTransactionId(buffer.Span, out var transactionId))
                        {
                            Log(TinyDbLogLevel.Warning, $"Invalid transaction begin record at {currentEntryStart}.");
                            replayStoppedAtInvalidRecord = true;
                            break;
                        }

                        pendingTransactions.TryAdd(transactionId, new List<(uint, byte[]?, byte[])>());
                    }
                    else if (entryType == EntryTypeTransactionPage)
                    {
                        if (!TryReadTransactionPage(buffer.Span, out var transactionId, out var beforeImage, out var afterImage))
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
                        pendingUndoRecords.Add((transactionId, pageId, beforeImage));
                    }
                    else if (entryType == EntryTypeTransactionCommit)
                    {
                        if (!TryReadTransactionId(buffer.Span, out var transactionId))
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
            for (var i = pendingUndoRecords.Count - 1; i >= 0; i--)
            {
                var record = pendingUndoRecords[i];
                if (!pendingTransactions.ContainsKey(record.TransactionId) || record.BeforeImage == null)
                {
                    continue;
                }

                await (restoreAsync ?? applyAsync)(record.PageId, record.BeforeImage).ConfigureAwait(false);
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

}
