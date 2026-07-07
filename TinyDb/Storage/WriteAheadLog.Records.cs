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

}
