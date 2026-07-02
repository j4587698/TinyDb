using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using TinyDb.Security;

namespace TinyDb.Storage;

internal interface IPageCodec
{
    uint LogicalPageSize { get; }
    uint PhysicalPageSize { get; }
    bool IsEncrypted { get; }
    byte[] Decode(uint pageId, byte[] frame);
    byte[] Encode(uint pageId, byte[] logicalPage);
    void DecodeTo(uint pageId, ReadOnlySpan<byte> frame, Span<byte> destination);
    void EncodeTo(uint pageId, ReadOnlySpan<byte> logicalPage, Span<byte> destination);
}

internal sealed class NoOpPageCodec : IPageCodec
{
    public NoOpPageCodec(uint pageSize)
    {
        LogicalPageSize = pageSize;
        PhysicalPageSize = pageSize;
    }

    public uint LogicalPageSize { get; }

    public uint PhysicalPageSize { get; }

    public bool IsEncrypted => false;

    public byte[] Decode(uint pageId, byte[] frame)
    {
        if (frame == null) throw new ArgumentNullException(nameof(frame));
        if (frame.Length == LogicalPageSize) return frame;

        var logical = new byte[LogicalPageSize];
        DecodeTo(pageId, frame, logical);
        return logical;
    }

    public byte[] Encode(uint pageId, byte[] logicalPage)
    {
        if (logicalPage == null) throw new ArgumentNullException(nameof(logicalPage));
        if (logicalPage.Length != LogicalPageSize)
        {
            throw new ArgumentException("Logical page size does not match codec configuration.", nameof(logicalPage));
        }

        return logicalPage;
    }

    public void DecodeTo(uint pageId, ReadOnlySpan<byte> frame, Span<byte> destination)
    {
        if (destination.Length != (int)LogicalPageSize)
        {
            throw new ArgumentException("Logical page size does not match codec configuration.", nameof(destination));
        }

        if (frame.Length != (int)PhysicalPageSize)
        {
            throw new ArgumentException("Physical page frame size does not match codec configuration.", nameof(frame));
        }

        frame.Slice(0, destination.Length).CopyTo(destination);
    }

    public void EncodeTo(uint pageId, ReadOnlySpan<byte> logicalPage, Span<byte> destination)
    {
        if (logicalPage.Length != (int)LogicalPageSize)
        {
            throw new ArgumentException("Logical page size does not match codec configuration.", nameof(logicalPage));
        }

        if (destination.Length != (int)PhysicalPageSize)
        {
            throw new ArgumentException("Physical page frame size does not match codec configuration.", nameof(destination));
        }

        logicalPage.CopyTo(destination);
    }
}

internal sealed class AesGcmPageCodec : IPageCodec, IDisposable
{
    private static readonly byte[] AadPrefix = Encoding.UTF8.GetBytes("TinyDb.Page.v1");
    private readonly byte[] _key;
    private readonly byte[] _databaseId;
    private readonly ulong _nonceEpoch;
    private long _nonceCounter;

    public AesGcmPageCodec(uint logicalPageSize, uint physicalPageSize, byte[] key, byte[] databaseId, ulong nonceEpoch)
    {
        if (physicalPageSize != logicalPageSize + EncryptionMetadata.FrameOverhead)
        {
            throw new ArgumentException("Physical page size must include AES-GCM frame overhead.", nameof(physicalPageSize));
        }

        if (key == null || key.Length != EncryptionMetadata.KeyLength)
        {
            throw new ArgumentException("Page encryption key must be 32 bytes.", nameof(key));
        }

        if (databaseId == null || databaseId.Length != EncryptionMetadata.DatabaseIdLength)
        {
            throw new ArgumentException("Database ID must be 16 bytes.", nameof(databaseId));
        }

        LogicalPageSize = logicalPageSize;
        PhysicalPageSize = physicalPageSize;
        _key = key.ToArray();
        _databaseId = databaseId.ToArray();
        _nonceEpoch = nonceEpoch;
    }

    public uint LogicalPageSize { get; }

    public uint PhysicalPageSize { get; }

    public bool IsEncrypted => true;

    private int AadLength => AadPrefix.Length + _databaseId.Length + sizeof(uint) + sizeof(uint);

    public byte[] Decode(uint pageId, byte[] frame)
    {
        if (frame == null) throw new ArgumentNullException(nameof(frame));
        if (frame.Length != PhysicalPageSize)
        {
            throw new ArgumentException("Physical page frame size does not match codec configuration.", nameof(frame));
        }

        var logical = new byte[LogicalPageSize];
        DecodeTo(pageId, frame, logical);
        return logical;
    }

    public void DecodeTo(uint pageId, ReadOnlySpan<byte> frame, Span<byte> destination)
    {
        if (frame.Length != (int)PhysicalPageSize)
        {
            throw new ArgumentException("Physical page frame size does not match codec configuration.", nameof(frame));
        }

        if (destination.Length != (int)LogicalPageSize)
        {
            throw new ArgumentException("Logical page size does not match codec configuration.", nameof(destination));
        }

        if (pageId == 1)
        {
            frame.Slice(0, destination.Length).CopyTo(destination);
            return;
        }

        var nonce = frame.Slice(0, EncryptionMetadata.NonceLength);
        var ciphertext = frame.Slice(EncryptionMetadata.NonceLength, (int)LogicalPageSize);
        var tag = frame.Slice(EncryptionMetadata.NonceLength + (int)LogicalPageSize, EncryptionMetadata.TagLength);

        try
        {
            Span<byte> aad = stackalloc byte[AadLength];
            WriteAad(pageId, aad);
            using var aes = new AesGcm(_key, EncryptionMetadata.TagLength);
            aes.Decrypt(nonce, ciphertext, tag, destination, aad);
        }
        catch (CryptographicException ex)
        {
            throw new InvalidDataException($"Encrypted page {pageId} authentication failed.", ex);
        }
    }

    public byte[] Encode(uint pageId, byte[] logicalPage)
    {
        if (logicalPage == null) throw new ArgumentNullException(nameof(logicalPage));
        if (logicalPage.Length != LogicalPageSize)
        {
            throw new ArgumentException("Logical page size does not match codec configuration.", nameof(logicalPage));
        }

        var frame = new byte[PhysicalPageSize];
        EncodeTo(pageId, logicalPage, frame);
        return frame;
    }

    public void EncodeTo(uint pageId, ReadOnlySpan<byte> logicalPage, Span<byte> destination)
    {
        if (logicalPage.Length != (int)LogicalPageSize)
        {
            throw new ArgumentException("Logical page size does not match codec configuration.", nameof(logicalPage));
        }

        if (destination.Length != (int)PhysicalPageSize)
        {
            throw new ArgumentException("Physical page frame size does not match codec configuration.", nameof(destination));
        }

        if (pageId == 1)
        {
            logicalPage.CopyTo(destination);
            destination.Slice(logicalPage.Length).Clear();
            return;
        }

        var nonce = destination.Slice(0, EncryptionMetadata.NonceLength);
        FillNonce(nonce);
        var ciphertext = destination.Slice(EncryptionMetadata.NonceLength, logicalPage.Length);
        var tag = destination.Slice(EncryptionMetadata.NonceLength + logicalPage.Length, EncryptionMetadata.TagLength);

        Span<byte> aad = stackalloc byte[AadLength];
        WriteAad(pageId, aad);
        using var aes = new AesGcm(_key, EncryptionMetadata.TagLength);
        aes.Encrypt(nonce, logicalPage, ciphertext, tag, aad);
    }

    private void FillNonce(Span<byte> nonce)
    {
        if (nonce.Length != EncryptionMetadata.NonceLength)
        {
            throw new ArgumentException("Nonce length does not match codec configuration.", nameof(nonce));
        }

        var counter = Interlocked.Increment(ref _nonceCounter);
        if ((ulong)counter > uint.MaxValue)
        {
            throw new InvalidOperationException("AES-GCM page nonce counter exhausted; rotate the encryption key.");
        }

        BinaryPrimitives.WriteUInt64LittleEndian(nonce, _nonceEpoch);
        BinaryPrimitives.WriteUInt32LittleEndian(nonce.Slice(sizeof(ulong), sizeof(uint)), (uint)counter);
    }

    private void WriteAad(uint pageId, Span<byte> destination)
    {
        if (destination.Length != AadLength)
        {
            throw new ArgumentException("AAD length does not match codec configuration.", nameof(destination));
        }

        AadPrefix.CopyTo(destination);
        _databaseId.CopyTo(destination.Slice(AadPrefix.Length));
        var offset = AadPrefix.Length + _databaseId.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(offset, sizeof(uint)), pageId);
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(offset + sizeof(uint), sizeof(uint)), LogicalPageSize);
    }

    public void Dispose()
    {
        CryptographicOperations.ZeroMemory(_key);
        CryptographicOperations.ZeroMemory(_databaseId);
    }
}
