using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using TinyDb.Security;

namespace TinyDb.Storage;

internal interface IPageCodec
{
    uint LogicalPageSize { get; }
    uint PhysicalPageSize { get; }
    bool IsEncrypted { get; }
    byte[] Decode(uint pageId, byte[] frame);
    byte[] Encode(uint pageId, byte[] logicalPage);
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
        Array.Copy(frame, logical, Math.Min(frame.Length, logical.Length));
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
}

internal sealed class AesGcmPageCodec : IPageCodec
{
    private static readonly byte[] AadPrefix = Encoding.UTF8.GetBytes("TinyDb.Page.v1");
    private readonly byte[] _key;
    private readonly byte[] _databaseId;

    public AesGcmPageCodec(uint logicalPageSize, uint physicalPageSize, byte[] key, byte[] databaseId)
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
    }

    public uint LogicalPageSize { get; }

    public uint PhysicalPageSize { get; }

    public bool IsEncrypted => true;

    public byte[] Decode(uint pageId, byte[] frame)
    {
        if (frame == null) throw new ArgumentNullException(nameof(frame));
        if (frame.Length != PhysicalPageSize)
        {
            throw new ArgumentException("Physical page frame size does not match codec configuration.", nameof(frame));
        }

        var logical = new byte[LogicalPageSize];
        if (pageId == 1)
        {
            Array.Copy(frame, logical, logical.Length);
            return logical;
        }

        var nonce = frame.AsSpan(0, EncryptionMetadata.NonceLength);
        var ciphertext = frame.AsSpan(EncryptionMetadata.NonceLength, (int)LogicalPageSize);
        var tag = frame.AsSpan(EncryptionMetadata.NonceLength + (int)LogicalPageSize, EncryptionMetadata.TagLength);

        try
        {
            using var aes = new AesGcm(_key, EncryptionMetadata.TagLength);
            aes.Decrypt(nonce, ciphertext, tag, logical, BuildAad(pageId));
            return logical;
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
        if (pageId == 1)
        {
            Array.Copy(logicalPage, frame, logicalPage.Length);
            return frame;
        }

        var nonce = frame.AsSpan(0, EncryptionMetadata.NonceLength);
        RandomNumberGenerator.Fill(nonce);
        var ciphertext = frame.AsSpan(EncryptionMetadata.NonceLength, logicalPage.Length);
        var tag = frame.AsSpan(EncryptionMetadata.NonceLength + logicalPage.Length, EncryptionMetadata.TagLength);

        using var aes = new AesGcm(_key, EncryptionMetadata.TagLength);
        aes.Encrypt(nonce, logicalPage, ciphertext, tag, BuildAad(pageId));
        return frame;
    }

    private byte[] BuildAad(uint pageId)
    {
        var aad = new byte[AadPrefix.Length + _databaseId.Length + sizeof(uint) + sizeof(uint)];
        AadPrefix.CopyTo(aad, 0);
        _databaseId.CopyTo(aad.AsSpan(AadPrefix.Length));
        var offset = AadPrefix.Length + _databaseId.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(aad.AsSpan(offset, sizeof(uint)), pageId);
        BinaryPrimitives.WriteUInt32LittleEndian(aad.AsSpan(offset + sizeof(uint), sizeof(uint)), LogicalPageSize);
        return aad;
    }
}
