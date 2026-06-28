using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using TinyDb.Security;

namespace TinyDb.Storage;

internal interface IWalCodec
{
    bool IsEncrypted { get; }
    int MaxOverhead { get; }
    byte[] Encode(byte entryType, uint pageId, long recordOffset, byte[] payload);
    byte[] Decode(byte entryType, uint pageId, long recordOffset, byte[] payload);
}

internal sealed class NoOpWalCodec : IWalCodec
{
    public bool IsEncrypted => false;

    public int MaxOverhead => 0;

    public byte[] Encode(byte entryType, uint pageId, long recordOffset, byte[] payload)
    {
        return payload ?? throw new ArgumentNullException(nameof(payload));
    }

    public byte[] Decode(byte entryType, uint pageId, long recordOffset, byte[] payload)
    {
        return payload ?? throw new ArgumentNullException(nameof(payload));
    }
}

internal sealed class AesGcmWalCodec : IWalCodec
{
    private static readonly byte[] Magic = { (byte)'W', (byte)'A', (byte)'E', (byte)'1' };
    private static readonly byte[] AadPrefix = Encoding.UTF8.GetBytes("TinyDb.Wal.v1");
    private const int MagicLength = 4;

    private readonly byte[] _key;
    private readonly byte[] _databaseId;

    public AesGcmWalCodec(byte[] key, byte[] databaseId)
    {
        if (key == null || key.Length != EncryptionMetadata.KeyLength)
        {
            throw new ArgumentException("WAL encryption key must be 32 bytes.", nameof(key));
        }

        if (databaseId == null || databaseId.Length != EncryptionMetadata.DatabaseIdLength)
        {
            throw new ArgumentException("Database ID must be 16 bytes.", nameof(databaseId));
        }

        _key = key.ToArray();
        _databaseId = databaseId.ToArray();
    }

    public bool IsEncrypted => true;

    public int MaxOverhead => MagicLength + EncryptionMetadata.NonceLength + EncryptionMetadata.TagLength;

    public byte[] Encode(byte entryType, uint pageId, long recordOffset, byte[] payload)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));

        var frame = new byte[MaxOverhead + payload.Length];
        Magic.CopyTo(frame, 0);
        var nonce = frame.AsSpan(MagicLength, EncryptionMetadata.NonceLength);
        RandomNumberGenerator.Fill(nonce);
        var ciphertext = frame.AsSpan(MagicLength + EncryptionMetadata.NonceLength, payload.Length);
        var tag = frame.AsSpan(MagicLength + EncryptionMetadata.NonceLength + payload.Length, EncryptionMetadata.TagLength);

        using var aes = new AesGcm(_key, EncryptionMetadata.TagLength);
        aes.Encrypt(nonce, payload, ciphertext, tag, BuildAad(entryType, pageId, recordOffset));
        return frame;
    }

    public byte[] Decode(byte entryType, uint pageId, long recordOffset, byte[] payload)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));
        if (payload.Length < MaxOverhead || !payload.AsSpan(0, MagicLength).SequenceEqual(Magic))
        {
            throw new InvalidDataException("Encrypted WAL record frame is invalid.");
        }

        var ciphertextLength = payload.Length - MaxOverhead;
        var plaintext = new byte[ciphertextLength];
        var nonce = payload.AsSpan(MagicLength, EncryptionMetadata.NonceLength);
        var ciphertext = payload.AsSpan(MagicLength + EncryptionMetadata.NonceLength, ciphertextLength);
        var tag = payload.AsSpan(MagicLength + EncryptionMetadata.NonceLength + ciphertextLength, EncryptionMetadata.TagLength);

        try
        {
            using var aes = new AesGcm(_key, EncryptionMetadata.TagLength);
            aes.Decrypt(nonce, ciphertext, tag, plaintext, BuildAad(entryType, pageId, recordOffset));
            return plaintext;
        }
        catch (CryptographicException ex)
        {
            throw new InvalidDataException("Encrypted WAL record authentication failed.", ex);
        }
    }

    private byte[] BuildAad(byte entryType, uint pageId, long recordOffset)
    {
        var aad = new byte[AadPrefix.Length + _databaseId.Length + 1 + sizeof(uint) + sizeof(long)];
        AadPrefix.CopyTo(aad, 0);
        _databaseId.CopyTo(aad.AsSpan(AadPrefix.Length));
        var offset = AadPrefix.Length + _databaseId.Length;
        aad[offset] = entryType;
        BinaryPrimitives.WriteUInt32LittleEndian(aad.AsSpan(offset + 1, sizeof(uint)), pageId);
        BinaryPrimitives.WriteInt64LittleEndian(aad.AsSpan(offset + 1 + sizeof(uint), sizeof(long)), recordOffset);
        return aad;
    }
}
