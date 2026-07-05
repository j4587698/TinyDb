using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using TinyDb.Security;

namespace TinyDb.Storage;

internal interface IWalCodec
{
    bool IsEncrypted { get; }
    int MaxOverhead { get; }
    byte[] Encode(byte entryType, uint pageId, long recordOffset, byte[] payload);
    byte[] Decode(byte entryType, uint pageId, long recordOffset, byte[] payload, int payloadLength);
}

internal sealed class NoOpWalCodec : IWalCodec
{
    public bool IsEncrypted => false;

    public int MaxOverhead => 0;

    public byte[] Encode(byte entryType, uint pageId, long recordOffset, byte[] payload)
    {
        return payload ?? throw new ArgumentNullException(nameof(payload));
    }

    public byte[] Decode(byte entryType, uint pageId, long recordOffset, byte[] payload, int payloadLength)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));
        if (payloadLength < 0 || payloadLength > payload.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(payloadLength));
        }

        return payload.Length == payloadLength ? payload : payload.AsSpan(0, payloadLength).ToArray();
    }
}

internal sealed class AesGcmWalCodec : IWalCodec, IDisposable
{
    private static readonly byte[] Magic = { (byte)'W', (byte)'A', (byte)'E', (byte)'1' };
    private static readonly byte[] AadPrefix = Encoding.UTF8.GetBytes("TinyDb.Wal.v1");
    private const int MagicLength = 4;

    private readonly byte[] _key;
    private readonly byte[] _databaseId;
    private readonly ulong _nonceEpoch;
    private long _nonceCounter;

    public AesGcmWalCodec(byte[] key, byte[] databaseId, ulong nonceEpoch)
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
        _nonceEpoch = nonceEpoch;
    }

    public bool IsEncrypted => true;

    public int MaxOverhead => MagicLength + EncryptionMetadata.NonceLength + EncryptionMetadata.TagLength;

    private int AadLength => AadPrefix.Length + _databaseId.Length + 1 + sizeof(uint) + sizeof(long);

    public byte[] Encode(byte entryType, uint pageId, long recordOffset, byte[] payload)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));

        var frame = new byte[MaxOverhead + payload.Length];
        Magic.CopyTo(frame, 0);
        var nonce = frame.AsSpan(MagicLength, EncryptionMetadata.NonceLength);
        FillNonce(nonce);
        var ciphertext = frame.AsSpan(MagicLength + EncryptionMetadata.NonceLength, payload.Length);
        var tag = frame.AsSpan(MagicLength + EncryptionMetadata.NonceLength + payload.Length, EncryptionMetadata.TagLength);

        Span<byte> aad = stackalloc byte[AadLength];
        WriteAad(entryType, pageId, recordOffset, aad);
        using var aes = new AesGcm(_key, EncryptionMetadata.TagLength);
        aes.Encrypt(nonce, payload, ciphertext, tag, aad);
        return frame;
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
            throw new InvalidOperationException("AES-GCM WAL nonce counter exhausted; rotate the encryption key.");
        }

        BinaryPrimitives.WriteUInt64LittleEndian(nonce, _nonceEpoch);
        BinaryPrimitives.WriteUInt32LittleEndian(nonce.Slice(sizeof(ulong), sizeof(uint)), (uint)counter);
    }

    public byte[] Decode(byte entryType, uint pageId, long recordOffset, byte[] payload, int payloadLength)
    {
        if (payload == null) throw new ArgumentNullException(nameof(payload));
        if (payloadLength < 0 || payloadLength > payload.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(payloadLength));
        }

        var payloadSpan = payload.AsSpan(0, payloadLength);
        if (payloadSpan.Length < MaxOverhead || !payloadSpan[..MagicLength].SequenceEqual(Magic))
        {
            throw new InvalidDataException("Encrypted WAL record frame is invalid.");
        }

        var ciphertextLength = payloadSpan.Length - MaxOverhead;
        var plaintext = new byte[ciphertextLength];
        var nonce = payloadSpan.Slice(MagicLength, EncryptionMetadata.NonceLength);
        var ciphertext = payloadSpan.Slice(MagicLength + EncryptionMetadata.NonceLength, ciphertextLength);
        var tag = payloadSpan.Slice(MagicLength + EncryptionMetadata.NonceLength + ciphertextLength, EncryptionMetadata.TagLength);

        try
        {
            Span<byte> aad = stackalloc byte[AadLength];
            WriteAad(entryType, pageId, recordOffset, aad);
            using var aes = new AesGcm(_key, EncryptionMetadata.TagLength);
            aes.Decrypt(nonce, ciphertext, tag, plaintext, aad);
            return plaintext;
        }
        catch (CryptographicException ex)
        {
            throw new InvalidDataException("Encrypted WAL record authentication failed.", ex);
        }
    }

    private void WriteAad(byte entryType, uint pageId, long recordOffset, Span<byte> destination)
    {
        if (destination.Length != AadLength)
        {
            throw new ArgumentException("AAD length does not match codec configuration.", nameof(destination));
        }

        AadPrefix.CopyTo(destination);
        _databaseId.CopyTo(destination.Slice(AadPrefix.Length));
        var offset = AadPrefix.Length + _databaseId.Length;
        destination[offset] = entryType;
        BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(offset + 1, sizeof(uint)), pageId);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(offset + 1 + sizeof(uint), sizeof(long)), recordOffset);
    }

    public void Dispose()
    {
        CryptographicOperations.ZeroMemory(_key);
        CryptographicOperations.ZeroMemory(_databaseId);
    }
}
