using System.Buffers.Binary;
using System.Security.Cryptography;
using TinyDb.Core;
using TinyDb.Storage;

namespace TinyDb.Security;

internal enum EncryptionCredentialKind : byte
{
    Password = 1,
    RawKey = 2
}

internal sealed class EncryptionMetadata
{
    internal const int VersionValue = 1;
    internal const int AlgorithmAesGcm256 = 1;
    internal const int KdfNone = 0;
    internal const int KdfPbkdf2Sha256 = 1;
    internal const int SaltLength = 16;
    internal const int DatabaseIdLength = 16;
    internal const int KeyLength = 32;
    internal const int NonceLength = 12;
    internal const int TagLength = 16;
    internal const int MacLength = 32;
    internal const int FrameOverhead = NonceLength + TagLength;
    internal const int LegacySerializedLength = 156;
    internal const int SerializedLength = 164;
    internal const int MaxSerializedLength = 512;

    private const uint MagicValue = 0x31434E45; // ENC1
    private const int NonceEpochOffset = 124;
    private const int MacOffset = SerializedLength - MacLength;
    private const int LegacyMacOffset = LegacySerializedLength - MacLength;

    public int Version { get; set; } = VersionValue;
    public EncryptionCredentialKind CredentialKind { get; set; }
    public byte Algorithm { get; set; } = AlgorithmAesGcm256;
    public byte Kdf { get; set; }
    public uint LogicalPageSize { get; set; }
    public uint PhysicalPageSize { get; set; }
    public uint StoredFrameOverhead { get; set; } = FrameOverhead;
    public int Iterations { get; set; }
    public byte[] DatabaseId { get; set; } = new byte[DatabaseIdLength];
    public byte[] KdfSalt { get; set; } = new byte[SaltLength];
    public byte[] WrappedDekNonce { get; set; } = new byte[NonceLength];
    public byte[] WrappedDekCiphertext { get; set; } = new byte[KeyLength];
    public byte[] WrappedDekTag { get; set; } = new byte[TagLength];
    public ulong NonceEpoch { get; set; }
    public byte[] MetadataMac { get; set; } = new byte[MacLength];
    private int _serializedLength = SerializedLength;

    public static bool HasMagic(ReadOnlySpan<byte> data)
    {
        return data.Length >= sizeof(uint) && BinaryPrimitives.ReadUInt32LittleEndian(data) == MagicValue;
    }

    public static EncryptionMetadata FromBytes(ReadOnlySpan<byte> data)
    {
        if (data.Length < LegacySerializedLength)
        {
            throw new SecurityCorruptedException("Encryption metadata is truncated.");
        }

        if (BinaryPrimitives.ReadUInt32LittleEndian(data) != MagicValue)
        {
            throw new SecurityCorruptedException("Encryption metadata marker is invalid.");
        }

        var version = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(4, 4));
        var length = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(8, 4));
        if (version != VersionValue || length is not (LegacySerializedLength or SerializedLength))
        {
            throw new SecurityCorruptedException("Encryption metadata version is not supported.");
        }

        if (data.Length < length)
        {
            throw new SecurityCorruptedException("Encryption metadata is truncated.");
        }

        var metadata = new EncryptionMetadata
        {
            _serializedLength = length,
            Version = version,
            CredentialKind = (EncryptionCredentialKind)data[12],
            Algorithm = data[13],
            Kdf = data[14],
            LogicalPageSize = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(16, 4)),
            PhysicalPageSize = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(20, 4)),
            StoredFrameOverhead = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(24, 4)),
            Iterations = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(28, 4)),
            DatabaseId = data.Slice(32, DatabaseIdLength).ToArray(),
            KdfSalt = data.Slice(48, SaltLength).ToArray(),
            WrappedDekNonce = data.Slice(64, NonceLength).ToArray(),
            WrappedDekCiphertext = data.Slice(76, KeyLength).ToArray(),
            WrappedDekTag = data.Slice(108, TagLength).ToArray(),
            NonceEpoch = length == SerializedLength
                ? BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(NonceEpochOffset, sizeof(ulong)))
                : 0UL,
            MetadataMac = data.Slice(length == SerializedLength ? MacOffset : LegacyMacOffset, MacLength).ToArray()
        };

        metadata.Validate();
        return metadata;
    }

    public byte[] ToBytes()
    {
        _serializedLength = SerializedLength;
        var buffer = new byte[SerializedLength];
        WriteTo(buffer, includeMac: true);
        return buffer;
    }

    public byte[] GetMacData()
    {
        var buffer = new byte[GetMacOffset()];
        WriteTo(buffer, includeMac: false);
        return buffer;
    }

    public void UseCurrentFormat()
    {
        _serializedLength = SerializedLength;
    }

    private void WriteTo(Span<byte> buffer, bool includeMac)
    {
        var requiredLength = includeMac ? _serializedLength : GetMacOffset();
        if (buffer.Length < requiredLength)
        {
            throw new ArgumentException("Destination is too small.", nameof(buffer));
        }

        BinaryPrimitives.WriteUInt32LittleEndian(buffer.Slice(0, 4), MagicValue);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(4, 4), Version);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(8, 4), _serializedLength);
        buffer[12] = (byte)CredentialKind;
        buffer[13] = Algorithm;
        buffer[14] = Kdf;
        buffer[15] = 0;
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.Slice(16, 4), LogicalPageSize);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.Slice(20, 4), PhysicalPageSize);
        BinaryPrimitives.WriteUInt32LittleEndian(buffer.Slice(24, 4), StoredFrameOverhead);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(28, 4), Iterations);
        DatabaseId.CopyTo(buffer.Slice(32, DatabaseIdLength));
        KdfSalt.CopyTo(buffer.Slice(48, SaltLength));
        WrappedDekNonce.CopyTo(buffer.Slice(64, NonceLength));
        WrappedDekCiphertext.CopyTo(buffer.Slice(76, KeyLength));
        WrappedDekTag.CopyTo(buffer.Slice(108, TagLength));
        if (_serializedLength == SerializedLength)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(buffer.Slice(NonceEpochOffset, sizeof(ulong)), NonceEpoch);
        }

        if (includeMac)
        {
            MetadataMac.CopyTo(buffer.Slice(GetMacOffset(), MacLength));
        }
    }

    private int GetMacOffset()
    {
        return _serializedLength == SerializedLength ? MacOffset : LegacyMacOffset;
    }

    public void Validate()
    {
        if (CredentialKind is not (EncryptionCredentialKind.Password or EncryptionCredentialKind.RawKey))
        {
            throw new SecurityCorruptedException("Encryption credential kind is invalid.");
        }

        if (Algorithm != AlgorithmAesGcm256)
        {
            throw new SecurityCorruptedException("Encryption algorithm is not supported.");
        }

        if (CredentialKind == EncryptionCredentialKind.Password)
        {
            if (Kdf != KdfPbkdf2Sha256 || Iterations < DatabaseSecurity.EncryptionPbkdf2Iterations)
            {
                throw new SecurityCorruptedException("Encryption KDF settings are invalid.");
            }
        }
        else if (Kdf != KdfNone)
        {
            throw new SecurityCorruptedException("Raw-key encryption metadata must not use a KDF.");
        }

        if (StoredFrameOverhead != FrameOverhead || PhysicalPageSize != LogicalPageSize + FrameOverhead)
        {
            throw new SecurityCorruptedException("Encrypted page frame size is invalid.");
        }

        ValidateArray(DatabaseId, DatabaseIdLength, nameof(DatabaseId));
        ValidateArray(KdfSalt, SaltLength, nameof(KdfSalt));
        ValidateArray(WrappedDekNonce, NonceLength, nameof(WrappedDekNonce));
        ValidateArray(WrappedDekCiphertext, KeyLength, nameof(WrappedDekCiphertext));
        ValidateArray(WrappedDekTag, TagLength, nameof(WrappedDekTag));
        ValidateArray(MetadataMac, MacLength, nameof(MetadataMac));
    }

    private static void ValidateArray(byte[] value, int expectedLength, string name)
    {
        if (value == null || value.Length != expectedLength)
        {
            throw new SecurityCorruptedException($"Encryption metadata field '{name}' is invalid.");
        }
    }
}

internal static class EncryptionMetadataStore
{
    public const int PageDataOffset = DatabaseHeader.Size;
    public const int FileOffset = Page.DataStartOffset + DatabaseHeader.Size;

    public static bool TryReadFromLogicalPage(ReadOnlySpan<byte> logicalPage, out EncryptionMetadata? metadata)
    {
        metadata = null;
        if (logicalPage.Length < FileOffset + EncryptionMetadata.LegacySerializedLength)
        {
            return false;
        }

        var availableLength = Math.Min(EncryptionMetadata.MaxSerializedLength, logicalPage.Length - FileOffset);
        var span = logicalPage.Slice(FileOffset, availableLength);
        if (!EncryptionMetadata.HasMagic(span))
        {
            return false;
        }

        metadata = EncryptionMetadata.FromBytes(span);
        return true;
    }

    public static bool TryReadFromDisk(IDiskStream diskStream, uint logicalPageSize, out EncryptionMetadata? metadata)
    {
        metadata = null;
        if (diskStream.Size < FileOffset + EncryptionMetadata.LegacySerializedLength)
        {
            return false;
        }

        var logicalPage = diskStream.ReadPage(0, checked((int)logicalPageSize));
        return TryReadFromLogicalPage(logicalPage, out metadata);
    }

    public static void WriteToDisk(IDiskStream diskStream, uint logicalPageSize, EncryptionMetadata metadata)
    {
        if (diskStream == null) throw new ArgumentNullException(nameof(diskStream));
        if (metadata == null) throw new ArgumentNullException(nameof(metadata));

        var logicalPage = diskStream.ReadPage(0, checked((int)logicalPageSize));
        WriteToLogicalPage(logicalPage, metadata);

        var header = PageHeader.FromByteArray(logicalPage);
        header.UpdateModification();
        header.Checksum = 0;
        header.WriteTo(logicalPage.AsSpan(0, PageHeader.Size));
        header.Checksum = header.CalculateChecksum(logicalPage);
        header.WriteTo(logicalPage.AsSpan(0, PageHeader.Size));

        diskStream.WritePage(0, logicalPage);
        diskStream.Flush();
    }

    public static void WriteToPage(Page page, EncryptionMetadata metadata)
    {
        if (page == null) throw new ArgumentNullException(nameof(page));
        if (metadata == null) throw new ArgumentNullException(nameof(metadata));
        if (page.PageID != 1) throw new ArgumentException("Encryption metadata must be stored on page 1.", nameof(page));

        var clear = new byte[EncryptionMetadata.MaxSerializedLength];
        page.WriteData(PageDataOffset, clear);
        page.WriteData(PageDataOffset, metadata.ToBytes());
    }

    public static void WriteToLogicalPage(Span<byte> logicalPage, EncryptionMetadata metadata)
    {
        if (metadata == null) throw new ArgumentNullException(nameof(metadata));
        if (logicalPage.Length < FileOffset + EncryptionMetadata.MaxSerializedLength)
        {
            throw new ArgumentException("Logical page is too small for encryption metadata.", nameof(logicalPage));
        }

        logicalPage.Slice(FileOffset, EncryptionMetadata.MaxSerializedLength).Clear();
        metadata.ToBytes().CopyTo(logicalPage.Slice(FileOffset));
    }
}
