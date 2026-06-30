using System.Security.Cryptography;
using System.Text;
using TinyDb.Core;
using TinyDb.Storage;

namespace TinyDb.Security;

internal sealed class EncryptionContext : IDisposable
{
    private static readonly byte[] WrapAadPrefix = Encoding.UTF8.GetBytes("TinyDb.DEK.v1");
    private static readonly byte[] PageKeyLabel = Encoding.UTF8.GetBytes("TinyDb.PageKey.v1");
    private static readonly byte[] WalKeyLabel = Encoding.UTF8.GetBytes("TinyDb.WalKey.v1");
    private static readonly byte[] MetadataMacLabel = Encoding.UTF8.GetBytes("TinyDb.MetadataMac.v1");

    private readonly byte[] _dek;

    private EncryptionContext(EncryptionMetadata metadata, byte[] dek)
    {
        Metadata = metadata ?? throw new ArgumentNullException(nameof(metadata));
        _dek = dek ?? throw new ArgumentNullException(nameof(dek));
        if (_dek.Length != EncryptionMetadata.KeyLength)
        {
            throw new ArgumentException("DEK must be 32 bytes.", nameof(dek));
        }

        var pageKey = DeriveSubkey(_dek, PageKeyLabel);
        var walKey = DeriveSubkey(_dek, WalKeyLabel);
        try
        {
            PageCodec = new AesGcmPageCodec(metadata.LogicalPageSize, metadata.PhysicalPageSize, pageKey, metadata.DatabaseId, metadata.NonceEpoch);
            WalCodec = new AesGcmWalCodec(walKey, metadata.DatabaseId, metadata.NonceEpoch);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pageKey);
            CryptographicOperations.ZeroMemory(walKey);
        }
    }

    public EncryptionMetadata Metadata { get; }

    public IPageCodec PageCodec { get; }

    public IWalCodec WalCodec { get; }

    public static EncryptionContext CreateNew(TinyDbOptions options)
    {
        if (options == null) throw new ArgumentNullException(nameof(options));

        var metadata = new EncryptionMetadata
        {
            LogicalPageSize = options.PageSize,
            PhysicalPageSize = options.PageSize + EncryptionMetadata.FrameOverhead
        };

        RandomNumberGenerator.Fill(metadata.DatabaseId);

        var kek = BuildNewKek(options, metadata);
        byte[]? dek = new byte[EncryptionMetadata.KeyLength];
        try
        {
            RandomNumberGenerator.Fill(dek);
            WrapDek(metadata, dek, kek);
            ReserveNextNonceEpoch(metadata, dek);
            var context = new EncryptionContext(metadata, dek);
            dek = null;
            return context;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(kek);
            if (dek != null)
            {
                CryptographicOperations.ZeroMemory(dek);
            }
        }
    }

    public static EncryptionContext OpenExisting(EncryptionMetadata metadata, TinyDbOptions options)
    {
        if (metadata == null) throw new ArgumentNullException(nameof(metadata));
        if (options == null) throw new ArgumentNullException(nameof(options));

        var kek = BuildExistingKek(metadata, options);
        byte[]? dek = null;
        try
        {
            dek = UnwrapDek(metadata, kek);
            VerifyMetadataMac(metadata, dek);
            ReserveNextNonceEpoch(metadata, dek);
            var context = new EncryptionContext(metadata, dek);
            dek = null;
            return context;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(kek);
            if (dek != null)
            {
                CryptographicOperations.ZeroMemory(dek);
            }
        }
    }

    public bool VerifyPassword(string password)
    {
        if (password == null) throw new ArgumentNullException(nameof(password));
        if (Metadata.CredentialKind != EncryptionCredentialKind.Password)
        {
            throw new InvalidOperationException("Password verification is only supported for password-encrypted databases.");
        }

        byte[]? dek = null;
        var kek = DerivePasswordKek(password, Metadata.KdfSalt, Metadata.Iterations);
        try
        {
            dek = UnwrapDek(Metadata, kek);
            VerifyMetadataMac(Metadata, dek);
            return true;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
        finally
        {
            CryptographicOperations.ZeroMemory(kek);
            if (dek != null)
            {
                CryptographicOperations.ZeroMemory(dek);
            }
        }
    }

    public void RewrapWithPassword(string newPassword)
    {
        if (string.IsNullOrWhiteSpace(newPassword))
        {
            throw new ArgumentException("New password cannot be empty.", nameof(newPassword));
        }

        if (newPassword.Length < 8)
        {
            throw new ArgumentException("New password must be at least 8 characters.", nameof(newPassword));
        }

        Metadata.CredentialKind = EncryptionCredentialKind.Password;
        Metadata.Kdf = EncryptionMetadata.KdfPbkdf2Sha256;
        Metadata.Iterations = DatabaseSecurity.EncryptionPbkdf2Iterations;
        RandomNumberGenerator.Fill(Metadata.KdfSalt);
        var kek = DerivePasswordKek(newPassword, Metadata.KdfSalt, Metadata.Iterations);
        try
        {
            WrapDek(Metadata, _dek, kek);
            UpdateMetadataMac(Metadata, _dek);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(kek);
        }
    }

    private static byte[] BuildNewKek(TinyDbOptions options, EncryptionMetadata metadata)
    {
        if (!string.IsNullOrWhiteSpace(options.Password))
        {
            metadata.CredentialKind = EncryptionCredentialKind.Password;
            metadata.Kdf = EncryptionMetadata.KdfPbkdf2Sha256;
            metadata.Iterations = DatabaseSecurity.EncryptionPbkdf2Iterations;
            RandomNumberGenerator.Fill(metadata.KdfSalt);
            return DerivePasswordKek(options.Password!, metadata.KdfSalt, metadata.Iterations);
        }

        if (options.EncryptionKey == null || options.EncryptionKey.Length != EncryptionMetadata.KeyLength)
        {
            throw new ArgumentException("Encryption requires Password or a 32-byte EncryptionKey.");
        }

        metadata.CredentialKind = EncryptionCredentialKind.RawKey;
        metadata.Kdf = EncryptionMetadata.KdfNone;
        metadata.Iterations = 0;
        Array.Clear(metadata.KdfSalt);
        return options.EncryptionKey.ToArray();
    }

    private static byte[] BuildExistingKek(EncryptionMetadata metadata, TinyDbOptions options)
    {
        if (metadata.CredentialKind == EncryptionCredentialKind.Password)
        {
            if (string.IsNullOrEmpty(options.Password))
            {
                throw new UnauthorizedAccessException("Encrypted database requires a password.");
            }

            return DerivePasswordKek(options.Password!, metadata.KdfSalt, metadata.Iterations);
        }

        if (options.EncryptionKey == null || options.EncryptionKey.Length != EncryptionMetadata.KeyLength)
        {
            throw new UnauthorizedAccessException("Encrypted database requires a 32-byte encryption key.");
        }

        return options.EncryptionKey.ToArray();
    }

    private static byte[] DerivePasswordKek(string password, byte[] salt, int iterations)
    {
        return Rfc2898DeriveBytes.Pbkdf2(
            password,
            salt,
            iterations,
            HashAlgorithmName.SHA256,
            EncryptionMetadata.KeyLength);
    }

    private static void WrapDek(EncryptionMetadata metadata, byte[] dek, byte[] kek)
    {
        RandomNumberGenerator.Fill(metadata.WrappedDekNonce);
        var aad = BuildWrapAad(metadata);
        using var aes = new AesGcm(kek, EncryptionMetadata.TagLength);
        aes.Encrypt(
            metadata.WrappedDekNonce,
            dek,
            metadata.WrappedDekCiphertext,
            metadata.WrappedDekTag,
            aad);
    }

    private static byte[] UnwrapDek(EncryptionMetadata metadata, byte[] kek)
    {
        var dek = new byte[EncryptionMetadata.KeyLength];
        var aad = BuildWrapAad(metadata);

        try
        {
            using var aes = new AesGcm(kek, EncryptionMetadata.TagLength);
            aes.Decrypt(
                metadata.WrappedDekNonce,
                metadata.WrappedDekCiphertext,
                metadata.WrappedDekTag,
                dek,
                aad);
            return dek;
        }
        catch (CryptographicException ex)
        {
            throw new UnauthorizedAccessException("Encrypted database credential verification failed.", ex);
        }
    }

    private static byte[] BuildWrapAad(EncryptionMetadata metadata)
    {
        var aad = new byte[WrapAadPrefix.Length + EncryptionMetadata.DatabaseIdLength + 8];
        WrapAadPrefix.CopyTo(aad, 0);
        metadata.DatabaseId.CopyTo(aad.AsSpan(WrapAadPrefix.Length));
        aad[WrapAadPrefix.Length + EncryptionMetadata.DatabaseIdLength] = (byte)metadata.CredentialKind;
        aad[WrapAadPrefix.Length + EncryptionMetadata.DatabaseIdLength + 1] = metadata.Kdf;
        BitConverter.GetBytes(metadata.LogicalPageSize).CopyTo(aad, WrapAadPrefix.Length + EncryptionMetadata.DatabaseIdLength + 2);
        return aad;
    }

    private static byte[] DeriveSubkey(byte[] dek, byte[] label)
    {
        using var hmac = new HMACSHA256(dek);
        return hmac.ComputeHash(label);
    }

    private static void ReserveNextNonceEpoch(EncryptionMetadata metadata, byte[] dek)
    {
        if (metadata.NonceEpoch == ulong.MaxValue)
        {
            throw new InvalidOperationException("AES-GCM nonce epoch exhausted; rotate the encryption key.");
        }

        metadata.NonceEpoch++;
        metadata.UseCurrentFormat();
        UpdateMetadataMac(metadata, dek);
    }

    private static void UpdateMetadataMac(EncryptionMetadata metadata, byte[] dek)
    {
        var key = DeriveSubkey(dek, MetadataMacLabel);
        try
        {
            using var hmac = new HMACSHA256(key);
            metadata.MetadataMac = hmac.ComputeHash(metadata.GetMacData());
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }
    }

    private static void VerifyMetadataMac(EncryptionMetadata metadata, byte[] dek)
    {
        var key = DeriveSubkey(dek, MetadataMacLabel);
        try
        {
            using var hmac = new HMACSHA256(key);
            var actual = hmac.ComputeHash(metadata.GetMacData());
            if (!CryptographicOperations.FixedTimeEquals(actual, metadata.MetadataMac))
            {
                throw new SecurityCorruptedException("Encryption metadata authentication failed.");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }
    }

    public void Dispose()
    {
        CryptographicOperations.ZeroMemory(_dek);
        if (PageCodec is IDisposable pageCodec)
        {
            pageCodec.Dispose();
        }

        if (WalCodec is IDisposable walCodec)
        {
            walCodec.Dispose();
        }
    }
}
