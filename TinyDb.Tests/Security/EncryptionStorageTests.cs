using System.Security.Cryptography;
using System.Text;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Security;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Security;

public sealed class EncryptionStorageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;

    public EncryptionStorageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"enc_storage_{Guid.NewGuid():N}.db");
        _walPath = GetWalPath(_dbPath);
    }

    public void Dispose()
    {
        TryDelete(_dbPath);
        TryDelete(_walPath);
        TryDelete(_walPath + ".bak");
        TryDelete(_dbPath + ".compact");
    }

    [Test]
    public async Task PasswordEncryptedDatabase_ShouldRoundTripAndHidePlaintext()
    {
        const string secret = "secret-field-value-83f0d5b56f";
        const string indexedSecret = "indexed-key-value-a64f3e";
        var largeSecret = "large-secret-payload-40c2d7-" + new string('x', 12000);

        using (var engine = CreatePasswordEngine(writeConcern: WriteConcern.Journaled))
        {
            var collection = engine.GetBsonCollection("secure_docs");
            collection.Insert(new BsonDocument()
                .Set("_id", 1)
                .Set("secretField", secret)
                .Set("indexedKey", indexedSecret)
                .Set("largePayload", largeSecret));

            engine.EnsureIndex("secure_docs", "indexedKey", "idx_secure_docs_key");

            var dbBytes = ReadAllBytesShared(_dbPath);
            AssertDoesNotContain(dbBytes, secret);
            AssertDoesNotContain(dbBytes, indexedSecret);
            AssertDoesNotContain(dbBytes, largeSecret);

            if (File.Exists(_walPath) && new FileInfo(_walPath).Length > 0)
            {
                var walBytes = ReadAllBytesShared(_walPath);
                AssertDoesNotContain(walBytes, secret);
                AssertDoesNotContain(walBytes, indexedSecret);
                AssertDoesNotContain(walBytes, largeSecret);
            }
        }

        await Assert.That(DatabaseSecurity.HasSecurityMetadata(_dbPath)).IsFalse();

        using (var reopened = new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            Password = "password123",
            EnableJournaling = false
        }))
        {
            var document = reopened.GetBsonCollection("secure_docs").FindAll().Single();
            await Assert.That(document["secretField"].ToString()).IsEqualTo(secret);
            await Assert.That(document["indexedKey"].ToString()).IsEqualTo(indexedSecret);
            await Assert.That(document["largePayload"].ToString()).IsEqualTo(largeSecret);
        }
    }

    [Test]
    public async Task KeyEncryptedDatabase_ShouldRoundTrip()
    {
        var key = Enumerable.Range(0, 32).Select(i => (byte)(i + 1)).ToArray();

        using (var engine = new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            EnableEncryption = true,
            EncryptionKey = key,
            EnableJournaling = false
        }))
        {
            engine.GetBsonCollection("key_docs").Insert(new BsonDocument()
                .Set("_id", 1)
                .Set("value", "key-secret-value-2a98"));
        }

        using (var reopened = new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            EncryptionKey = key,
            EnableJournaling = false
        }))
        {
            var document = reopened.GetBsonCollection("key_docs").FindAll().Single();
            await Assert.That(document["value"].ToString()).IsEqualTo("key-secret-value-2a98");
        }

        await Assert.That(DatabaseSecurity.HasSecurityMetadata(_dbPath)).IsFalse();
    }

    [Test]
    public async Task EncryptedDatabase_ShouldRejectMissingOrWrongCredential()
    {
        using (var engine = CreatePasswordEngine())
        {
            engine.GetBsonCollection("secure_docs").Insert(new BsonDocument().Set("_id", 1).Set("value", "secret"));
        }

        await Assert.That(() => new TinyDbEngine(_dbPath, new TinyDbOptions { EnableJournaling = false }))
            .Throws<UnauthorizedAccessException>();

        await Assert.That(() => new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            Password = "wrongpass123",
            EnableJournaling = false
        })).Throws<UnauthorizedAccessException>();
    }

    [Test]
    public async Task ChangePassword_ShouldRewrapEncryptedDatabase()
    {
        using (var engine = CreatePasswordEngine())
        {
            engine.GetBsonCollection("secure_docs").Insert(new BsonDocument()
                .Set("_id", 1)
                .Set("value", "change-password-secret"));
        }

        var changed = PasswordManager.ChangePassword(_dbPath, "password123", "newpassword456");
        await Assert.That(changed).IsTrue();
        await Assert.That(PasswordManager.VerifyPassword(_dbPath, "password123")).IsFalse();
        await Assert.That(PasswordManager.VerifyPassword(_dbPath, "newpassword456")).IsTrue();

        using var reopened = new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            Password = "newpassword456",
            EnableJournaling = false
        });
        var document = reopened.GetBsonCollection("secure_docs").FindAll().Single();
        await Assert.That(document["value"].ToString()).IsEqualTo("change-password-secret");
    }

    [Test]
    public async Task ChangePassword_WithWrongOldPasswordOnOpenEncryptedEngine_ShouldReturnFalse()
    {
        using var engine = CreatePasswordEngine();
        engine.GetBsonCollection("secure_docs").Insert(new BsonDocument()
            .Set("_id", 1)
            .Set("value", "wrong-old-password-secret"));

        await Assert.That(DatabaseSecurity.AuthenticateDatabase(engine, "password123")).IsTrue();
        await Assert.That(DatabaseSecurity.AuthenticateDatabase(engine, "wrongpass123")).IsFalse();

        var changed = DatabaseSecurity.ChangePassword(engine, "wrongpass123", "newpassword456");
        await Assert.That(changed).IsFalse();
    }

    [Test]
    public async Task PasswordEncryptedDatabase_ShouldIgnoreLegacySecurityMetadata()
    {
        using (var engine = CreatePasswordEngine())
        {
            engine.GetBsonCollection("legacy_security_docs").Insert(new BsonDocument()
                .Set("_id", 1)
                .Set("value", "legacy-security-secret"));
            WriteLegacySecurityMetadata(engine, "stale-password");
        }

        using (var reopened = new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            Password = "password123",
            EnableJournaling = false
        }))
        {
            await Assert.That(reopened.TryGetSecurityMetadata(out _)).IsTrue();
            var document = reopened.GetBsonCollection("legacy_security_docs").FindAll().Single();
            await Assert.That(document["value"].ToString()).IsEqualTo("legacy-security-secret");
            await Assert.That(DatabaseSecurity.AuthenticateDatabase(reopened, "password123")).IsTrue();
            await Assert.That(DatabaseSecurity.AuthenticateDatabase(reopened, "stale-password")).IsFalse();
        }

        var changed = PasswordManager.ChangePassword(_dbPath, "password123", "newpassword456");
        await Assert.That(changed).IsTrue();
        await Assert.That(PasswordManager.VerifyPassword(_dbPath, "password123")).IsFalse();
        await Assert.That(PasswordManager.VerifyPassword(_dbPath, "stale-password")).IsFalse();
        await Assert.That(PasswordManager.VerifyPassword(_dbPath, "newpassword456")).IsTrue();
    }

    [Test]
    public async Task CompactDatabase_ShouldKeepEncryptedStorage()
    {
        const string secret = "compact-secret-value-a8d1";

        using (var engine = CreatePasswordEngine())
        {
            engine.GetBsonCollection("compact_docs").Insert(new BsonDocument()
                .Set("_id", 1)
                .Set("value", secret));

            engine.CompactDatabase();
        }

        var dbBytes = ReadAllBytesShared(_dbPath);
        AssertDoesNotContain(dbBytes, secret);

        using var reopened = new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            Password = "password123",
            EnableJournaling = false
        });
        var document = reopened.GetBsonCollection("compact_docs").FindAll().Single();
        await Assert.That(document["value"].ToString()).IsEqualTo(secret);
    }

    [Test]
    public async Task ReadOnlyEncryptedDatabase_ShouldRequireCorrectPassword()
    {
        using (var engine = CreatePasswordEngine())
        {
            engine.GetBsonCollection("readonly_docs").Insert(new BsonDocument()
                .Set("_id", 1)
                .Set("value", "readonly-secret"));
        }

        using (var reopened = new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            Password = "password123",
            ReadOnly = true,
            EnableJournaling = false
        }))
        {
            var document = reopened.GetBsonCollection("readonly_docs").FindAll().Single();
            await Assert.That(document["value"].ToString()).IsEqualTo("readonly-secret");
        }

        await Assert.That(() => new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            Password = "wrongpass123",
            ReadOnly = true,
            EnableJournaling = false
        })).Throws<UnauthorizedAccessException>();
    }

    [Test]
    public async Task EncryptedWalReplay_ShouldRecoverAndHidePlaintext()
    {
        const string secret = "wal-recovery-secret-7c22";

        using (var engine = CreatePasswordEngine(writeConcern: WriteConcern.Journaled))
        {
            engine.GetBsonCollection("wal_docs").Insert(new BsonDocument()
                .Set("_id", 1)
                .Set("value", secret));

            await Assert.That(File.Exists(_walPath)).IsTrue();
            File.Copy(_walPath, _walPath + ".bak", overwrite: true);
            AssertDoesNotContain(ReadAllBytesShared(_walPath + ".bak"), secret);
        }

        File.Copy(_walPath + ".bak", _walPath, overwrite: true);

        using (var reopened = new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            Password = "password123",
            EnableJournaling = true,
            BackgroundFlushInterval = Timeout.InfiniteTimeSpan
        }))
        {
            var document = reopened.GetBsonCollection("wal_docs").FindAll().Single();
            await Assert.That(document["value"].ToString()).IsEqualTo(secret);
        }

        await Assert.That(new FileInfo(_walPath).Length).IsEqualTo(0);
    }

    [Test]
    public async Task TamperedEncryptedDataPage_ShouldFailToOpen()
    {
        using (var engine = CreatePasswordEngine())
        {
            engine.GetBsonCollection("tamper_docs").Insert(new BsonDocument()
                .Set("_id", 1)
                .Set("value", "tamper-secret"));
        }

        var physicalPageSize = (int)TinyDbOptions.DefaultPageSize + 28;
        using (var stream = new FileStream(_dbPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
            stream.Position = physicalPageSize + 32;
            var value = stream.ReadByte();
            stream.Position = physicalPageSize + 32;
            stream.WriteByte((byte)(value ^ 0x5A));
        }

        await Assert.That(() => new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            Password = "password123",
            EnableJournaling = false
        })).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task TamperedEncryptionMetadata_ShouldFailToOpen()
    {
        using (var engine = CreatePasswordEngine())
        {
            engine.GetBsonCollection("metadata_tamper_docs").Insert(new BsonDocument()
                .Set("_id", 1)
                .Set("value", "metadata-tamper-secret"));
        }

        using (var stream = new FileStream(_dbPath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
        {
            stream.Position = EncryptionMetadataStore.FileOffset + EncryptionMetadata.SerializedLength - 1;
            var value = stream.ReadByte();
            stream.Position = EncryptionMetadataStore.FileOffset + EncryptionMetadata.SerializedLength - 1;
            stream.WriteByte((byte)(value ^ 0x5A));
        }

        await Assert.That(() => new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            Password = "password123",
            EnableJournaling = false
        })).Throws<SecurityCorruptedException>();
    }

    private TinyDbEngine CreatePasswordEngine(WriteConcern writeConcern = WriteConcern.Synced)
    {
        return new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            EnableEncryption = true,
            Password = "password123",
            EnableJournaling = true,
            WriteConcern = writeConcern,
            BackgroundFlushInterval = Timeout.InfiniteTimeSpan
        });
    }

    private static void AssertDoesNotContain(byte[] haystack, string needle)
    {
        var needleBytes = Encoding.UTF8.GetBytes(needle);
        if (IndexOf(haystack, needleBytes) >= 0)
        {
            throw new InvalidOperationException($"Raw file bytes contain plaintext '{needle}'.");
        }
    }

    private static byte[] ReadAllBytesShared(string path)
    {
        using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var buffer = new byte[stream.Length];
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = stream.Read(buffer, offset, buffer.Length - offset);
            if (read == 0) break;
            offset += read;
        }

        return buffer;
    }

    private static void WriteLegacySecurityMetadata(TinyDbEngine engine, string password)
    {
        var salt = new byte[DatabaseSecurityMetadata.SaltLength];
        RandomNumberGenerator.Fill(salt);
        var keyHash = Rfc2898DeriveBytes.Pbkdf2(
            password,
            salt,
            DatabaseSecurity.EncryptionPbkdf2Iterations,
            HashAlgorithmName.SHA256,
            DatabaseSecurityMetadata.KeyHashLength);
        engine.SetSecurityMetadata(new DatabaseSecurityMetadata(salt, keyHash));
    }

    private static int IndexOf(byte[] haystack, byte[] needle)
    {
        if (needle.Length == 0) return 0;
        for (var i = 0; i <= haystack.Length - needle.Length; i++)
        {
            var found = true;
            for (var j = 0; j < needle.Length; j++)
            {
                if (haystack[i + j] == needle[j]) continue;
                found = false;
                break;
            }

            if (found) return i;
        }

        return -1;
    }

    private static string GetWalPath(string dbPath)
    {
        var directory = Path.GetDirectoryName(dbPath)!;
        var fileNameNoExt = Path.GetFileNameWithoutExtension(dbPath);
        var ext = Path.GetExtension(dbPath).TrimStart('.');
        return Path.Combine(directory, $"{fileNameNoExt}-wal.{ext}");
    }

    private static void TryDelete(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch
        {
        }
    }
}
