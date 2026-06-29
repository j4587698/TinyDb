using TinyDb.Security;
using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Metadata;
using System.Text;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Security;

public class PasswordManagerTests : IDisposable
{
    private readonly string _testDbPath;

    public PasswordManagerTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"pwd_mgr_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task CreateSecureDatabase_Should_Create_Protected_Db()
    {
        using (var engine = PasswordManager.CreateSecureDatabase(_testDbPath, "password123"))
        {
            await Assert.That(engine.IsInitialized).IsTrue();
        } // Dispose here
        
        await Assert.That(PasswordManager.IsPasswordProtected(_testDbPath)).IsTrue();
    }

    [Test]
    public async Task CreateSecureDatabase_Should_Encrypt_Data_At_Rest()
    {
        const string secret = "password-manager-secret-7f01";

        using (var engine = PasswordManager.CreateSecureDatabase(_testDbPath, "password123"))
        {
            var collection = engine.GetBsonCollection("secure_docs");
            collection.Insert(new BsonDocument()
                .Set("_id", 1)
                .Set("value", secret));
            engine.Flush();
        }

        var dbBytes = File.ReadAllBytes(_testDbPath);
        await Assert.That(IndexOf(dbBytes, Encoding.UTF8.GetBytes(secret))).IsEqualTo(-1);
        await Assert.That(PasswordManager.VerifyPassword(_testDbPath, "wrongpass123")).IsFalse();
        await Assert.That(PasswordManager.VerifyPassword(_testDbPath, "password123")).IsTrue();
    }

    [Test]
    public async Task OpenSecureDatabase_Should_Work_With_Correct_Password()
    {
        PasswordManager.CreateSecureDatabase(_testDbPath, "password123").Dispose();
        
        using var engine = PasswordManager.OpenSecureDatabase(_testDbPath, "password123");
        await Assert.That(engine.IsInitialized).IsTrue();
    }

    [Test]
    public async Task OpenDatabase_Should_Detect_Password()
    {
        PasswordManager.CreateSecureDatabase(_testDbPath, "password123").Dispose();
        
        // Should fail without password
        await Assert.That(() => PasswordManager.OpenDatabase(_testDbPath))
            .Throws<UnauthorizedAccessException>();
            
        // Should succeed with password
        using var engine = PasswordManager.OpenDatabase(_testDbPath, "password123");
        await Assert.That(engine.IsInitialized).IsTrue();
    }

    [Test]
    public async Task ChangePassword_Should_Work()
    {
        PasswordManager.CreateSecureDatabase(_testDbPath, "oldpass123").Dispose();
        
        var changed = PasswordManager.ChangePassword(_testDbPath, "oldpass123", "newpass123");
        await Assert.That(changed).IsTrue();
        
        await Assert.That(PasswordManager.VerifyPassword(_testDbPath, "newpass123")).IsTrue();
        await Assert.That(PasswordManager.VerifyPassword(_testDbPath, "oldpass123")).IsFalse();
    }

    [Test]
    public async Task RemovePassword_OnEncryptedDatabase_ShouldThrow()
    {
        PasswordManager.CreateSecureDatabase(_testDbPath, "password").Dispose();
        
        await Assert.That(() => PasswordManager.RemovePassword(_testDbPath, "password"))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task GenerateStrongPassword_Should_Generate_Valid_Password()
    {
        var pwd = PasswordManager.GenerateStrongPassword(16);
        await Assert.That(pwd.Length).IsEqualTo(16);
        await Assert.That((int)PasswordManager.CheckPasswordStrength(pwd)).IsGreaterThanOrEqualTo((int)PasswordStrength.Strong);
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
}
