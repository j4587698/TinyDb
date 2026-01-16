using TinyDb.Security;
using TinyDb.Core;
using TinyDb.Metadata;
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
        PasswordManager.CreateSecureDatabase(_testDbPath, "oldpass").Dispose();
        
        var changed = PasswordManager.ChangePassword(_testDbPath, "oldpass", "newpass");
        await Assert.That(changed).IsTrue();
        
        await Assert.That(PasswordManager.VerifyPassword(_testDbPath, "newpass")).IsTrue();
        await Assert.That(PasswordManager.VerifyPassword(_testDbPath, "oldpass")).IsFalse();
    }

    [Test]
    public async Task RemovePassword_Should_Work()
    {
        PasswordManager.CreateSecureDatabase(_testDbPath, "password").Dispose();
        
        var removed = PasswordManager.RemovePassword(_testDbPath, "password");
        await Assert.That(removed).IsTrue();
        
        await Assert.That(PasswordManager.IsPasswordProtected(_testDbPath)).IsFalse();
    }

    [Test]
    public async Task GenerateStrongPassword_Should_Generate_Valid_Password()
    {
        var pwd = PasswordManager.GenerateStrongPassword(16);
        await Assert.That(pwd.Length).IsEqualTo(16);
        await Assert.That((int)PasswordManager.CheckPasswordStrength(pwd)).IsGreaterThanOrEqualTo((int)PasswordStrength.Strong);
    }
}
