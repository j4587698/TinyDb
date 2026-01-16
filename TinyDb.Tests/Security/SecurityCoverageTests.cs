using System;
using TinyDb.Core;
using TinyDb.Security;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Security;

[NotInParallel]
public class SecurityCoverageTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"sec_cov_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testFile);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_testFile))
            File.Delete(_testFile);
    }

    [Test]
    public async Task Exceptions_ShouldBeThrownCorrectly()
    {
        // DatabaseAlreadyProtectedException
        await Assert.That(() => throw new DatabaseAlreadyProtectedException())
            .Throws<DatabaseAlreadyProtectedException>();
        await Assert.That(() => throw new DatabaseAlreadyProtectedException("test"))
            .Throws<DatabaseAlreadyProtectedException>();
        await Assert.That(() => throw new DatabaseAlreadyProtectedException("test", new Exception()))
            .Throws<DatabaseAlreadyProtectedException>();

        // PasswordVerificationException
        await Assert.That(() => throw new PasswordVerificationException())
            .Throws<PasswordVerificationException>();
        await Assert.That(() => throw new PasswordVerificationException("test"))
            .Throws<PasswordVerificationException>();
        await Assert.That(() => throw new PasswordVerificationException("test", new Exception()))
            .Throws<PasswordVerificationException>();

        // WeakPasswordException
        await Assert.That(() => throw new WeakPasswordException())
            .Throws<WeakPasswordException>();
        await Assert.That(() => throw new WeakPasswordException("test"))
            .Throws<WeakPasswordException>();
        await Assert.That(() => throw new WeakPasswordException("test", new Exception()))
            .Throws<WeakPasswordException>();

        // DatabaseNotProtectedException
        await Assert.That(() => throw new DatabaseNotProtectedException())
            .Throws<DatabaseNotProtectedException>();
        await Assert.That(() => throw new DatabaseNotProtectedException("test"))
            .Throws<DatabaseNotProtectedException>();
        await Assert.That(() => throw new DatabaseNotProtectedException("test", new Exception()))
            .Throws<DatabaseNotProtectedException>();

        // SecurityCorruptedException
        await Assert.That(() => throw new SecurityCorruptedException())
            .Throws<SecurityCorruptedException>();
        await Assert.That(() => throw new SecurityCorruptedException("test"))
            .Throws<SecurityCorruptedException>();
        await Assert.That(() => throw new SecurityCorruptedException("test", new Exception()))
            .Throws<SecurityCorruptedException>();
    }

    [Test]
    public async Task SecureTinyDbEngine_ShouldHandlePasswordChange()
    {
        // 1. Protect DB
        DatabaseSecurity.CreateSecureDatabase(_engine, "StrongPass123!");
        
        // 2. Auth
        await Assert.That(DatabaseSecurity.AuthenticateDatabase(_engine, "StrongPass123!")).IsTrue();
        
        // 3. Change Password
        DatabaseSecurity.ChangePassword(_engine, "StrongPass123!", "NewStrongPass456!");
        
        // 4. Verify old fails, new works
        await Assert.That(DatabaseSecurity.AuthenticateDatabase(_engine, "StrongPass123!")).IsFalse();
        await Assert.That(DatabaseSecurity.AuthenticateDatabase(_engine, "NewStrongPass456!")).IsTrue();
        
        // 5. Remove password
        DatabaseSecurity.RemovePassword(_engine, "NewStrongPass456!");
        await Assert.That(DatabaseSecurity.IsDatabaseSecure(_engine)).IsFalse();
    }
}