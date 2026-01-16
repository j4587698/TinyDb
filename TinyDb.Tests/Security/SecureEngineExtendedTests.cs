using System;
using System.IO;
using TinyDb.Core;
using TinyDb.Security;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Security;

[NotInParallel]
public class SecureEngineExtendedTests
{
    private string _testFile = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"sec_ext_{Guid.NewGuid():N}.db");
    }

    [After(Test)]
    public void Cleanup()
    {
        if (File.Exists(_testFile)) File.Delete(_testFile);
    }

    [Test]
    public async Task Constructor_InvalidPassword_ShouldThrow()
    {
        await Assert.That(() => new SecureTinyDbEngine(_testFile, "123"))
            .Throws<ArgumentException>();
            
        await Assert.That(() => new SecureTinyDbEngine(_testFile, ""))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task Constructor_ProtectedButNoPassword_ShouldThrow()
    {
        // Setup protected DB
        using (var engine = new TinyDbEngine(_testFile))
        {
            DatabaseSecurity.CreateSecureDatabase(engine, "password");
        }

        await Assert.That(() => new SecureTinyDbEngine(_testFile))
            .Throws<UnauthorizedAccessException>();
    }

    [Test]
    public async Task Constructor_UnprotectedButPasswordProvided_ShouldProtectIt()
    {
        // Setup unprotected DB
        using (var engine = new TinyDbEngine(_testFile)) { }

        // Should NOT throw, but protect it (TinyDbEngine behavior)
        using var secureEngine = new SecureTinyDbEngine(_testFile, "password");
        
        await Assert.That(secureEngine.IsAuthenticated).IsTrue();
        await Assert.That(secureEngine.IsPasswordProtected()).IsTrue();
    }

    [Test]
    public async Task SetPassword_AlreadyProtected_ShouldThrow()
    {
        // Setup unprotected
        using (var engine = new TinyDbEngine(_testFile)) { }
        
        using var secureEngine = new SecureTinyDbEngine(_testFile);
        secureEngine.SetPassword("newpass"); // Should work
        
        await Assert.That(() => secureEngine.SetPassword("another"))
            .Throws<InvalidOperationException>();
    }
    
    [Test]
    public async Task Access_AfterDispose_ShouldThrow()
    {
        var secureEngine = new SecureTinyDbEngine(_testFile);
        secureEngine.Dispose();
        
        await Assert.That(() => secureEngine.GetCollectionNames())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task ChangePassword_WrongOldPassword_ShouldReturnFalse()
    {
        // Setup protected DB
        using (var engine = new TinyDbEngine(_testFile))
        {
            DatabaseSecurity.CreateSecureDatabase(engine, "password");
        }
        
        using var secureEngine = new SecureTinyDbEngine(_testFile, "password");
        var changed = secureEngine.ChangePassword("wrong", "newpassword");
        await Assert.That(changed).IsFalse();
    }
}
