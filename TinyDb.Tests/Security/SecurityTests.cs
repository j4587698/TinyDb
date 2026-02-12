using System;
using System.IO;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Security;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Security;

[NotInParallel]
public class SecurityTests
{
    private string _testFile = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"security_test_{Guid.NewGuid():N}.db");
    }

    [After(Test)]
    public void Cleanup()
    {
        if (File.Exists(_testFile))
        {
            File.Delete(_testFile);
        }
    }

    [Test]
    public async Task SecureDatabase_Lifecycle_ShouldWork()
    {
        // 1. Create and protect
        using (var engine = new TinyDbEngine(_testFile))
        {
            await Assert.That(DatabaseSecurity.IsDatabaseSecure(engine)).IsFalse();
            DatabaseSecurity.CreateSecureDatabase(engine, "password123");
            await Assert.That(DatabaseSecurity.IsDatabaseSecure(engine)).IsTrue();
        }

        // 2. Open with SecureTinyDbEngine
        using (var secureEngine = new SecureTinyDbEngine(_testFile, "password123"))
        {
            await Assert.That(secureEngine.IsAuthenticated).IsTrue();
            await Assert.That(secureEngine.IsPasswordProtected()).IsTrue();
        }

        // 3. Open with wrong password should throw
        await Assert.That(() => new SecureTinyDbEngine(_testFile, "wrong_password"))
            .Throws<UnauthorizedAccessException>();

        // 4. Change password
        using (var secureEngine = new SecureTinyDbEngine(_testFile, "password123"))
        {
            var changed = secureEngine.ChangePassword("password123", "new_password_456");
            await Assert.That(changed).IsTrue();
        }

        // 5. Open with new password
        using (var secureEngine = new SecureTinyDbEngine(_testFile, "new_password_456"))
        {
            await Assert.That(secureEngine.IsAuthenticated).IsTrue();
        }

        // 6. Remove password
        using (var secureEngine = new SecureTinyDbEngine(_testFile, "new_password_456"))
        {
            var removed = secureEngine.RemovePassword("new_password_456");
            await Assert.That(removed).IsTrue();
            await Assert.That(secureEngine.IsPasswordProtected()).IsFalse();
        }
    }

    [Test]
    public async Task SecureTinyDbEngine_BackwardCompatibility_ShouldWork()
    {
        // Create unprotected DB
        using (var engine = new TinyDbEngine(_testFile))
        {
            engine.GetBsonCollection("test").Insert(new BsonDocument().Set("a", 1));
        }

        // Open with SecureTinyDbEngine without password
        using (var secureEngine = new SecureTinyDbEngine(_testFile))
        {
            await Assert.That(secureEngine.IsAuthenticated).IsTrue();
            await Assert.That(secureEngine.IsPasswordProtected()).IsFalse();
            var col = secureEngine.GetCollection<BsonDocument>("test");
            await Assert.That(col).IsNotNull();
        }
    }

    [Test]
    public async Task CreateSecureDatabase_WhenAlreadyProtected_ShouldThrow()
    {
        using var engine = new TinyDbEngine(_testFile);
        DatabaseSecurity.CreateSecureDatabase(engine, "pass1");
        
        await Assert.That(() => DatabaseSecurity.CreateSecureDatabase(engine, "pass2"))
            .Throws<DatabaseAlreadyProtectedException>();
    }
}
