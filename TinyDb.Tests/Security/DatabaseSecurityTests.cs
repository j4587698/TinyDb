using TinyDb.Core;
using TinyDb.Security;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Security;

public class DatabaseSecurityTests : IDisposable
{
    private readonly string _testDbPath;

    public DatabaseSecurityTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"sec_test_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Security_Password_Lifecycle_Should_Work()
    {
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            // 1. Create secure database
            DatabaseSecurity.CreateSecureDatabase(engine, "password123");
            await Assert.That(DatabaseSecurity.IsDatabaseSecure(engine)).IsTrue();

            // 2. Authenticate correctly
            await Assert.That(DatabaseSecurity.AuthenticateDatabase(engine, "password123")).IsTrue();

            // 3. Authenticate incorrectly
            await Assert.That(DatabaseSecurity.AuthenticateDatabase(engine, "wrongpassword")).IsFalse();

            // 4. Change password
            var changed = DatabaseSecurity.ChangePassword(engine, "password123", "newpassword456");
            await Assert.That(changed).IsTrue();
            await Assert.That(DatabaseSecurity.AuthenticateDatabase(engine, "newpassword456")).IsTrue();

            // 5. Remove password
            var removed = DatabaseSecurity.RemovePassword(engine, "newpassword456");
            await Assert.That(removed).IsTrue();
            await Assert.That(DatabaseSecurity.IsDatabaseSecure(engine)).IsFalse();
        }
    }

    [Test]
    public async Task CreateSecureDatabase_When_Already_Protected_Should_Throw()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        DatabaseSecurity.CreateSecureDatabase(engine, "password123");

        await Assert.That(() => DatabaseSecurity.CreateSecureDatabase(engine, "newpass"))
            .Throws<DatabaseAlreadyProtectedException>();
    }

    [Test]
    public async Task ChangePassword_With_Wrong_Old_Password_Should_Return_False()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        DatabaseSecurity.CreateSecureDatabase(engine, "password123");

        var result = DatabaseSecurity.ChangePassword(engine, "wrong", "newpass");
        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task AuthenticateDatabase_With_NullPassword_ShouldThrow()
    {
        using var engine = new TinyDbEngine(_testDbPath);

        await Assert.That(() => DatabaseSecurity.AuthenticateDatabase(engine, null!))
            .ThrowsExactly<ArgumentNullException>();
    }

    [Test]
    public async Task ChangePassword_With_InvalidNewPassword_ShouldThrow()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        DatabaseSecurity.CreateSecureDatabase(engine, "password123");

        await Assert.That(() => DatabaseSecurity.ChangePassword(engine, "password123", null!))
            .Throws<ArgumentException>();

        await Assert.That(() => DatabaseSecurity.ChangePassword(engine, "password123", "123"))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task RemovePassword_With_WrongPassword_ShouldReturnFalse()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        DatabaseSecurity.CreateSecureDatabase(engine, "password123");

        var removed = DatabaseSecurity.RemovePassword(engine, "wrongpassword");
        await Assert.That(removed).IsFalse();
    }

    [Test]
    public async Task CreateSecureDatabase_With_Invalid_Password_Should_Throw()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        await Assert.That(() => DatabaseSecurity.CreateSecureDatabase(engine, ""))
            .Throws<ArgumentException>();
        await Assert.That(() => DatabaseSecurity.CreateSecureDatabase(engine, "123"))
            .Throws<ArgumentException>();
    }
}
