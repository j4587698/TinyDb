using System;
using System.IO;
using TinyDb.Core;
using TinyDb.Security;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Security;

public class DatabaseSecurityTests
{
    [Test]
    public async Task Engine_Should_Create_And_Enforce_Password_Protection()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"secure_db_{Guid.NewGuid():N}.db");
        var walPath = $"{dbPath}.wal";

        try
        {
            using (var engine = new TinyDbEngine(dbPath, new TinyDbOptions { Password = "StrongPass123!" }))
            {
                await Assert.That(DatabaseSecurity.IsDatabaseSecure(engine)).IsTrue();
            }

            await Assert.That(() => new TinyDbEngine(dbPath)).Throws<UnauthorizedAccessException>();

            using var reopened = new TinyDbEngine(dbPath, new TinyDbOptions { Password = "StrongPass123!" });
            await Assert.That(DatabaseSecurity.AuthenticateDatabase(reopened, "StrongPass123!")).IsTrue();
        }
        finally
        {
            if (File.Exists(dbPath))
            {
                File.Delete(dbPath);
            }

            if (File.Exists(walPath))
            {
                File.Delete(walPath);
            }
        }
    }

    [Test]
    public async Task PasswordManager_Should_Manage_Database_Password_Lifecycle()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"manager_db_{Guid.NewGuid():N}.db");
        var walPath = $"{dbPath}.wal";

        try
        {
            using (new TinyDbEngine(dbPath))
            {
                // 初始化空数据库
            }

            PasswordManager.SetPassword(dbPath, "InitPass123!");

            await Assert.That(PasswordManager.IsPasswordProtected(dbPath)).IsTrue();
            await Assert.That(PasswordManager.VerifyPassword(dbPath, "InitPass123!")).IsTrue();
            await Assert.That(PasswordManager.VerifyPassword(dbPath, "WrongPass456!")).IsFalse();

            var changed = PasswordManager.ChangePassword(dbPath, "InitPass123!", "NewPass456!");
            await Assert.That(changed).IsTrue();
            await Assert.That(PasswordManager.VerifyPassword(dbPath, "NewPass456!")).IsTrue();

            var removed = PasswordManager.RemovePassword(dbPath, "NewPass456!");
            await Assert.That(removed).IsTrue();
            await Assert.That(PasswordManager.IsPasswordProtected(dbPath)).IsFalse();
        }
        finally
        {
            if (File.Exists(dbPath))
            {
                File.Delete(dbPath);
            }

            if (File.Exists(walPath))
            {
                File.Delete(walPath);
            }
        }
    }
}
