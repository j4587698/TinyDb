using System;
using System.IO;
using System.Linq;
using TinyDb.Security;
using TinyDb.Core;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Security;

/// <summary>
/// Edge case tests for PasswordManager to improve coverage
/// Focuses on: password validation, strength checking, file not found scenarios
/// </summary>
public class PasswordManagerEdgeCaseTests : IDisposable
{
    private readonly string _testDbPath;

    public PasswordManagerEdgeCaseTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"pwd_edge_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    #region GenerateStrongPassword Tests

    [Test]
    public async Task GenerateStrongPassword_WithLengthLessThan4_ShouldThrowArgumentException()
    {
        await Assert.That(() => PasswordManager.GenerateStrongPassword(3))
            .Throws<ArgumentException>();
        
        await Assert.That(() => PasswordManager.GenerateStrongPassword(2))
            .Throws<ArgumentException>();
        
        await Assert.That(() => PasswordManager.GenerateStrongPassword(1))
            .Throws<ArgumentException>();
        
        await Assert.That(() => PasswordManager.GenerateStrongPassword(0))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task GenerateStrongPassword_WithLength4_ShouldSucceed()
    {
        var pwd = PasswordManager.GenerateStrongPassword(4);
        await Assert.That(pwd.Length).IsEqualTo(4);
    }

    [Test]
    public async Task GenerateStrongPassword_WithoutSpecialChars_ShouldNotContainSpecialChars()
    {
        const string specialChars = "!@#$%^&*()_+-=[]{}|;:,.<>?";
        
        var pwd = PasswordManager.GenerateStrongPassword(12, includeSpecialChars: false);
        
        await Assert.That(pwd.Length).IsEqualTo(12);
        await Assert.That(pwd.Any(c => specialChars.Contains(c))).IsFalse();
    }

    [Test]
    public async Task GenerateStrongPassword_WithSpecialChars_ShouldContainVariousCharTypes()
    {
        var pwd = PasswordManager.GenerateStrongPassword(16, includeSpecialChars: true);
        
        await Assert.That(pwd.Length).IsEqualTo(16);
        // Should contain at least lowercase, uppercase, and digits
        await Assert.That(pwd.Any(char.IsLower)).IsTrue();
        await Assert.That(pwd.Any(char.IsUpper)).IsTrue();
        await Assert.That(pwd.Any(char.IsDigit)).IsTrue();
    }

    [Test]
    public async Task GenerateStrongPassword_ShouldGenerateDifferentPasswords()
    {
        var pwd1 = PasswordManager.GenerateStrongPassword(12);
        var pwd2 = PasswordManager.GenerateStrongPassword(12);
        
        // Very unlikely to be the same
        await Assert.That(pwd1).IsNotEqualTo(pwd2);
    }

    #endregion

    #region CheckPasswordStrength Tests

    [Test]
    public async Task CheckPasswordStrength_EmptyPassword_ShouldReturnWeak()
    {
        await Assert.That(PasswordManager.CheckPasswordStrength("")).IsEqualTo(PasswordStrength.Weak);
        await Assert.That(PasswordManager.CheckPasswordStrength(null!)).IsEqualTo(PasswordStrength.Weak);
    }

    [Test]
    public async Task CheckPasswordStrength_WeakPassword_ShouldReturnWeak()
    {
        // Score <= 2: Very short, no variety
        await Assert.That(PasswordManager.CheckPasswordStrength("ab")).IsEqualTo(PasswordStrength.Weak);
        await Assert.That(PasswordManager.CheckPasswordStrength("123")).IsEqualTo(PasswordStrength.Weak);
        await Assert.That(PasswordManager.CheckPasswordStrength("abc")).IsEqualTo(PasswordStrength.Weak);
    }

    [Test]
    public async Task CheckPasswordStrength_MediumPassword_ShouldReturnMedium()
    {
        // Score 3-4: Some variety but not fully strong
        await Assert.That(PasswordManager.CheckPasswordStrength("Abc123")).IsEqualTo(PasswordStrength.Medium);
        await Assert.That(PasswordManager.CheckPasswordStrength("Password1")).IsEqualTo(PasswordStrength.Medium);
    }

    [Test]
    public async Task CheckPasswordStrength_StrongPassword_ShouldReturnStrong()
    {
        // Score 5-6: Good length with variety
        await Assert.That(PasswordManager.CheckPasswordStrength("Abc12345!")).IsEqualTo(PasswordStrength.Strong);
        await Assert.That(PasswordManager.CheckPasswordStrength("MyPassw0rd!")).IsEqualTo(PasswordStrength.Strong);
    }

    [Test]
    public async Task CheckPasswordStrength_VeryStrongPassword_ShouldReturnVeryStrong()
    {
        // Score >= 7: Must be >=16 chars with all character types (4 char types + 3 length points)
        // Password: >= 16 chars, has lowercase, uppercase, digits, special chars
        await Assert.That(PasswordManager.CheckPasswordStrength("AbcDefGh12345!@#")).IsEqualTo(PasswordStrength.VeryStrong);
        await Assert.That(PasswordManager.CheckPasswordStrength("MyV3ryStr0ngP@ssword!")).IsEqualTo(PasswordStrength.VeryStrong);
    }

    [Test]
    public async Task CheckPasswordStrength_LongPasswordOnlyLowercase_ShouldNotBeVeryStrong()
    {
        // Even a long password with only lowercase should not be VeryStrong
        var strength = PasswordManager.CheckPasswordStrength("abcdefghijklmnopqrstuvwxyz");
        await Assert.That(strength).IsNotEqualTo(PasswordStrength.VeryStrong);
    }

    #endregion

    #region SetPassword Validation Tests

    [Test]
    public async Task SetPassword_WithEmptyPassword_ShouldThrowArgumentException()
    {
        // Create a database first
        using (new TinyDbEngine(_testDbPath)) { }
        
        await Assert.That(() => PasswordManager.SetPassword(_testDbPath, ""))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task SetPassword_WithWhitespacePassword_ShouldThrowArgumentException()
    {
        // Create a database first
        using (new TinyDbEngine(_testDbPath)) { }
        
        await Assert.That(() => PasswordManager.SetPassword(_testDbPath, "   "))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task SetPassword_WithShortPassword_ShouldThrowArgumentException()
    {
        // Create a database first
        using (new TinyDbEngine(_testDbPath)) { }
        
        await Assert.That(() => PasswordManager.SetPassword(_testDbPath, "abc"))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task SetPassword_WithMinLengthPassword_ShouldSucceed()
    {
        // Create a database first
        using (new TinyDbEngine(_testDbPath)) { }
        
        // 4 characters is the minimum
        PasswordManager.SetPassword(_testDbPath, "abcd");
        
        await Assert.That(PasswordManager.IsPasswordProtected(_testDbPath)).IsTrue();
    }

    #endregion

    #region File Not Found Tests

    [Test]
    public async Task OpenSecureDatabase_WithNonExistentFile_ShouldThrowFileNotFoundException()
    {
        var nonExistentPath = Path.Combine(Path.GetTempPath(), $"non_existent_{Guid.NewGuid():N}.db");
        
        await Assert.That(() => PasswordManager.OpenSecureDatabase(nonExistentPath, "password"))
            .Throws<FileNotFoundException>();
    }

    [Test]
    public async Task OpenDatabase_WithNonExistentFile_ShouldThrowFileNotFoundException()
    {
        var nonExistentPath = Path.Combine(Path.GetTempPath(), $"non_existent_{Guid.NewGuid():N}.db");
        
        await Assert.That(() => PasswordManager.OpenDatabase(nonExistentPath))
            .Throws<FileNotFoundException>();
    }

    [Test]
    public async Task OpenDatabase_WithNonExistentFileAndPassword_ShouldThrowFileNotFoundException()
    {
        var nonExistentPath = Path.Combine(Path.GetTempPath(), $"non_existent_{Guid.NewGuid():N}.db");
        
        await Assert.That(() => PasswordManager.OpenDatabase(nonExistentPath, "password"))
            .Throws<FileNotFoundException>();
    }

    #endregion

    #region CreateSecureDatabase Validation Tests

    [Test]
    public async Task CreateSecureDatabase_WithEmptyPassword_ShouldThrowArgumentException()
    {
        await Assert.That(() => PasswordManager.CreateSecureDatabase(_testDbPath, ""))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task CreateSecureDatabase_WithShortPassword_ShouldThrowArgumentException()
    {
        await Assert.That(() => PasswordManager.CreateSecureDatabase(_testDbPath, "abc"))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task CreateSecureDatabase_WithExistingFile_ShouldReplaceFile()
    {
        // Create a file first
        File.WriteAllText(_testDbPath, "dummy content");
        
        // CreateSecureDatabase should replace it
        using var engine = PasswordManager.CreateSecureDatabase(_testDbPath, "password");
        await Assert.That(engine.IsInitialized).IsTrue();
    }

    #endregion

    #region ChangePassword Validation Tests

    [Test]
    public async Task ChangePassword_WithEmptyNewPassword_ShouldThrowArgumentException()
    {
        PasswordManager.CreateSecureDatabase(_testDbPath, "oldpass").Dispose();
        
        await Assert.That(() => PasswordManager.ChangePassword(_testDbPath, "oldpass", ""))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task ChangePassword_WithShortNewPassword_ShouldThrowArgumentException()
    {
        PasswordManager.CreateSecureDatabase(_testDbPath, "oldpass").Dispose();
        
        await Assert.That(() => PasswordManager.ChangePassword(_testDbPath, "oldpass", "abc"))
            .Throws<ArgumentException>();
    }

    #endregion

    #region VerifyPassword Tests

    [Test]
    public async Task VerifyPassword_WithWrongPassword_ShouldReturnFalse()
    {
        PasswordManager.CreateSecureDatabase(_testDbPath, "correct").Dispose();
        
        await Assert.That(PasswordManager.VerifyPassword(_testDbPath, "wrong")).IsFalse();
    }

    [Test]
    public async Task VerifyPassword_WithCorrectPassword_ShouldReturnTrue()
    {
        PasswordManager.CreateSecureDatabase(_testDbPath, "correct").Dispose();
        
        await Assert.That(PasswordManager.VerifyPassword(_testDbPath, "correct")).IsTrue();
    }

    #endregion

    #region IsPasswordProtected Tests

    [Test]
    public async Task IsPasswordProtected_WithUnprotectedDatabase_ShouldReturnFalse()
    {
        using (new TinyDbEngine(_testDbPath)) { }
        
        await Assert.That(PasswordManager.IsPasswordProtected(_testDbPath)).IsFalse();
    }

    [Test]
    public async Task IsPasswordProtected_WithProtectedDatabase_ShouldReturnTrue()
    {
        PasswordManager.CreateSecureDatabase(_testDbPath, "password").Dispose();
        
        await Assert.That(PasswordManager.IsPasswordProtected(_testDbPath)).IsTrue();
    }

    #endregion
}
