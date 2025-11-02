using System;
using System.IO;
using System.Security.Cryptography;
using TinyDb.Core;

namespace TinyDb.Security;

/// <summary>
/// TinyDb 数据库安全认证系统。
/// 通过数据库头部存储密码派生信息，提供数据库级密码保护。
/// </summary>
public static class DatabaseSecurity
{
    private const int Iterations = 10000;
    private const int KeySize = 32;
    private const int SaltSize = 16;

    public static void CreateSecureDatabase(TinyDbEngine engine, string password)
    {
        if (string.IsNullOrWhiteSpace(password))
            throw new ArgumentException("密码不能为空", nameof(password));

        if (password.Length < 4)
            throw new ArgumentException("密码长度至少4位", nameof(password));

        if (engine.TryGetSecurityMetadata(out _))
            throw new DatabaseAlreadyProtectedException();

        var metadata = BuildSecurityMetadata(password);
        engine.SetSecurityMetadata(metadata);
    }

    public static bool AuthenticateDatabase(TinyDbEngine engine, string password)
    {
        if (password == null) throw new ArgumentNullException(nameof(password));

        if (!engine.TryGetSecurityMetadata(out var metadata))
            return true;

        var derivedKey = DeriveKey(password, metadata.Salt);
        return CryptographicOperations.FixedTimeEquals(derivedKey, metadata.KeyHash);
    }

    public static bool ChangePassword(TinyDbEngine engine, string oldPassword, string newPassword)
    {
        if (!AuthenticateDatabase(engine, oldPassword))
            return false;

        if (string.IsNullOrWhiteSpace(newPassword))
            throw new ArgumentException("新密码不能为空", nameof(newPassword));

        if (newPassword.Length < 4)
            throw new ArgumentException("新密码长度至少4位", nameof(newPassword));

        if (!engine.TryGetSecurityMetadata(out _))
            return false;

        var metadata = BuildSecurityMetadata(newPassword);
        engine.SetSecurityMetadata(metadata);
        return true;
    }

    public static bool RemovePassword(TinyDbEngine engine, string password)
    {
        if (!AuthenticateDatabase(engine, password))
            return false;

        if (!engine.TryGetSecurityMetadata(out _))
            return false;

        engine.ClearSecurityMetadata();
        return true;
    }

    public static bool IsDatabaseSecure(TinyDbEngine engine)
    {
        return engine.TryGetSecurityMetadata(out _);
    }

    internal static bool HasSecurityMetadata(string databasePath)
    {
        return TryReadSecurityMetadata(databasePath, out _);
    }

    private static bool TryReadSecurityMetadata(string databasePath, out DatabaseSecurityMetadata metadata)
    {
        metadata = default;

        if (!File.Exists(databasePath))
            return false;

        using var stream = File.Open(databasePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        if (stream.Length < DatabaseHeader.Size)
            return false;

        var buffer = new byte[DatabaseHeader.Size];
        var read = stream.Read(buffer, 0, buffer.Length);
        if (read < buffer.Length)
            return false;

        var header = DatabaseHeader.FromByteArray(buffer);
        return header.TryGetSecurityMetadata(out metadata);
    }

    private static DatabaseSecurityMetadata BuildSecurityMetadata(string password)
    {
        var salt = GenerateSalt();
        var keyHash = DeriveKey(password, salt);
        return new DatabaseSecurityMetadata(salt, keyHash);
    }

    private static byte[] GenerateSalt()
    {
        var salt = new byte[SaltSize];
        RandomNumberGenerator.Fill(salt);
        return salt;
    }

    private static byte[] DeriveKey(string password, byte[] salt)
    {
        using var pbkdf2 = new Rfc2898DeriveBytes(password, salt, Iterations, HashAlgorithmName.SHA256);
        return pbkdf2.GetBytes(KeySize);
    }
}
