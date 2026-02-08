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

    /// <summary>
    /// 创建受密码保护的数据库。
    /// </summary>
    /// <param name="engine">数据库引擎实例。</param>
    /// <param name="password">要设置的密码。</param>
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

    /// <summary>
    /// 验证数据库密码。
    /// </summary>
    /// <param name="engine">数据库引擎实例。</param>
    /// <param name="password">要验证的密码。</param>
    /// <returns>如果验证成功则为 true；否则为 false。</returns>
    public static bool AuthenticateDatabase(TinyDbEngine engine, string password)
    {
        if (password == null) throw new ArgumentNullException(nameof(password));

        if (!engine.TryGetSecurityMetadata(out var metadata))
            return true;

        var derivedKey = DeriveKey(password, metadata.Salt);
        return CryptographicOperations.FixedTimeEquals(derivedKey, metadata.KeyHash);
    }

    /// <summary>
    /// 更改数据库密码。
    /// </summary>
    /// <param name="engine">数据库引擎实例。</param>
    /// <param name="oldPassword">旧密码。</param>
    /// <param name="newPassword">新密码。</param>
    /// <returns>如果更改成功则为 true；否则为 false。</returns>
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

    /// <summary>
    /// 移除数据库密码保护。
    /// </summary>
    /// <param name="engine">数据库引擎实例。</param>
    /// <param name="password">当前密码。</param>
    /// <returns>如果成功移除则为 true；否则为 false。</returns>
    public static bool RemovePassword(TinyDbEngine engine, string password)
    {
        if (!AuthenticateDatabase(engine, password))
            return false;

        if (!engine.TryGetSecurityMetadata(out _))
            return false;

        engine.ClearSecurityMetadata();
        return true;
    }

    /// <summary>
    /// 检查数据库是否受密码保护。
    /// </summary>
    /// <param name="engine">数据库引擎实例。</param>
    /// <returns>如果受保护则为 true。</returns>
    public static bool IsDatabaseSecure(TinyDbEngine engine)
    {
        return engine.TryGetSecurityMetadata(out _);
    }

    /// <summary>
    /// 检查指定的数据库文件是否包含安全元数据。
    /// </summary>
    /// <param name="databasePath">数据库文件路径。</param>
    /// <returns>如果包含则为 true。</returns>
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
        return TryReadSecurityMetadata(stream, out metadata);
    }

    internal static bool TryReadSecurityMetadata(Stream stream, out DatabaseSecurityMetadata metadata)
    {
        metadata = default;

        if (stream.Length < DatabaseHeader.Size)
            return false;

        var buffer = new byte[DatabaseHeader.Size];
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = stream.Read(buffer, offset, buffer.Length - offset);
            if (read == 0)
                return false;

            offset += read;
        }

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
        return Rfc2898DeriveBytes.Pbkdf2(password, salt, Iterations, HashAlgorithmName.SHA256, KeySize);
    }
}
