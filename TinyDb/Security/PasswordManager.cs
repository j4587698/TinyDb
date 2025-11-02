using TinyDb.Core;
using TinyDb.Metadata;

namespace TinyDb.Security;

/// <summary>
/// TinyDb密码管理器，提供便捷的密码管理API
/// </summary>
public static class PasswordManager
{
    /// <summary>
    /// 为现有数据库设置密码保护
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <param name="password">要设置的密码</param>
    /// <exception cref="InvalidOperationException">数据库已受保护时抛出</exception>
    /// <exception cref="ArgumentException">密码无效时抛出</exception>
    public static void SetPassword(string filePath, string password)
    {
        ValidatePassword(password);

        var options = new TinyDbOptions { Password = password };
        using var engine = new TinyDbEngine(filePath, options);
    }

    /// <summary>
    /// 验证数据库密码
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <param name="password">待验证密码</param>
    /// <returns>验证是否成功</returns>
    public static bool VerifyPassword(string filePath, string password)
    {
        try
        {
            var options = new TinyDbOptions { Password = password };
            using var engine = new TinyDbEngine(filePath, options);
            return true;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
    }

    /// <summary>
    /// 更改数据库密码
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <param name="oldPassword">旧密码</param>
    /// <param name="newPassword">新密码</param>
    /// <returns>更改是否成功</returns>
    /// <exception cref="ArgumentException">新密码无效时抛出</exception>
    public static bool ChangePassword(string filePath, string oldPassword, string newPassword)
    {
        ValidatePassword(newPassword);

        using var engine = new TinyDbEngine(filePath, new TinyDbOptions { Password = oldPassword });
        return DatabaseSecurity.ChangePassword(engine, oldPassword, newPassword);
    }

    /// <summary>
    /// 移除数据库密码保护
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <param name="password">当前密码</param>
    /// <returns>移除是否成功</returns>
    public static bool RemovePassword(string filePath, string password)
    {
        using var engine = new TinyDbEngine(filePath, new TinyDbOptions { Password = password });
        return DatabaseSecurity.RemovePassword(engine, password);
    }

    /// <summary>
    /// 检查数据库是否受密码保护
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <returns>是否受保护</returns>
    public static bool IsPasswordProtected(string filePath)
    {
        if (DatabaseSecurity.HasSecurityMetadata(filePath))
        {
            return true;
        }

        try
        {
            using var engine = new TinyDbEngine(filePath);
            return DatabaseSecurity.IsDatabaseSecure(engine);
        }
        catch (UnauthorizedAccessException)
        {
            // 如果抛出密码验证异常，说明数据库受保护
            return true;
        }
    }

    /// <summary>
    /// 创建新的受密码保护数据库
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <param name="password">数据库密码</param>
    /// <param name="options">额外选项</param>
    /// <returns>数据库引擎实例</returns>
    /// <exception cref="ArgumentException">密码无效时抛出</exception>
    public static TinyDbEngine CreateSecureDatabase(string filePath, string password, TinyDbOptions? options = null)
    {
        ValidatePassword(password);

        // 如果文件已存在，先删除
        if (System.IO.File.Exists(filePath))
        {
            System.IO.File.Delete(filePath);
        }

        var dbOptions = options ?? new TinyDbOptions();
        dbOptions.Password = password;

        return new TinyDbEngine(filePath, dbOptions);
    }

    /// <summary>
    /// 打开受密码保护的数据库
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <param name="password">数据库密码</param>
    /// <param name="options">额外选项</param>
    /// <returns>数据库引擎实例</returns>
    /// <exception cref="UnauthorizedAccessException">密码验证失败时抛出</exception>
    /// <exception cref="FileNotFoundException">数据库文件不存在时抛出</exception>
    public static TinyDbEngine OpenSecureDatabase(string filePath, string password, TinyDbOptions? options = null)
    {
        if (!System.IO.File.Exists(filePath))
            throw new FileNotFoundException("数据库文件不存在", filePath);

        var dbOptions = options ?? new TinyDbOptions();
        dbOptions.Password = password;

        return new TinyDbEngine(filePath, dbOptions);
    }

    /// <summary>
    /// 打开数据库（自动检测密码保护）
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <param name="password">数据库密码（可选）</param>
    /// <param name="options">额外选项</param>
    /// <returns>数据库引擎实例</returns>
    /// <exception cref="FileNotFoundException">数据库文件不存在时抛出</exception>
    /// <exception cref="UnauthorizedAccessException">需要密码但未提供或密码错误时抛出</exception>
    public static TinyDbEngine OpenDatabase(string filePath, string? password = null, TinyDbOptions? options = null)
    {
        if (!System.IO.File.Exists(filePath))
            throw new FileNotFoundException("数据库文件不存在", filePath);

        var dbOptions = options ?? new TinyDbOptions();

        // 检查是否需要密码
        if (IsPasswordProtected(filePath))
        {
            if (string.IsNullOrWhiteSpace(password))
                throw new UnauthorizedAccessException("数据库受密码保护，请提供正确密码");

            dbOptions.Password = password;
        }

        return new TinyDbEngine(filePath, dbOptions);
    }

    /// <summary>
    /// 生成强密码建议
    /// </summary>
    /// <param name="length">密码长度，默认12位</param>
    /// <param name="includeSpecialChars">是否包含特殊字符</param>
    /// <returns>生成的密码</returns>
    public static string GenerateStrongPassword(int length = 12, bool includeSpecialChars = true)
    {
        if (length < 4)
            throw new ArgumentException("密码长度至少4位", nameof(length));

        const string lowerChars = "abcdefghijklmnopqrstuvwxyz";
        const string upperChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        const string numbers = "0123456789";
        const string specialChars = "!@#$%^&*()_+-=[]{}|;:,.<>?";

        var chars = lowerChars + upperChars + numbers;
        if (includeSpecialChars)
            chars += specialChars;

        var random = new Random();
        var password = new char[length];

        // 确保包含各种字符类型
        password[0] = lowerChars[random.Next(lowerChars.Length)];
        password[1] = upperChars[random.Next(upperChars.Length)];
        password[2] = numbers[random.Next(numbers.Length)];
        if (includeSpecialChars && length > 3)
        {
            password[3] = specialChars[random.Next(specialChars.Length)];
        }

        // 填充剩余位置
        for (int i = includeSpecialChars ? 4 : 3; i < length; i++)
        {
            password[i] = chars[random.Next(chars.Length)];
        }

        // 打乱字符顺序
        for (int i = 0; i < length; i++)
        {
            int j = random.Next(length);
            (password[i], password[j]) = (password[j], password[i]);
        }

        return new string(password);
    }

    /// <summary>
    /// 验证密码强度
    /// </summary>
    /// <param name="password">待验证密码</param>
    /// <returns>密码强度等级</returns>
    public static PasswordStrength CheckPasswordStrength(string password)
    {
        if (string.IsNullOrEmpty(password))
            return PasswordStrength.Weak;

        int score = 0;

        // 长度评分
        if (password.Length >= 8) score++;
        if (password.Length >= 12) score++;
        if (password.Length >= 16) score++;

        // 字符类型评分
        if (password.Any(char.IsLower)) score++;
        if (password.Any(char.IsUpper)) score++;
        if (password.Any(char.IsDigit)) score++;
        if (password.Any(c => !char.IsLetterOrDigit(c))) score++;

        return score switch
        {
            <= 2 => PasswordStrength.Weak,
            3 or 4 => PasswordStrength.Medium,
            5 or 6 => PasswordStrength.Strong,
            _ => PasswordStrength.VeryStrong
        };
    }

    /// <summary>
    /// 验证密码有效性
    /// </summary>
    /// <param name="password">待验证密码</param>
    /// <exception cref="ArgumentException">密码无效时抛出</exception>
    private static void ValidatePassword(string password)
    {
        if (string.IsNullOrWhiteSpace(password))
            throw new ArgumentException("密码不能为空", nameof(password));

        if (password.Length < 4)
            throw new ArgumentException("密码长度至少4位", nameof(password));
    }
}
