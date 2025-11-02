namespace TinyDb.Security;

/// <summary>
/// TinyDb安全相关异常基类
/// </summary>
public abstract class TinyDbSecurityException : Exception
{
    protected TinyDbSecurityException(string message) : base(message) { }
    protected TinyDbSecurityException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// 数据库未受密码保护异常
/// </summary>
public class DatabaseNotProtectedException : TinyDbSecurityException
{
    public DatabaseNotProtectedException() : base("数据库未设置密码保护") { }
    public DatabaseNotProtectedException(string message) : base(message) { }
    public DatabaseNotProtectedException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// 数据库已受密码保护异常
/// </summary>
public class DatabaseAlreadyProtectedException : TinyDbSecurityException
{
    public DatabaseAlreadyProtectedException() : base("数据库已设置密码保护") { }
    public DatabaseAlreadyProtectedException(string message) : base(message) { }
    public DatabaseAlreadyProtectedException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// 密码验证失败异常
/// </summary>
public class PasswordVerificationException : TinyDbSecurityException
{
    public PasswordVerificationException() : base("密码验证失败") { }
    public PasswordVerificationException(string message) : base(message) { }
    public PasswordVerificationException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// 密码强度不足异常
/// </summary>
public class WeakPasswordException : TinyDbSecurityException
{
    public WeakPasswordException() : base("密码强度不足，不满足安全要求") { }
    public WeakPasswordException(string message) : base(message) { }
    public WeakPasswordException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// 安全配置损坏异常
/// </summary>
public class SecurityCorruptedException : TinyDbSecurityException
{
    public SecurityCorruptedException() : base("数据库安全配置损坏或被篡改") { }
    public SecurityCorruptedException(string message) : base(message) { }
    public SecurityCorruptedException(string message, Exception innerException) : base(message, innerException) { }
}