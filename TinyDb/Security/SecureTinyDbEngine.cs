using TinyDb.Core;
using TinyDb.Collections;

namespace TinyDb.Security;

/// <summary>
/// 安全的TinyDb引擎包装器，提供密码保护功能
/// </summary>
public sealed class SecureTinyDbEngine : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string? _password;
    private bool _isAuthenticated;
    private bool _disposed;

    /// <summary>
    /// 内部引擎实例
    /// </summary>
    public TinyDbEngine Engine => _engine;

    /// <summary>
    /// 是否已通过身份验证
    /// </summary>
    public bool IsAuthenticated => _isAuthenticated;

    /// <summary>
    /// 数据库文件路径
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// 创建受密码保护的数据库引擎
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <param name="password">数据库密码</param>
    /// <param name="createIfNotExists">如果数据库不存在是否创建</param>
    /// <exception cref="ArgumentException">密码为空或过短时抛出</exception>
    /// <exception cref="UnauthorizedAccessException">密码验证失败时抛出</exception>
    public SecureTinyDbEngine(string filePath, string password, bool createIfNotExists = false)
    {
        if (string.IsNullOrWhiteSpace(password))
            throw new ArgumentException("密码不能为空", nameof(password));

        if (password.Length < 4)
            throw new ArgumentException("密码长度至少4位", nameof(password));

        FilePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        _password = password;

        try
        {
            _engine = new TinyDbEngine(filePath);

            // 检查数据库是否已存在并受保护
            if (DatabaseSecurity.IsDatabaseSecure(_engine))
            {
                // 验证密码
                _isAuthenticated = DatabaseSecurity.AuthenticateDatabase(_engine, password);
                if (!_isAuthenticated)
                {
                    _engine.Dispose();
                    throw new UnauthorizedAccessException("数据库密码验证失败，无法访问数据库");
                }
            }
            else if (createIfNotExists)
            {
                // 创建新的受保护数据库
                DatabaseSecurity.CreateSecureDatabase(_engine, password);
                _isAuthenticated = true;
            }
            else
            {
                // 数据库未受保护，但提供了密码
                _engine.Dispose();
                throw new InvalidOperationException("指定数据库未设置密码保护");
            }
        }
        catch
        {
            _engine?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// 创建不受密码保护的数据库引擎（向后兼容）
    /// </summary>
    /// <param name="filePath">数据库文件路径</param>
    /// <exception cref="UnauthorizedAccessException">数据库受密码保护但未提供密码时抛出</exception>
    public SecureTinyDbEngine(string filePath)
    {
        FilePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        _password = null;

        try
        {
            _engine = new TinyDbEngine(filePath);

            // 检查数据库是否受保护
            if (DatabaseSecurity.IsDatabaseSecure(_engine))
            {
                _engine.Dispose();
                throw new UnauthorizedAccessException("数据库受密码保护，请提供正确密码");
            }

            _isAuthenticated = true;
        }
        catch
        {
            _engine?.Dispose();
            throw;
        }
    }

    /// <summary>
    /// 设置数据库密码（仅对未受保护的数据库）
    /// </summary>
    /// <param name="password">要设置的密码</param>
    /// <exception cref="InvalidOperationException">数据库已受保护时抛出</exception>
    public void SetPassword(string password)
    {
        ThrowIfDisposed();
        ThrowIfNotAuthenticated();

        if (DatabaseSecurity.IsDatabaseSecure(_engine))
            throw new InvalidOperationException("数据库已设置密码保护");

        DatabaseSecurity.CreateSecureDatabase(_engine, password);
        _isAuthenticated = true;
    }

    /// <summary>
    /// 更改数据库密码
    /// </summary>
    /// <param name="oldPassword">旧密码</param>
    /// <param name="newPassword">新密码</param>
    /// <returns>更改是否成功</returns>
    public bool ChangePassword(string oldPassword, string newPassword)
    {
        ThrowIfDisposed();
        ThrowIfNotAuthenticated();

        return DatabaseSecurity.ChangePassword(_engine, oldPassword, newPassword);
    }

    /// <summary>
    /// 移除数据库密码保护
    /// </summary>
    /// <param name="password">当前密码</param>
    /// <returns>移除是否成功</returns>
    public bool RemovePassword(string password)
    {
        ThrowIfDisposed();
        ThrowIfNotAuthenticated();

        return DatabaseSecurity.RemovePassword(_engine, password);
    }

    /// <summary>
    /// 检查数据库是否受密码保护
    /// </summary>
    /// <returns>是否受保护</returns>
    public bool IsPasswordProtected()
    {
        ThrowIfDisposed();
        return DatabaseSecurity.IsDatabaseSecure(_engine);
    }

    /// <summary>
    /// 获取集合（需要身份验证）
    /// </summary>
    /// <typeparam name="T">实体类型</typeparam>
    /// <param name="name">集合名称</param>
    /// <returns>集合实例</returns>
    /// <exception cref="UnauthorizedAccessException">未通过身份验证时抛出</exception>
    public ILiteCollection<T> GetCollection<T>(string? name = null) where T : class, new()
    {
        ThrowIfDisposed();
        ThrowIfNotAuthenticated();
        return _engine.GetCollection<T>(name);
    }

    /// <summary>
    /// 获取集合名称列表（需要身份验证）
    /// </summary>
    /// <returns>集合名称列表</returns>
    /// <exception cref="UnauthorizedAccessException">未通过身份验证时抛出</exception>
    public IEnumerable<string> GetCollectionNames()
    {
        ThrowIfDisposed();
        ThrowIfNotAuthenticated();
        return _engine.GetCollectionNames();
    }

    /// <summary>
    /// 检查集合是否存在（需要身份验证）
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <returns>是否存在</returns>
    /// <exception cref="UnauthorizedAccessException">未通过身份验证时抛出</exception>
    public bool CollectionExists(string collectionName)
    {
        ThrowIfDisposed();
        ThrowIfNotAuthenticated();
        return _engine.CollectionExists(collectionName);
    }

    /// <summary>
    /// 删除集合（需要身份验证）
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <exception cref="UnauthorizedAccessException">未通过身份验证时抛出</exception>
    public void DropCollection(string collectionName)
    {
        ThrowIfDisposed();
        ThrowIfNotAuthenticated();
        _engine.DropCollection(collectionName);
    }

    /// <summary>
    /// 获取数据库统计信息（需要身份验证）
    /// </summary>
    /// <returns>统计信息</returns>
    /// <exception cref="UnauthorizedAccessException">未通过身份验证时抛出</exception>
    public string GetDatabaseStats()
    {
        ThrowIfDisposed();
        ThrowIfNotAuthenticated();
        return _engine.ToString();
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _engine?.Dispose();
            _disposed = true;
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(SecureTinyDbEngine));
    }

    private void ThrowIfNotAuthenticated()
    {
        if (!_isAuthenticated)
            throw new UnauthorizedAccessException("数据库身份验证失败，无法执行操作");
    }
}