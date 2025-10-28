namespace SimpleDb.Core;

/// <summary>
/// SimpleDb 数据库配置选项
/// </summary>
public sealed class SimpleDbOptions
{
    /// <summary>
    /// 默认页面大小
    /// </summary>
    public const uint DefaultPageSize = 8192;

    /// <summary>
    /// 默认缓存大小
    /// </summary>
    public const int DefaultCacheSize = 1000;

    /// <summary>
    /// 默认超时时间
    /// </summary>
    public static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(30);

    /// <summary>
    /// 页面大小（字节），必须 >= 4096 且为 2 的幂
    /// </summary>
    public uint PageSize { get; set; } = DefaultPageSize;

    /// <summary>
    /// 页面缓存大小
    /// </summary>
    public int CacheSize { get; set; } = DefaultCacheSize;

    /// <summary>
    /// 是否启用日志（WAL）
    /// </summary>
    public bool EnableJournaling { get; set; } = true;

    /// <summary>
    /// 是否启用自动检查点
    /// </summary>
    public bool EnableAutoCheckpoint { get; set; } = true;

    /// <summary>
    /// 操作超时时间
    /// </summary>
    public TimeSpan Timeout { get; set; } = DefaultTimeout;

    /// <summary>
    /// 是否启用只读模式
    /// </summary>
    public bool ReadOnly { get; set; } = false;

    /// <summary>
    /// 是否启用严格模式
    /// </summary>
    public bool StrictMode { get; set; } = true;

    /// <summary>
    /// 数据库名称
    /// </summary>
    public string DatabaseName { get; set; } = "SimpleDb";

    /// <summary>
    /// 用户自定义数据（64字节）
    /// </summary>
    public byte[] UserData { get; set; } = Array.Empty<byte>();

    /// <summary>
    /// 是否启用压缩
    /// </summary>
    public bool EnableCompression { get; set; } = false;

    /// <summary>
    /// 是否启用加密
    /// </summary>
    public bool EnableEncryption { get; set; } = false;

    /// <summary>
    /// 加密密钥（启用加密时使用）
    /// </summary>
    public byte[]? EncryptionKey { get; set; }

    /// <summary>
    /// 最大事务大小（文档数量）
    /// </summary>
    public int MaxTransactionSize { get; set; } = 10000;

    /// <summary>
    /// 最大并发事务数量
    /// </summary>
    public int MaxTransactions { get; set; } = 100;

    /// <summary>
    /// 事务超时时间
    /// </summary>
    public TimeSpan TransactionTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// 是否启用同步写入
    /// </summary>
    public bool SynchronousWrites { get; set; } = true;

    /// <summary>
    /// 验证选项并抛出异常（如果有问题）
    /// </summary>
    public void Validate()
    {
        // 验证页面大小
        if (PageSize < 4096)
            throw new ArgumentException("Page size must be at least 4096 bytes", nameof(PageSize));

        if (!IsPowerOfTwo(PageSize))
            throw new ArgumentException("Page size must be a power of 2", nameof(PageSize));

        // 验证缓存大小
        if (CacheSize <= 0)
            throw new ArgumentException("Cache size must be positive", nameof(CacheSize));

        // 验证超时时间
        if (Timeout <= TimeSpan.Zero)
            throw new ArgumentException("Timeout must be positive", nameof(Timeout));

        // 验证数据库名称
        if (string.IsNullOrWhiteSpace(DatabaseName))
            throw new ArgumentException("Database name cannot be empty", nameof(DatabaseName));

        if (System.Text.Encoding.UTF8.GetByteCount(DatabaseName) > 63)
            throw new ArgumentException("Database name is too long (max 63 bytes)", nameof(DatabaseName));

        // 验证用户数据
        if (UserData != null && UserData.Length > 64)
            throw new ArgumentException("User data is too long (max 64 bytes)", nameof(UserData));

        // 验证加密配置
        if (EnableEncryption)
        {
            if (EncryptionKey == null || EncryptionKey.Length == 0)
                throw new ArgumentException("Encryption key is required when encryption is enabled", nameof(EncryptionKey));

            if (EncryptionKey.Length < 16)
                throw new ArgumentException("Encryption key must be at least 16 bytes", nameof(EncryptionKey));
        }

        // 验证事务大小
        if (MaxTransactionSize <= 0)
            throw new ArgumentException("Max transaction size must be positive", nameof(MaxTransactionSize));

        // 验证事务配置
        if (MaxTransactions <= 0)
            throw new ArgumentException("Max transactions must be positive", nameof(MaxTransactions));

        if (TransactionTimeout <= TimeSpan.Zero)
            throw new ArgumentException("Transaction timeout must be positive", nameof(TransactionTimeout));
    }

    /// <summary>
    /// 检查数字是否为2的幂
    /// </summary>
    /// <param name="value">要检查的数字</param>
    /// <returns>是否为2的幂</returns>
    private static bool IsPowerOfTwo(uint value)
    {
        return value != 0 && (value & (value - 1)) == 0;
    }

    /// <summary>
    /// 克隆选项
    /// </summary>
    /// <returns>新的选项实例</returns>
    public SimpleDbOptions Clone()
    {
        return new SimpleDbOptions
        {
            PageSize = PageSize,
            CacheSize = CacheSize,
            EnableJournaling = EnableJournaling,
            EnableAutoCheckpoint = EnableAutoCheckpoint,
            Timeout = Timeout,
            ReadOnly = ReadOnly,
            StrictMode = StrictMode,
            DatabaseName = DatabaseName,
            UserData = UserData?.ToArray() ?? Array.Empty<byte>(),
            EnableCompression = EnableCompression,
            EnableEncryption = EnableEncryption,
            EncryptionKey = EncryptionKey?.ToArray(),
            MaxTransactionSize = MaxTransactionSize,
            MaxTransactions = MaxTransactions,
            TransactionTimeout = TransactionTimeout,
            SynchronousWrites = SynchronousWrites
        };
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"SimpleDbOptions: PageSize={PageSize}, CacheSize={CacheSize}, " +
               $"Journaling={EnableJournaling}, ReadOnly={ReadOnly}, Timeout={Timeout.TotalSeconds}s";
    }
}