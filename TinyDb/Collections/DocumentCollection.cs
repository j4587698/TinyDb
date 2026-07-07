using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;
using TinyDb.Query;
using TinyDb.Index;
using TinyDb.Attributes;
using TinyDb.IdGeneration;

namespace TinyDb.Collections;

/// <summary>
/// 文档集合实现
/// </summary>
/// <typeparam name="T">文档类型</typeparam>
public sealed partial class DocumentCollection<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : ITinyCollection<T>, IDocumentCollection where T : class
{
    private const int SqlWriteBatchSize = 1000;

    private readonly TinyDbEngine _engine;
    private readonly string _name;
    private readonly QueryExecutor _queryExecutor;
    private int _disposed;

    /// <summary>
    /// 集合名称
    /// </summary>
    public string CollectionName => _name;

    /// <summary>
    /// 集合名称（IDocumentCollection 接口）
    /// </summary>
    string IDocumentCollection.Name => _name;

    /// <summary>
    /// 文档类型
    /// </summary>
    Type IDocumentCollection.DocumentType => typeof(T);

    /// <summary>
    /// 初始化文档集合
    /// </summary>
    /// <param name="engine">数据库引擎</param>
    /// <param name="name">集合名称</param>
    public DocumentCollection(TinyDbEngine engine, string name)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _name = name ?? throw new ArgumentNullException(nameof(name));
        _queryExecutor = new QueryExecutor(engine);

        // 自动扫描并创建基于属性的索引
        CreateAutoIndexes();
    }

    /// <summary>
    /// 创建自动索引
    /// </summary>
    private void CreateAutoIndexes()
    {
        IndexScanner.ScanAndCreateIndexes(_engine, typeof(T), _name);
    }

    /// <summary>
    /// 获取数据库引擎实例
    /// </summary>
    /// <returns>数据库引擎实例</returns>
    public TinyDbEngine Database => _engine;

    /// <summary>
    /// 获取索引管理器
    /// </summary>
    /// <returns>索引管理器实例</returns>
    public IndexManager GetIndexManager()
    {
        ThrowIfDisposed();
        return _engine.GetIndexManager(_name);
    }

    private static void ValidatePaginationArguments(int skip, int limit)
    {
        if (skip < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(skip), "skip must be greater than or equal to 0.");
        }

        if (limit < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(limit), "limit must be greater than or equal to 0.");
        }
    }


    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(nameof(DocumentCollection<T>));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        Interlocked.Exchange(ref _disposed, 1);
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"DocumentCollection<{typeof(T).Name}>[{_name}]";
    }
}
