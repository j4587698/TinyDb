using TinyDb.Bson;

namespace TinyDb.Core;

/// <summary>
/// 事务实现
/// </summary>
internal sealed class Transaction : ITransaction
{
    private readonly TransactionManager _manager;
    private readonly List<TransactionOperation> _operations;
    private readonly Dictionary<Guid, TransactionSavepoint> _savepoints;
    private bool _disposed;

    /// <summary>
    /// 事务ID
    /// </summary>
    public Guid TransactionId { get; }

    /// <summary>
    /// 事务状态
    /// </summary>
    public TransactionState State { get; internal set; }

    /// <summary>
    /// 开始时间
    /// </summary>
    public DateTime StartTime { get; }

    /// <summary>
    /// 操作列表
    /// </summary>
    internal List<TransactionOperation> Operations => _operations;

    /// <summary>
    /// 保存点列表
    /// </summary>
    internal Dictionary<Guid, TransactionSavepoint> Savepoints => _savepoints;

    /// <summary>
    /// 初始化事务
    /// </summary>
    /// <param name="manager">事务管理器</param>
    internal Transaction(TransactionManager manager)
    {
        _manager = manager ?? throw new ArgumentNullException(nameof(manager));
        TransactionId = Guid.NewGuid();
        State = TransactionState.Active;
        StartTime = DateTime.UtcNow;
        _operations = new List<TransactionOperation>();
        _savepoints = new Dictionary<Guid, TransactionSavepoint>();
    }

    /// <summary>
    /// 提交事务
    /// </summary>
    public void Commit()
    {
        ThrowIfDisposed();
        if (State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Transaction cannot be committed in state {State}");
        }

        try
        {
            _manager.CommitTransaction(this);
        }
        finally
        {
            // 清除事务上下文
            if (_manager._engine != null)
            {
                _manager._engine.ClearCurrentTransaction();
            }
        }
    }

    /// <summary>
    /// 回滚事务
    /// </summary>
    public void Rollback()
    {
        ThrowIfDisposed();

        // 允许在Active和Failed状态下回滚
        if (State != TransactionState.Active && State != TransactionState.Failed)
        {
            throw new InvalidOperationException($"Transaction cannot be rolled back in state {State}");
        }

        try
        {
            // 如果事务已经失败，先重置状态为Active以便回滚
            if (State == TransactionState.Failed)
            {
                State = TransactionState.Active;
            }

            _manager.RollbackTransaction(this);
        }
        finally
        {
            // 清除事务上下文
            if (_manager._engine != null)
            {
                _manager._engine.ClearCurrentTransaction();
            }
        }
    }

    /// <summary>
    /// 创建保存点
    /// </summary>
    /// <param name="name">保存点名称</param>
    /// <returns>保存点ID</returns>
    public Guid CreateSavepoint(string name)
    {
        ThrowIfDisposed();
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Savepoint name cannot be null or empty", nameof(name));
        if (State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Cannot create savepoint in transaction state {State}");
        }

        return _manager.CreateSavepoint(this, name);
    }

    /// <summary>
    /// 回滚到保存点
    /// </summary>
    /// <param name="savepointId">保存点ID</param>
    public void RollbackToSavepoint(Guid savepointId)
    {
        ThrowIfDisposed();
        if (State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Cannot rollback to savepoint in transaction state {State}");
        }

        _manager.RollbackToSavepoint(this, savepointId);
    }

    /// <summary>
    /// 释放保存点
    /// </summary>
    /// <param name="savepointId">保存点ID</param>
    public void ReleaseSavepoint(Guid savepointId)
    {
        ThrowIfDisposed();
        if (State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Cannot release savepoint in transaction state {State}");
        }

        _manager.ReleaseSavepoint(this, savepointId);
    }

    /// <summary>
    /// 记录插入操作
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="document">文档</param>
    /// <returns>文档ID</returns>
    internal BsonValue RecordInsert(string collectionName, BsonDocument document)
    {
        ThrowIfDisposed();
        if (State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Cannot record operation in transaction state {State}");
        }

        var documentId = document.TryGetValue("_id", out var id) ? id : ObjectId.NewObjectId();
        var operation = new TransactionOperation(
            TransactionOperationType.Insert,
            collectionName,
            documentId,
            null,
            document);

        // 通过管理器添加操作，避免重复添加
        _manager.RecordOperation(this, operation);
        return documentId;
    }

    /// <summary>
    /// 记录更新操作
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="originalDocument">原始文档</param>
    /// <param name="newDocument">新文档</param>
    internal void RecordUpdate(string collectionName, BsonDocument originalDocument, BsonDocument newDocument)
    {
        ThrowIfDisposed();
        if (State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Cannot record operation in transaction state {State}");
        }

        var documentId = newDocument.TryGetValue("_id", out var id) ? id : originalDocument["_id"];
        var operation = new TransactionOperation(
            TransactionOperationType.Update,
            collectionName,
            documentId,
            originalDocument,
            newDocument);

        // 通过管理器添加操作，避免重复添加
        _manager.RecordOperation(this, operation);
    }

    /// <summary>
    /// 记录删除操作
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="document">被删除的文档</param>
    internal void RecordDelete(string collectionName, BsonDocument document)
    {
        ThrowIfDisposed();
        if (State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Cannot record operation in transaction state {State}");
        }

        var documentId = document.TryGetValue("_id", out var id) ? id : ObjectId.NewObjectId();
        var operation = new TransactionOperation(
            TransactionOperationType.Delete,
            collectionName,
            documentId,
            document,
            null);

        // 通过管理器添加操作，避免重复添加
        _manager.RecordOperation(this, operation);
    }

    /// <summary>
    /// 记录创建索引操作
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="indexName">索引名称</param>
    /// <param name="indexFields">索引字段</param>
    /// <param name="unique">索引是否唯一</param>
    internal void RecordCreateIndex(string collectionName, string indexName, string[] indexFields, bool unique)
    {
        ThrowIfDisposed();
        if (State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Cannot record operation in transaction state {State}");
        }

        var operation = new TransactionOperation(
            TransactionOperationType.CreateIndex,
            collectionName,
            null,
            null,
            null,
            null,
            indexName,
            indexFields,
            unique);

        _manager.RecordOperation(this, operation);
    }

    /// <summary>
    /// 记录删除索引操作
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <param name="indexName">索引名称</param>
    internal void RecordDropIndex(string collectionName, string indexName)
    {
        ThrowIfDisposed();
        if (State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Cannot record operation in transaction state {State}");
        }

        // 为了回滚删除索引操作，我们需要知道被删除的索引的定义
        // 在 Record 阶段，我们查询现有索引信息并保存到 TransactionOperation 中
        
        string[]? indexFields = null;
        bool unique = false;
        
        // 尝试从元数据获取现有索引信息用于回滚
        try
        {
            if (!string.IsNullOrEmpty(indexName))
            {
                var indexManager = _manager._engine.GetIndexManager(collectionName);
                var index = indexManager.GetIndex(indexName);
                if (index != null)
                {
                    var stats = index.GetStatistics();
                    indexFields = stats.Fields;
                    unique = stats.IsUnique;
                }
            }
        }
        catch
        {
            // 忽略错误，如果无法获取信息，回滚可能受限
        }

        var operation = new TransactionOperation(
            TransactionOperationType.DropIndex,
            collectionName,
            null,
            null,
            null,
            null,
            indexName,
            indexFields,
            unique);

        _manager.RecordOperation(this, operation);
    }

    /// <summary>
    /// 获取事务统计信息
    /// </summary>
    /// <returns>统计信息</returns>
    public TransactionStatistics GetStatistics()
    {
        ThrowIfDisposed();

        var duration = DateTime.UtcNow - StartTime;
        var operationCounts = _operations.GroupBy(op => op.OperationType)
            .ToDictionary(g => g.Key, g => g.Count());

        return new TransactionStatistics
        {
            TransactionId = TransactionId,
            State = State,
            StartTime = StartTime,
            Duration = duration,
            OperationCount = _operations.Count,
            SavepointCount = _savepoints.Count,
            OperationCounts = operationCounts,
            IsReadOnly = _operations.Count == 0
        };
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(Transaction));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            try
            {
                // 如果事务仍然活动，自动回滚
                if (State == TransactionState.Active)
                {
                    try
                    {
                        Rollback();
                    }
                    catch
                    {
                        // 忽略回滚错误
                    }
                }

                _operations.Clear();
                _savepoints.Clear();
            }
            catch
            {
            }
            finally
            {
                _disposed = true;
            }
        }
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"Transaction[{TransactionId:N}]: {State}, {_operations.Count} operations, {_savepoints.Count} savepoints";
    }
}

/// <summary>
/// 事务统计信息
/// </summary>
public sealed class TransactionStatistics
{
    /// <summary>
    /// 事务ID
    /// </summary>
    public Guid TransactionId { get; init; }

    /// <summary>
    /// 事务状态
    /// </summary>
    public TransactionState State { get; init; }

    /// <summary>
    /// 开始时间
    /// </summary>
    public DateTime StartTime { get; init; }

    /// <summary>
    /// 持续时间
    /// </summary>
    public TimeSpan Duration { get; init; }

    /// <summary>
    /// 操作数量
    /// </summary>
    public int OperationCount { get; init; }

    /// <summary>
    /// 保存点数量
    /// </summary>
    public int SavepointCount { get; init; }

    /// <summary>
    /// 操作类型统计
    /// </summary>
    public Dictionary<TransactionOperationType, int> OperationCounts { get; init; } = new();

    /// <summary>
    /// 是否只读事务
    /// </summary>
    public bool IsReadOnly { get; init; }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"Transaction[{TransactionId:N}]: {State}, {OperationCount} ops, {Duration.TotalSeconds:F1}s, " +
               $"{(IsReadOnly ? "read-only" : "read-write")}";
    }
}
