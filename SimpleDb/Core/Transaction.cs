using SimpleDb.Bson;

namespace SimpleDb.Core;

/// <summary>
/// 事务接口
/// </summary>
public interface ITransaction : IDisposable
{
    /// <summary>
    /// 事务ID
    /// </summary>
    Guid TransactionId { get; }

    /// <summary>
    /// 事务状态
    /// </summary>
    TransactionState State { get; }

    /// <summary>
    /// 开始时间
    /// </summary>
    DateTime StartTime { get; }

    /// <summary>
    /// 提交事务
    /// </summary>
    void Commit();

    /// <summary>
    /// 回滚事务
    /// </summary>
    void Rollback();

    /// <summary>
    /// 创建保存点
    /// </summary>
    /// <param name="name">保存点名称</param>
    /// <returns>保存点ID</returns>
    Guid CreateSavepoint(string name);

    /// <summary>
    /// 回滚到保存点
    /// </summary>
    /// <param name="savepointId">保存点ID</param>
    void RollbackToSavepoint(Guid savepointId);

    /// <summary>
    /// 释放保存点
    /// </summary>
    /// <param name="savepointId">保存点ID</param>
    void ReleaseSavepoint(Guid savepointId);
}

/// <summary>
/// 事务状态
/// </summary>
public enum TransactionState
{
    /// <summary>
    /// 活动状态
    /// </summary>
    Active,

    /// <summary>
    /// 正在提交
    /// </summary>
    Committing,

    /// <summary>
    /// 已提交
    /// </summary>
    Committed,

    /// <summary>
    /// 正在回滚
    /// </summary>
    RollingBack,

    /// <summary>
    /// 已回滚
    /// </summary>
    RolledBack,

    /// <summary>
    /// 已失败
    /// </summary>
    Failed
}

/// <summary>
/// 事务操作类型
/// </summary>
public enum TransactionOperationType
{
    /// <summary>
    /// 插入
    /// </summary>
    Insert,

    /// <summary>
    /// 更新
    /// </summary>
    Update,

    /// <summary>
    /// 删除
    /// </summary>
    Delete,

    /// <summary>
    /// 创建索引
    /// </summary>
    CreateIndex,

    /// <summary>
    /// 删除索引
    /// </summary>
    DropIndex
}

/// <summary>
/// 事务操作记录
/// </summary>
public sealed class TransactionOperation
{
    /// <summary>
    /// 操作ID
    /// </summary>
    public Guid OperationId { get; }

    /// <summary>
    /// 操作类型
    /// </summary>
    public TransactionOperationType OperationType { get; }

    /// <summary>
    /// 集合名称
    /// </summary>
    public string CollectionName { get; }

    /// <summary>
    /// 文档ID
    /// </summary>
    public BsonValue? DocumentId { get; }

    /// <summary>
    /// 原始文档（用于更新回滚）
    /// </summary>
    public BsonDocument? OriginalDocument { get; }

    /// <summary>
    /// 新文档（用于插入回滚）
    /// </summary>
    public BsonDocument? NewDocument { get; }

    /// <summary>
    /// 操作时间
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    /// 保存点ID
    /// </summary>
    public Guid? SavepointId { get; }

    /// <summary>
    /// 初始化事务操作
    /// </summary>
    /// <param name="operationType">操作类型</param>
    /// <param name="collectionName">集合名称</param>
    /// <param name="documentId">文档ID</param>
    /// <param name="originalDocument">原始文档</param>
    /// <param name="newDocument">新文档</param>
    /// <param name="savepointId">保存点ID</param>
    public TransactionOperation(
        TransactionOperationType operationType,
        string collectionName,
        BsonValue? documentId = null,
        BsonDocument? originalDocument = null,
        BsonDocument? newDocument = null,
        Guid? savepointId = null)
    {
        OperationId = Guid.NewGuid();
        OperationType = operationType;
        CollectionName = collectionName;
        DocumentId = documentId;
        OriginalDocument = originalDocument;
        NewDocument = newDocument;
        Timestamp = DateTime.UtcNow;
        SavepointId = savepointId;
    }

    /// <summary>
    /// 克隆操作
    /// </summary>
    /// <returns>新的操作实例</returns>
    public TransactionOperation Clone()
    {
        return new TransactionOperation(
            OperationType,
            CollectionName,
            DocumentId,
            OriginalDocument?.Clone() as BsonDocument,
            NewDocument?.Clone() as BsonDocument,
            SavepointId
        );
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"Transaction[{OperationId:N}]: {OperationType} on {CollectionName} at {Timestamp:HH:mm:ss.fff}";
    }
}

/// <summary>
/// 事务保存点
/// </summary>
public sealed class TransactionSavepoint
{
    /// <summary>
    /// 保存点ID
    /// </summary>
    public Guid SavepointId { get; }

    /// <summary>
    /// 保存点名称
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// 创建时间
    /// </summary>
    public DateTime CreatedAt { get; }

    /// <summary>
    /// 操作数量（创建时的操作数量）
    /// </summary>
    public int OperationCount { get; }

    /// <summary>
    /// 初始化保存点
    /// </summary>
    /// <param name="name">保存点名称</param>
    /// <param name="operationCount">操作数量</param>
    public TransactionSavepoint(string name, int operationCount)
    {
        SavepointId = Guid.NewGuid();
        Name = name;
        CreatedAt = DateTime.UtcNow;
        OperationCount = operationCount;
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"Savepoint[{Name}]: {SavepointId:N} at {CreatedAt:HH:mm:ss.fff} ({OperationCount} operations)";
    }
}