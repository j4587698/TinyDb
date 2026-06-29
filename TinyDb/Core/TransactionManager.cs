using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Metadata;
using System.Collections.Concurrent;

namespace TinyDb.Core;

/// <summary>
/// 事务管理器
/// </summary>
public sealed class TransactionManager : IDisposable
{
    internal readonly TinyDbEngine _engine;
    private readonly Dictionary<Guid, Transaction> _activeTransactions;
    private readonly ConcurrentDictionary<string, List<ForeignKeyDefinition>> _foreignKeyCache = new(StringComparer.Ordinal);
    private readonly object _foreignKeyCacheLock = new();
    private readonly object _lock = new();
    private int _disposed;

    private record ForeignKeyDefinition(string FieldName, string ReferencedCollection);

    /// <summary>
    /// 活动事务数量
    /// </summary>
    public int ActiveTransactionCount
    {
        get
        {
            lock (_lock)
            {
                return _activeTransactions.Count;
            }
        }
    }

    /// <summary>
    /// 最大事务数量
    /// </summary>
    public int MaxTransactions { get; }

    /// <summary>
    /// 事务超时时间
    /// </summary>
    public TimeSpan TransactionTimeout { get; }

    /// <summary>
    /// 初始化事务管理器
    /// </summary>
    /// <param name="engine">数据库引擎</param>
    /// <param name="maxTransactions">最大事务数量</param>
    /// <param name="transactionTimeout">事务超时时间</param>
    public TransactionManager(TinyDbEngine engine, int maxTransactions = 100, TimeSpan? transactionTimeout = null)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        MaxTransactions = maxTransactions;
        TransactionTimeout = transactionTimeout ?? TimeSpan.FromMinutes(5);
        _activeTransactions = new Dictionary<Guid, Transaction>();

        // 启动超时检查任务
        _ = StartTimeoutCheckTask();
    }

    /// <summary>
    /// 开始新事务
    /// </summary>
    /// <returns>事务实例</returns>
    public ITransaction BeginTransaction()
    {
        ThrowIfDisposed();
        lock (_lock)
        {
            if (_activeTransactions.Count >= MaxTransactions)
            {
                throw new InvalidOperationException($"Maximum number of transactions ({MaxTransactions}) reached");
            }

            var transaction = new Transaction(this);
            _activeTransactions[transaction.TransactionId] = transaction;
            return transaction;
        }
    }

    /// <summary>
    /// 提交事务
    /// </summary>
    /// <param name="transaction">事务</param>
    internal void CommitTransaction(Transaction transaction)
    {
        ThrowIfDisposed();
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));

        TransactionOperation[] operations;
        lock (_lock)
        {
            if (!_activeTransactions.ContainsKey(transaction.TransactionId))
            {
                throw new InvalidOperationException("Transaction is not active");
            }

            lock (transaction.SyncRoot)
            {
                transaction.State = TransactionState.Committing;
                operations = transaction.Operations.ToArray();
            }
        }

        try
        {
            using var collectionLocks = _engine.EnterCollectionWriteLocks(
                operations.Select(static operation => operation.CollectionName));

            // 验证所有操作
            ValidateOperations(operations);

            // 应用所有更改到数据库
            var durabilityScope = _engine.BeginTransactionDurabilityScope(transaction.TransactionId);
            try
            {
                ApplyOperationsToDatabase(operations);

                try
                {
                    // 关键：在标记为 Committed 之前，确保所有变更已持久化到磁盘/WAL
                    durabilityScope.Commit();
                }
                catch (Exception commitEx)
                {
                    durabilityScope.Dispose();
                    durabilityScope = null;

                    var rollbackErrors = RollbackAppliedOperations(operations);
                    if (rollbackErrors.Count == 0)
                    {
                        throw new InvalidOperationException(
                            "Transaction durability commit failed after applying operations; applied operations were rolled back.",
                            commitEx);
                    }

                    var errors = new List<Exception>
                    {
                        new InvalidOperationException(
                            "Transaction durability commit failed after applying operations.",
                            commitEx)
                    };
                    errors.AddRange(rollbackErrors);
                    throw new AggregateException(
                        "Transaction durability commit failed and rollback compensation encountered errors.",
                        errors);
                }
            }
            finally
            {
                durabilityScope?.Dispose();
            }

            lock (_lock)
            {
                transaction.State = TransactionState.Committed;
            }
        }
        catch (Exception ex)
        {
            lock (_lock)
            {
                transaction.State = TransactionState.Failed;
            }

            throw new InvalidOperationException($"Failed to commit transaction: {ex.Message}", ex);
        }
        finally
        {
            lock (_lock)
            {
                _activeTransactions.Remove(transaction.TransactionId);
            }
        }
    }

    /// <summary>
    /// 回滚事务
    /// </summary>
    /// <param name="transaction">事务</param>
    internal void RollbackTransaction(Transaction transaction)
    {
        ThrowIfDisposed();
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));

        lock (_lock)
        {
            if (!_activeTransactions.ContainsKey(transaction.TransactionId))
            {
                throw new InvalidOperationException("Transaction is not active");
            }

            try
            {
                transaction.State = TransactionState.RollingBack;

                // 回滚所有操作
                RollbackOperations(transaction);

                transaction.State = TransactionState.RolledBack;
            }
            catch (Exception ex)
            {
                transaction.State = TransactionState.Failed;
                throw new InvalidOperationException($"Failed to rollback transaction: {ex.Message}", ex);
            }
            finally
            {
                _activeTransactions.Remove(transaction.TransactionId);
            }
        }
    }

    /// <summary>
    /// 创建保存点
    /// </summary>
    /// <param name="transaction">事务</param>
    /// <param name="name">保存点名称</param>
    /// <returns>保存点ID</returns>
    internal Guid CreateSavepoint(Transaction transaction, string name)
    {
        ThrowIfDisposed();
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));
        if (string.IsNullOrEmpty(name)) throw new ArgumentException("Savepoint name cannot be null or empty", nameof(name));

        lock (_lock)
        {
            if (!_activeTransactions.ContainsKey(transaction.TransactionId))
            {
                throw new InvalidOperationException("Transaction is not active");
            }

            var savepoint = new TransactionSavepoint(name, transaction.GetOperationsSnapshot().Length);
            transaction.Savepoints[savepoint.SavepointId] = savepoint;
            return savepoint.SavepointId;
        }
    }

    /// <summary>
    /// 回滚到保存点
    /// </summary>
    /// <param name="transaction">事务</param>
    /// <param name="savepointId">保存点ID</param>
    internal void RollbackToSavepoint(Transaction transaction, Guid savepointId)
    {
        ThrowIfDisposed();
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));

        lock (_lock)
        {
            if (!_activeTransactions.ContainsKey(transaction.TransactionId))
            {
                throw new InvalidOperationException("Transaction is not active");
            }

            if (!transaction.Savepoints.TryGetValue(savepointId, out var savepoint))
            {
                throw new ArgumentException("Savepoint not found", nameof(savepointId));
            }

            // 事务采用延迟提交模型：保存点回滚只需丢弃保存点之后的挂起操作，
            // 不应对数据库执行补偿写入。
            lock (transaction.SyncRoot)
            {
                transaction.Operations.RemoveRange(savepoint.OperationCount, transaction.Operations.Count - savepoint.OperationCount);
            }

            // 移除后续的保存点
            var savepointsToRemove = transaction.Savepoints
                .Where(kvp => kvp.Value.CreatedAt > savepoint.CreatedAt)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var id in savepointsToRemove)
            {
                transaction.Savepoints.Remove(id);
            }
        }
    }

    /// <summary>
    /// 释放保存点
    /// </summary>
    /// <param name="transaction">事务</param>
    /// <param name="savepointId">保存点ID</param>
    internal void ReleaseSavepoint(Transaction transaction, Guid savepointId)
    {
        ThrowIfDisposed();
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));

        lock (_lock)
        {
            if (!_activeTransactions.ContainsKey(transaction.TransactionId))
            {
                throw new InvalidOperationException("Transaction is not active");
            }

            transaction.Savepoints.Remove(savepointId);
        }
    }

    /// <summary>
    /// 记录操作
    /// </summary>
    /// <param name="transaction">事务</param>
    /// <param name="operation">操作</param>
    internal void RecordOperation(Transaction transaction, TransactionOperation operation)
    {
        ThrowIfDisposed();
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));
        if (operation == null) throw new ArgumentNullException(nameof(operation));

        lock (_lock)
        {
            if (!_activeTransactions.ContainsKey(transaction.TransactionId))
            {
                throw new InvalidOperationException("Transaction is not active");
            }

            lock (transaction.SyncRoot)
            {
                if (transaction.State != TransactionState.Active)
                {
                    throw new InvalidOperationException($"Transaction cannot record operations in state {transaction.State}");
                }

                transaction.Operations.Add(operation);
            }
        }
    }

    /// <summary>
    /// 验证操作
    /// </summary>
    /// <param name="operations">事务操作快照</param>
    private void ValidateOperations(IReadOnlyList<TransactionOperation> operations)
    {
        // 检查重复的文档ID插入（只检查非null ID的重复）
        var insertOperations = operations
            .Where(op => op.OperationType == TransactionOperationType.Insert && op.DocumentId != null)
            .GroupBy(op => new { op.CollectionName, op.DocumentId })
            .Where(group => group.Count() > 1)
            .SelectMany(group => group)
            .ToList();

        if (insertOperations.Count > 0)
        {
            throw new InvalidOperationException("Duplicate document IDs detected in transaction");
        }

        // 检查外键约束
        ValidateForeignKeys(operations);
        ValidateWriteConflicts(operations);
    }

    private void ValidateForeignKeys(IReadOnlyList<TransactionOperation> transactionOperations)
    {
        var operations = transactionOperations
            .Where(op => op.OperationType == TransactionOperationType.Insert || op.OperationType == TransactionOperationType.Update)
            .ToList();

        foreach (var op in operations)
        {
            if (op.NewDocument == null) continue;

            var foreignKeys = GetForeignKeyDefinitions(op.CollectionName);
            if (foreignKeys.Count == 0) continue;

            foreach (var fk in foreignKeys)
            {
                BsonValue? fkValue;
                bool found = op.NewDocument.TryGetValue(fk.FieldName, out fkValue) && !fkValue.IsNull;
                
                if (!found)
                {
                    // Try camelCase
                    var camelName = ToCamelCase(fk.FieldName);
                    if (camelName != fk.FieldName)
                    {
                        found = op.NewDocument.TryGetValue(camelName, out fkValue) && !fkValue.IsNull;
                    }
                }

                if (found)
                {
                    var referencedDoc = _engine.FindById(fk.ReferencedCollection, fkValue!);

                    if (referencedDoc == null)
                    {
                         throw new InvalidOperationException($"Foreign key constraint violation: Field '{fk.FieldName}' in collection '{op.CollectionName}' references non-existent document ID '{fkValue}' in collection '{fk.ReferencedCollection}'.");
                    }
                }
                else
                {
                    // FK field missing, skip validation (nullable FK)
                }
            }
        }
    }

    private void ValidateWriteConflicts(IReadOnlyList<TransactionOperation> operations)
    {
        var checkedDocuments = new HashSet<string>(StringComparer.Ordinal);

        foreach (var op in operations)
        {
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;
            if (op.OperationType is not (TransactionOperationType.Insert or TransactionOperationType.Update or TransactionOperationType.Delete)) continue;

            var key = $"{op.CollectionName}\0{op.DocumentId}";
            if (!checkedDocuments.Add(key)) continue;

            var committedDocument = _engine.FindCommittedById(op.CollectionName, op.DocumentId);

            if (op.OperationType == TransactionOperationType.Insert)
            {
                // 插入冲突由提交阶段的主键和唯一索引约束兜底。这里提前读取会和事务内可见性路径互相干扰，
                // 将未提交插入误判为已提交文档。
                continue;
            }

            if (op.OriginalDocument == null)
            {
                if (committedDocument != null)
                {
                    throw new InvalidOperationException($"Write conflict: document '{op.DocumentId}' changed in collection '{op.CollectionName}'.");
                }

                continue;
            }

            if (committedDocument == null || !committedDocument.Equals(op.OriginalDocument))
            {
                throw new InvalidOperationException($"Write conflict: document '{op.DocumentId}' changed in collection '{op.CollectionName}'.");
            }
        }
    }

    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();
        return char.ToLowerInvariant(name[0]) + name.Substring(1);
    }

    private List<ForeignKeyDefinition> GetForeignKeyDefinitions(string collectionName)
    {
        lock (_foreignKeyCacheLock)
        {
            return _foreignKeyCache.GetOrAdd(collectionName, name =>
            {
                var definitions = new List<ForeignKeyDefinition>();

                // 扫描所有元数据集合以找到匹配的集合名称
                var metadataCollectionNames = new List<string> { "__sys_catalog" };

                foreach (var metaColName in metadataCollectionNames)
                {
                    try
                    {
                        var metaCol = _engine.GetCollection<MetadataDocument>(metaColName);
                        var metaDoc = metaCol.FindById(name);

                        if (metaDoc != null)
                        {
                            var entityMeta = metaDoc.ToEntityMetadata();
                            foreach (var prop in entityMeta.Properties)
                            {
                                if (!string.IsNullOrEmpty(prop.ForeignKeyCollection))
                                {
                                    definitions.Add(new ForeignKeyDefinition(prop.PropertyName, prop.ForeignKeyCollection!));
                                }
                            }
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new InvalidOperationException(
                            $"Failed to read foreign key metadata for collection '{name}' from '{metaColName}'.", ex);
                    }
                }

                return definitions;
            });
        }
    }

    internal void ClearForeignKeyCache()
    {
        _foreignKeyCache.Clear();
    }

    /// <summary>
    /// 将操作应用到数据库
    /// </summary>
    /// <param name="transaction">事务</param>
    private void ApplyOperationsToDatabase(IReadOnlyList<TransactionOperation> operations)
    {
        int appliedCount = 0;
        try
        {
            for (int i = 0; i < operations.Count; i++)
            {
                ApplySingleOperation(operations[i]);
                appliedCount++;
            }
        }
        catch (Exception ex)
        {
            var compensationErrors = new List<Exception>();

            // 关键：当前操作可能已部分生效（例如写入页面后索引插入失败）。
            // 先尝试对“失败的那一条”进行补偿，避免残留半应用状态。
            try
            {
                if (appliedCount >= 0 && appliedCount < operations.Count)
                {
                    RollbackSingleOperation(operations[appliedCount]);
                }
            }
            catch (Exception compensationEx)
            {
                compensationErrors.Add(new InvalidOperationException(
                    "Rollback of failed operation during compensation failed.",
                    compensationEx));
            }

            // 如果应用过程中出错，回滚已经成功的操作
            for (int i = appliedCount - 1; i >= 0; i--)
            {
                try
                {
                    RollbackSingleOperation(operations[i]);
                }
                catch (Exception rollbackEx)
                {
                    compensationErrors.Add(new InvalidOperationException(
                        $"Rollback compensation failed for operation index {i}.",
                        rollbackEx));
                }
            }

            if (compensationErrors.Count == 0)
            {
                throw new InvalidOperationException("Transaction apply failed.", ex);
            }

            var allErrors = new List<Exception>
            {
                new InvalidOperationException("Transaction apply failed.", ex)
            };
            allErrors.AddRange(compensationErrors);
            throw new AggregateException("Transaction apply failed and compensation rollback encountered errors.", allErrors);
        }
    }

    private List<Exception> RollbackAppliedOperations(IReadOnlyList<TransactionOperation> operations)
    {
        var rollbackErrors = new List<Exception>();
        for (int i = operations.Count - 1; i >= 0; i--)
        {
            try
            {
                RollbackSingleOperation(operations[i]);
            }
            catch (Exception rollbackEx)
            {
                rollbackErrors.Add(new InvalidOperationException(
                    $"Rollback compensation failed for operation index {i}.",
                    rollbackEx));
            }
        }

        return rollbackErrors;
    }

    /// <summary>
    /// 应用单个操作
    /// </summary>
    /// <param name="operation">操作</param>
    private void ApplySingleOperation(TransactionOperation operation)
    {
        switch (operation.OperationType)
        {
            case TransactionOperationType.Insert:
                if (operation.NewDocument != null)
                {
                    _engine.InsertDocument(operation.CollectionName, operation.NewDocument);
                }
                break;

            case TransactionOperationType.Update:
                if (operation.NewDocument != null)
                {
                    _engine.UpdateDocument(operation.CollectionName, operation.NewDocument);
                }
                break;

            case TransactionOperationType.Delete:
                if (operation.DocumentId != null)
                {
                    _engine.DeleteDocument(operation.CollectionName, operation.DocumentId);
                }
                break;

            case TransactionOperationType.CreateIndex:
                // 事务性索引创建
                // 尝试创建索引，如果失败抛出异常导致事务回滚
                try 
                {
                    var indexManager = _engine.GetIndexManager(operation.CollectionName);
                    if (operation.IndexFields != null && !string.IsNullOrEmpty(operation.IndexName))
                    {
                        indexManager.CreateIndex(operation.IndexName, operation.IndexFields, operation.IndexUnique, operation.IndexSparse);
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Failed to apply CreateIndex operation: {ex.Message}", ex);
                }
                break;

            case TransactionOperationType.DropIndex:
                // 事务性索引删除
                try
                {
                    var indexManager = _engine.GetIndexManager(operation.CollectionName);
                    if (!string.IsNullOrEmpty(operation.IndexName))
                    {
                        indexManager.DropIndex(operation.IndexName);
                    }
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"Failed to apply DropIndex operation: {ex.Message}", ex);
                }
                break;

            default:
                throw new NotSupportedException($"Operation type {operation.OperationType} is not supported");
        }
    }

    /// <summary>
    /// 回滚操作
    /// </summary>
    /// <param name="transaction">事务</param>
    private void RollbackOperations(Transaction transaction)
    {
        // 事务采用延迟提交模型：显式回滚仅需丢弃挂起操作。
        // 真正的补偿回滚仅在 Commit 过程中部分应用失败时发生（见 ApplyOperationsToDatabase）。
        lock (transaction.SyncRoot)
        {
            transaction.Operations.Clear();
        }
        transaction.Savepoints.Clear();
    }

    /// <summary>
    /// 回滚单个操作
    /// </summary>
    /// <param name="operation">操作</param>
    private void RollbackSingleOperation(TransactionOperation operation)
    {
        // 如果在 Commit 过程中部分应用了操作后发生失败，需要对已经物理生效的操作进行补偿（撤销）。
        try
        {
            switch (operation.OperationType)
            {
                case TransactionOperationType.Insert:
                    if (operation.DocumentId != null)
                        _engine.DeleteDocument(operation.CollectionName, operation.DocumentId);
                    break;

                case TransactionOperationType.Update:
                    if (operation.OriginalDocument != null)
                        _engine.UpdateDocument(operation.CollectionName, operation.OriginalDocument);
                    break;

                case TransactionOperationType.Delete:
                    if (operation.OriginalDocument != null)
                        _engine.InsertDocument(operation.CollectionName, operation.OriginalDocument);
                    break;

                case TransactionOperationType.CreateIndex:
                    if (!string.IsNullOrEmpty(operation.IndexName))
                        _engine.GetIndexManager(operation.CollectionName).DropIndex(operation.IndexName);
                    break;

                case TransactionOperationType.DropIndex:
                    if (!string.IsNullOrEmpty(operation.IndexName) && operation.IndexFields != null)
                        _engine.GetIndexManager(operation.CollectionName).CreateIndex(operation.IndexName, operation.IndexFields, operation.IndexUnique, operation.IndexSparse);
                    break;

                default:
                    throw new NotSupportedException($"Operation type {operation.OperationType} is not supported during rollback compensation");
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"Transaction compensation failed for {operation.OperationType}.", ex);
        }
    }

    /// <summary>
    /// 启动超时检查任务
    /// </summary>
    /// <returns>任务</returns>
    private async Task StartTimeoutCheckTask()
    {
        while (Volatile.Read(ref _disposed) == 0)
        {
            await Task.Delay(TimeSpan.FromSeconds(30)); // 每30秒检查一次
            if (Volatile.Read(ref _disposed) != 0) break;
            CheckAndCleanupExpiredTransactions();
        }
    }

    /// <summary>
    /// 检查并清理过期事务
    /// </summary>
    private void CheckAndCleanupExpiredTransactions()
    {
        lock (_lock)
        {
            var expiredTransactions = _activeTransactions
                .Where(kvp => kvp.Value.State == TransactionState.Active &&
                              DateTime.UtcNow - kvp.Value.StartTime > TransactionTimeout)
                .Select(kvp => kvp.Value)
                .ToList();

            foreach (var expiredTransaction in expiredTransactions)
            {
                if (expiredTransaction.State != TransactionState.Active) continue;
                expiredTransaction.State = TransactionState.Failed;
                _activeTransactions.Remove(expiredTransaction.TransactionId);
            }
        }
    }

    /// <summary>
    /// 获取事务统计信息
    /// </summary>
    /// <returns>统计信息</returns>
    public TransactionManagerStatistics GetStatistics()
    {
        ThrowIfDisposed();

        lock (_lock)
        {
            var transactions = _activeTransactions.Values.ToList();
            var operationCounts = transactions.Select(t => t.GetOperationsSnapshot().Length).ToList();
            return new TransactionManagerStatistics
            {
                ActiveTransactionCount = transactions.Count,
                MaxTransactions = MaxTransactions,
                TransactionTimeout = TransactionTimeout,
                AverageOperationCount = operationCounts.Count > 0 ? operationCounts.Average() : 0,
                TotalOperations = operationCounts.Sum(),
                AverageTransactionAge = transactions.Count > 0 ?
                    transactions.Average(t => (DateTime.UtcNow - t.StartTime).TotalSeconds) : 0,
                States = transactions.GroupBy(t => t.State).ToDictionary(g => g.Key, g => g.Count())
            };
        }
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0)
            throw new ObjectDisposedException(nameof(TransactionManager));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        // 回滚所有活动事务
        lock (_lock)
        {
            foreach (var transaction in _activeTransactions.Values)
            {
                transaction.State = TransactionState.Failed;
            }

            _activeTransactions.Clear();
        }
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"TransactionManager: {ActiveTransactionCount}/{MaxTransactions} active, Timeout={TransactionTimeout.TotalMinutes:F1}min";
    }
}

/// <summary>
/// 事务管理器统计信息
/// </summary>
public sealed class TransactionManagerStatistics
{
    public int ActiveTransactionCount { get; init; }
    public int MaxTransactions { get; init; }
    public TimeSpan TransactionTimeout { get; init; }
    public double AverageOperationCount { get; init; }
    public int TotalOperations { get; init; }
    public double AverageTransactionAge { get; init; }
    public Dictionary<TransactionState, int> States { get; init; } = new();

    public override string ToString()
    {
        return $"TransactionManager: {ActiveTransactionCount}/{MaxTransactions} active, " +
               $"{TotalOperations} total operations, AvgAge={AverageTransactionAge:F1}s";
    }
}
