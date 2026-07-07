using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Metadata;
using System.Collections.Concurrent;

namespace TinyDb.Core;

public sealed partial class TransactionManager
{

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
            var aggregate = new AggregateException("Transaction apply failed and compensation rollback encountered errors.", allErrors);
            _engine.MarkCorrupted(aggregate);
            throw aggregate;
        }
    }


    private List<Exception> RollbackDurabilityScope(WriteAheadLog.WalTransactionScope durabilityScope)
    {
        var rollbackErrors = new List<Exception>();
        try
        {
            _engine.RollbackTransactionDurabilityScope(durabilityScope);
        }
        catch (Exception rollbackEx)
        {
            rollbackErrors.Add(new InvalidOperationException(
                "Transaction durability rollback failed while restoring WAL before-images.",
                rollbackEx));
        }
        finally
        {
            durabilityScope.Dispose();
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
                    if (operation.IndexFields != null && !string.IsNullOrEmpty(operation.IndexName))
                    {
                        var created = _engine.EnsureIndex(operation.CollectionName, operation.IndexFields, operation.IndexName, operation.IndexUnique, operation.IndexSparse);
                        operation.MarkIndexCreated(created);
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
            transaction.ClearOperations();
            transaction.Savepoints.Clear();
        }
    }


    /// <summary>
    /// 回滚单个操作
    /// </summary>
    /// <param name="operation">操作</param>
    internal void RollbackSingleOperation(TransactionOperation operation)
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
                    if (operation.WasIndexCreated && !string.IsNullOrEmpty(operation.IndexName))
                        _engine.GetIndexManager(operation.CollectionName).DropIndex(operation.IndexName);
                    break;

                case TransactionOperationType.DropIndex:
                    if (!string.IsNullOrEmpty(operation.IndexName) && operation.IndexFields != null)
                        _engine.EnsureIndex(operation.CollectionName, operation.IndexFields, operation.IndexName, operation.IndexUnique, operation.IndexSparse);
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

}
