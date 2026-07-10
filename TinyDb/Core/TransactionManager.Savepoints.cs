using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Metadata;
using System.Collections.Concurrent;

namespace TinyDb.Core;

public sealed partial class TransactionManager
{

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
            EnsureTransactionActive(transaction);

            try
            {
                var state = transaction.State;
                if (state != TransactionState.Active && state != TransactionState.Failed)
                {
                    throw new InvalidOperationException($"Transaction cannot be rolled back in state {state}");
                }

                if (!transaction.TryTransitionState(state, TransactionState.RollingBack))
                {
                    throw new InvalidOperationException($"Transaction cannot be rolled back in state {transaction.State}");
                }

                // 回滚所有操作
                RollbackOperations(transaction);

                transaction.TryTransitionState(TransactionState.RollingBack, TransactionState.RolledBack);
            }
            catch (Exception ex)
            {
                transaction.MarkFailed();
                throw new InvalidOperationException($"Failed to rollback transaction: {ex.Message}", ex);
            }
            finally
            {
                RemoveActiveTransaction(transaction.TransactionId);
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

        lock (transaction.SyncRoot)
        {
            EnsureTransactionActive(transaction);
            if (transaction.State != TransactionState.Active)
            {
                throw new InvalidOperationException($"Cannot create savepoint in transaction state {transaction.State}");
            }

            var savepoint = new TransactionSavepoint(name, transaction.OperationCount);
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

        lock (transaction.SyncRoot)
        {
            EnsureTransactionActive(transaction);
            if (transaction.State != TransactionState.Active)
            {
                throw new InvalidOperationException($"Cannot rollback to savepoint in transaction state {transaction.State}");
            }

            if (!transaction.Savepoints.TryGetValue(savepointId, out var savepoint))
            {
                throw new ArgumentException("Savepoint not found", nameof(savepointId));
            }

            // 事务采用延迟提交模型：保存点回滚只需丢弃保存点之后的挂起操作，
            // 不应对数据库执行补偿写入。
            transaction.TrimOperations(savepoint.OperationCount);

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

        lock (transaction.SyncRoot)
        {
            EnsureTransactionActive(transaction);
            if (transaction.State != TransactionState.Active)
            {
                throw new InvalidOperationException($"Cannot release savepoint in transaction state {transaction.State}");
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

        EnsureTransactionActive(transaction);

        lock (transaction.SyncRoot)
        {
            var state = transaction.State;
            if (state != TransactionState.Active)
            {
                throw new InvalidOperationException($"Transaction cannot record operations in state {state}");
            }

            if (transaction.OperationCount >= MaxTransactionSize)
            {
                throw new InvalidOperationException(
                    $"Transaction {transaction.TransactionId} exceeded the configured maximum operation count of {MaxTransactionSize}.");
            }

            transaction.AddOperation(operation);
        }
    }

}
