using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Metadata;
using System.Collections.Concurrent;

namespace TinyDb.Core;

public sealed partial class TransactionManager
{

    /// <summary>
    /// 启动超时检查任务
    /// </summary>
    /// <returns>任务</returns>
    private async Task StartTimeoutCheckTask(CancellationToken cancellationToken)
    {
        while (Volatile.Read(ref _disposed) == 0)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(30), cancellationToken).ConfigureAwait(false);
                if (Volatile.Read(ref _disposed) != 0) break;
                CheckAndCleanupExpiredTransactions();
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _engine.Log(TinyDbLogLevel.Warning, "Transaction timeout check failed.", ex);
            }
        }
    }


    /// <summary>
    /// 检查并清理过期事务
    /// </summary>
    internal void CheckAndCleanupExpiredTransactions()
    {
        lock (_lock)
        {
            var now = DateTime.UtcNow;
            var expiredTransactions = _activeTransactions
                .Where(kvp => kvp.Value.State == TransactionState.Active &&
                              now - kvp.Value.StartTime > TransactionTimeout)
                .Select(kvp => kvp.Value)
                .ToList();

            foreach (var expiredTransaction in expiredTransactions)
            {
                var failureReason = CreateTransactionTimeoutMessage(expiredTransaction.TransactionId, now);
                if (!expiredTransaction.TryTransitionState(TransactionState.Active, TransactionState.Failed)) continue;
                expiredTransaction.FailureReason = failureReason;
                RemoveActiveTransaction(expiredTransaction.TransactionId);
                _timedOutTransactions[expiredTransaction.TransactionId] = now;
            }

            PruneTimedOutTransactionRecords(now);
        }
    }


    private void EnsureTransactionActive(Transaction transaction)
    {
        if (_activeTransactions.ContainsKey(transaction.TransactionId))
        {
            return;
        }

        if (_timedOutTransactions.TryGetValue(transaction.TransactionId, out var timedOutAt))
        {
            throw new InvalidOperationException(CreateTransactionTimeoutMessage(transaction.TransactionId, timedOutAt));
        }

        throw new InvalidOperationException("Transaction is not active");
    }


    internal void EnsureTransactionReadable(Transaction transaction)
    {
        if (transaction == null) throw new ArgumentNullException(nameof(transaction));
        EnsureTransactionActive(transaction);

        if (transaction.State != TransactionState.Active)
        {
            throw new InvalidOperationException($"Transaction cannot be read in state {transaction.State}");
        }
    }


    private string CreateTransactionTimeoutMessage(Guid transactionId, DateTime timedOutAt)
    {
        return $"Transaction {transactionId} timed out at {timedOutAt:O} after exceeding the configured timeout of {TransactionTimeout}.";
    }


    private void PruneTimedOutTransactionRecords(DateTime now)
    {
        if (_timedOutTransactions.Count == 0)
        {
            return;
        }

        var retention = TransactionTimeout > TimeSpan.FromMinutes(5)
            ? TransactionTimeout
            : TimeSpan.FromMinutes(5);
        var expiredKeys = _timedOutTransactions
            .Where(kvp => now - kvp.Value > retention)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var transactionId in expiredKeys)
        {
            _timedOutTransactions.TryRemove(transactionId, out _);
        }
    }

}
