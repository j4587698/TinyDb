using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Metadata;
using System.Collections.Concurrent;

namespace TinyDb.Core;

public sealed partial class TransactionManager
{

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
        _timeoutCheckCts.Cancel();

        // 回滚所有活动事务
        FailActiveTransactions();

        WaitForTimeoutCheckTask();
        _timeoutCheckCts.Dispose();
    }


    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _timeoutCheckCts.Cancel();

        FailActiveTransactions();
        await WaitForTimeoutCheckTaskAsync().ConfigureAwait(false);
        _timeoutCheckCts.Dispose();
    }


    private void FailActiveTransactions()
    {
        lock (_lock)
        {
            foreach (var transaction in _activeTransactions.Values)
            {
                transaction.MarkFailed();
            }

            _activeTransactions.Clear();
            Interlocked.Exchange(ref _activeTransactionCount, 0);
        }
    }


    private void WaitForTimeoutCheckTask()
    {
        try
        {
            if (!_timeoutCheckTask.Wait(TimeoutCheckStopTimeout))
            {
                throw new TimeoutException();
            }
        }
        catch (TimeoutException)
        {
            _engine.Log(TinyDbLogLevel.Warning, "Transaction timeout check task did not stop before dispose timeout.");
        }
        catch (AggregateException ex) when (ex.InnerExceptions.Count == 1 && ex.InnerException is OperationCanceledException)
        {
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _engine.Log(TinyDbLogLevel.Warning, "Transaction timeout check task stopped with an error.", ex);
        }
    }
    private async Task WaitForTimeoutCheckTaskAsync()
    {
        try
        {
            await _timeoutCheckTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception ex)
        {
            _engine.Log(TinyDbLogLevel.Warning, "Transaction timeout check task stopped with an error.", ex);
        }
    }


    public override string ToString()
    {
        return $"TransactionManager: {ActiveTransactionCount}/{MaxTransactions} active, Timeout={TransactionTimeout.TotalMinutes:F1}min";
    }
}
