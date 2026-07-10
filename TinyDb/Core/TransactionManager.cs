using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Metadata;
using System.Collections.Concurrent;

namespace TinyDb.Core;

/// <summary>
/// 事务管理器
/// </summary>
public sealed partial class TransactionManager : IDisposable, IAsyncDisposable
{
    private static readonly TimeSpan TimeoutCheckStopTimeout = TimeSpan.FromSeconds(5);
    internal readonly TinyDbEngine _engine;
    private readonly ConcurrentDictionary<Guid, Transaction> _activeTransactions;
    private readonly ConcurrentDictionary<Guid, DateTime> _timedOutTransactions;
    private readonly ConcurrentDictionary<string, Lazy<List<ForeignKeyDefinition>>> _foreignKeyCache = new(StringComparer.Ordinal);
    private readonly object _lock = new();
    private readonly CancellationTokenSource _timeoutCheckCts = new();
    private readonly Task _timeoutCheckTask;
    private int _activeTransactionCount;
    private int _disposed;

    internal record ForeignKeyDefinition(string FieldName, string ReferencedCollection);

    /// <summary>
    /// 活动事务数量
    /// </summary>
    public int ActiveTransactionCount
    {
        get => Volatile.Read(ref _activeTransactionCount);
    }

    /// <summary>
    /// 最大事务数量
    /// </summary>
    public int MaxTransactions { get; }

    public int MaxTransactionSize { get; }

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
    public TransactionManager(
        TinyDbEngine engine,
        int maxTransactions = 100,
        TimeSpan? transactionTimeout = null,
        int maxTransactionSize = 10000)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        if (maxTransactionSize <= 0) throw new ArgumentOutOfRangeException(nameof(maxTransactionSize));
        MaxTransactions = maxTransactions;
        MaxTransactionSize = maxTransactionSize;
        TransactionTimeout = transactionTimeout ?? TimeSpan.FromMinutes(5);
        _activeTransactions = new ConcurrentDictionary<Guid, Transaction>();
        _timedOutTransactions = new ConcurrentDictionary<Guid, DateTime>();

        // 启动超时检查任务
        _timeoutCheckTask = StartTimeoutCheckTask(_timeoutCheckCts.Token);
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
            if (Volatile.Read(ref _activeTransactionCount) >= MaxTransactions)
            {
                throw new InvalidOperationException($"Maximum number of transactions ({MaxTransactions}) reached");
            }

            var transaction = new Transaction(this);
            if (!_activeTransactions.TryAdd(transaction.TransactionId, transaction))
            {
                throw new InvalidOperationException($"Transaction '{transaction.TransactionId}' already exists");
            }

            Interlocked.Increment(ref _activeTransactionCount);
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
        lock (transaction.SyncRoot)
        {
            EnsureTransactionActive(transaction);

            if (!transaction.TryTransitionState(TransactionState.Active, TransactionState.Committing))
            {
                throw new InvalidOperationException($"Transaction cannot be committed in state {transaction.State}");
            }

            operations = transaction.GetOperationsSnapshot();
        }

        try
        {
            var transactionLockCollections = GetTransactionLockCollectionNames(operations).ToArray();
            using var collectionLocks = _engine.EnterCollectionCommitGates(transactionLockCollections);
            using var documentLocks = _engine.EnterCollectionDocumentLocks(GetTransactionDocumentLockKeys(operations));

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
                    var rollbackErrors = RollbackDurabilityScope(durabilityScope);
                    durabilityScope = null;
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
                    var aggregate = new AggregateException(
                        "Transaction durability commit failed and rollback compensation encountered errors.",
                        errors);
                    _engine.MarkCorrupted(aggregate);
                    throw aggregate;
                }
            }
            catch (Exception applyEx) when (durabilityScope != null)
            {
                var rollbackErrors = RollbackDurabilityScope(durabilityScope);
                durabilityScope = null;
                if (rollbackErrors.Count == 0)
                {
                    throw new InvalidOperationException(
                        "Transaction apply failed; applied page changes were rolled back.",
                        applyEx);
                }

                var errors = new List<Exception>
                {
                    new InvalidOperationException(
                        "Transaction apply failed.",
                        applyEx)
                };
                errors.AddRange(rollbackErrors);
                var aggregate = new AggregateException(
                    "Transaction apply failed and durability rollback encountered errors.",
                    errors);
                _engine.MarkCorrupted(aggregate);
                throw aggregate;
            }
            finally
            {
                durabilityScope?.Dispose();
            }

            transaction.TryTransitionState(TransactionState.Committing, TransactionState.Committed);
        }
        catch (Exception ex)
        {
            transaction.MarkFailed();

            throw new InvalidOperationException($"Failed to commit transaction: {ex.Message}", ex);
        }
        finally
        {
            RemoveActiveTransaction(transaction.TransactionId);
        }
    }

    private bool RemoveActiveTransaction(Guid transactionId)
    {
        if (!_activeTransactions.TryRemove(transactionId, out _))
        {
            return false;
        }

        Interlocked.Decrement(ref _activeTransactionCount);
        return true;
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
