using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Security;
using TinyDb.Collections;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Attributes;
using TinyDb.Utils;

namespace TinyDb.Core;

internal sealed class EngineWalDurabilityScope : IDisposable
{
    private readonly WriteAheadLog.WalTransactionScope _walScope;
    private readonly IDisposable? _pageMutationLocks;
    private bool _disposed;

    public EngineWalDurabilityScope(WriteAheadLog.WalTransactionScope walScope, IDisposable? pageMutationLocks)
    {
        _walScope = walScope ?? throw new ArgumentNullException(nameof(walScope));
        _pageMutationLocks = pageMutationLocks;
    }

    public void Commit()
    {
        _walScope.Commit();
    }

    public Task CommitAsync(CancellationToken cancellationToken = default)
    {
        return _walScope.CommitAsync(cancellationToken);
    }

    public void Rollback(Action<uint, byte[]> restore, Action<uint>? discardNewPage)
    {
        _walScope.Rollback(restore, discardNewPage);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        try
        {
            _pageMutationLocks?.Dispose();
        }
        finally
        {
            _walScope.Dispose();
        }
    }
}

public sealed partial class TinyDbEngine
{
    /// <summary>
    /// 开始一个新事务。
    /// </summary>
    /// <returns>新创建的事务。</returns>
    public ITransaction BeginTransaction()
    {
        ThrowIfDisposed();
        EnsureInitialized();
        var t = _transactionManager.BeginTransaction();
        _currentTransaction.Value = new CurrentTransactionHolder(t);
        return t;
    }

    /// <summary>
    /// 获取指定集合的元数据。
    /// </summary>
    internal BsonDocument GetCollectionMetadata(string collectionName)
    {
        return _collectionMetaStore.GetMetadata(collectionName);
    }

    /// <summary>
    /// 确保当前事务的所有变更已持久化。
    /// </summary>
    internal void CommitTransactionDurability()
    {
        // 强制刷新 WAL 缓冲区到磁盘。
        // 对于最高安全级别 (WriteConcern.Journaled)，这将等待磁盘确认。
        _writeAheadLog.FlushLog();
    }

    internal EngineWalDurabilityScope BeginTransactionDurabilityScope(Guid transactionId)
    {
        var walScope = _writeAheadLog.BeginTransaction(transactionId);
        var pageMutationLocks = _writeAheadLog.IsEnabled
            ? CollectionState.RetainPageMutationLocksForCurrentContext()
            : null;
        return new EngineWalDurabilityScope(walScope, pageMutationLocks);
    }

    internal void RollbackTransactionDurabilityScope(EngineWalDurabilityScope durabilityScope)
    {
        if (durabilityScope == null) throw new ArgumentNullException(nameof(durabilityScope));

        durabilityScope.Rollback(
            (pageId, beforeImage) => _pageManager.RestorePage(pageId, beforeImage),
            pageId => _pageManager.DiscardCachedPage(pageId));
        ResetRuntimeStateAfterDurabilityRollback();
    }

    private void ResetRuntimeStateAfterDurabilityRollback()
    {
        lock (_lock)
        {
            _pageManager.ClearCache(flushDirtyPages: false);
            ReadHeader();
            _pageManager.Initialize(
                _header.TotalPages,
                _header.FirstFreePage,
                _header.FreePageCount,
                _header.HasFreePageCount);

            lock (_collectionStateInitLock)
            {
                Volatile.Write(
                    ref _collectionStates,
                    new ConcurrentDictionary<string, CollectionState>(StringComparer.Ordinal));
            }
            DisposeIndexManagers();

            _collectionMetaStore = new CollectionMetaStore(
                _pageManager,
                () => _header.CollectionInfoPage,
                id => _header.CollectionInfoPage = id);
            _collectionMetaStore.LoadCollections();

            _metadataManager = new TinyDb.Metadata.MetadataManager(this);
            _identitySequences.Clear();
            _transactionManager.ClearForeignKeyCache();
        }
    }

    private EngineWalDurabilityScope? BeginImplicitWalTransaction()
    {
        if (!_writeAheadLog.IsEnabled || _writeAheadLog.IsInTransactionScope)
        {
            return null;
        }

        var walScope = _writeAheadLog.BeginTransaction(Guid.NewGuid(), flushOnCommit: false);
        var pageMutationLocks = CollectionState.RetainPageMutationLocksForCurrentContext();
        return new EngineWalDurabilityScope(walScope, pageMutationLocks);
    }

    private readonly struct PendingImplicitWalTransaction
    {
        private readonly Task? _beginTask;

        public PendingImplicitWalTransaction(Guid transactionId, Task beginTask)
        {
            TransactionId = transactionId;
            _beginTask = beginTask;
        }

        public bool HasTransaction => _beginTask != null;
        public Guid TransactionId { get; }
        public Task BeginTask => _beginTask ?? Task.CompletedTask;
    }

    private PendingImplicitWalTransaction PrepareImplicitWalTransactionAsync(CancellationToken cancellationToken)
    {
        if (!_writeAheadLog.IsEnabled || _writeAheadLog.IsInTransactionScope)
        {
            return default;
        }

        var transactionId = Guid.NewGuid();
        return new PendingImplicitWalTransaction(
            transactionId,
            _writeAheadLog.WriteTransactionBeginAsync(transactionId, cancellationToken));
    }

    private EngineWalDurabilityScope? EnterImplicitWalTransactionContext(
        PendingImplicitWalTransaction pendingTransaction)
    {
        if (!pendingTransaction.HasTransaction)
        {
            return null;
        }

        var walScope = _writeAheadLog.EnterTransactionContext(pendingTransaction.TransactionId, flushOnCommit: false);
        var pageMutationLocks = CollectionState.RetainPageMutationLocksForCurrentContext();
        return new EngineWalDurabilityScope(walScope, pageMutationLocks);
    }

    private void RollbackImplicitWalTransaction(
        EngineWalDurabilityScope? durabilityScope,
        Exception originalException)
    {
        if (durabilityScope == null)
        {
            return;
        }

        try
        {
            RollbackTransactionDurabilityScope(durabilityScope);
        }
        catch (Exception rollbackException)
        {
            var aggregate = new AggregateException(
                "Implicit WAL transaction failed and rollback encountered errors.",
                originalException,
                rollbackException);
            MarkCorrupted(aggregate);
            throw aggregate;
        }
    }

    internal Transaction? GetCurrentTransaction()
    {
        var holder = _currentTransaction.Value;
        if (holder is not { IsActive: true } ||
            holder.Transaction is not Transaction transaction)
        {
            return null;
        }

        var state = transaction.State;
        if (state is TransactionState.Committed or TransactionState.RolledBack ||
            state == TransactionState.Failed && transaction.FailureReason == null)
        {
            ClearCurrentTransaction();
            return null;
        }

        _transactionManager.EnsureTransactionReadable(transaction);
        return transaction;
    }

    internal IDisposable SuppressCurrentTransaction()
    {
        var previous = _currentTransaction.Value;
        _currentTransaction.Value = null;
        return new CurrentTransactionScope(this, previous);
    }

    private sealed class CurrentTransactionScope : IDisposable
    {
        private readonly TinyDbEngine _engine;
        private readonly CurrentTransactionHolder? _previous;
        private bool _disposed;

        public CurrentTransactionScope(TinyDbEngine engine, CurrentTransactionHolder? previous)
        {
            _engine = engine;
            _previous = previous;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _engine._currentTransaction.Value = _previous is { IsActive: true } ? _previous : null;
            _disposed = true;
        }
    }

    /// <summary>
    /// 获取关于事务的统计信息。
    /// </summary>
    /// <returns>事务管理器统计信息。</returns>
    public TransactionManagerStatistics GetTransactionStatistics()
    {
        ThrowIfDisposed();
        return _transactionManager.GetStatistics();
    }

    internal void ClearForeignKeyCache()
    {
        _transactionManager.ClearForeignKeyCache();
    }


    internal void ClearCurrentTransaction()
    {
        _currentTransaction.Value?.Clear();
        _currentTransaction.Value = null;
    }

    private sealed class CurrentTransactionHolder
    {
        private int _isActive = 1;

        public CurrentTransactionHolder(ITransaction transaction)
        {
            Transaction = transaction;
        }

        public ITransaction Transaction { get; private set; }
        public bool IsActive => Volatile.Read(ref _isActive) != 0;

        public void Clear()
        {
            if (Interlocked.Exchange(ref _isActive, 0) == 0)
            {
                return;
            }

            Transaction = null!;
        }
    }

    private void EnsureWriteDurability()
    {
        if (_writeAheadLog.IsInTransactionScope) return;
        _flushScheduler.EnsureDurability(_options.WriteConcern);
    }

    private Task EnsureWriteDurabilityAsync(CancellationToken cancellationToken = default)
    {
        if (_writeAheadLog.IsInTransactionScope) return Task.CompletedTask;
        return _flushScheduler.EnsureDurabilityAsync(_options.WriteConcern, cancellationToken);
    }

    private IEnumerable<BsonDocument> MergeTransactionOperations(string col, IEnumerable<BsonDocument> ds, Transaction tx)
    {
        var overlay = new Dictionary<BsonValue, BsonDocument?>(BsonValueComparer.EqualityComparer);
        foreach (var op in tx.GetOperationsSnapshot().Where(o => o.CollectionName == col))
        {
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;

            if (op.OperationType == TransactionOperationType.Delete)
            {
                overlay[op.DocumentId] = null;
            }
            else if (op.NewDocument != null)
            {
                overlay[op.DocumentId] = op.NewDocument;
            }
        }

        if (overlay.Count == 0)
        {
            foreach (var document in ds)
            {
                yield return document;
            }

            yield break;
        }

        var seen = new HashSet<BsonValue>(BsonValueComparer.EqualityComparer);
        foreach (var document in ds)
        {
            if (!document.TryGetValue("_id", out var id) || id == null || id.IsNull)
            {
                continue;
            }

            if (overlay.TryGetValue(id, out var transactionDocument))
            {
                seen.Add(id);
                if (transactionDocument != null)
                {
                    yield return transactionDocument;
                }

                continue;
            }

            yield return document;
        }

        foreach (var pair in overlay)
        {
            if (pair.Value != null && !seen.Contains(pair.Key))
            {
                yield return pair.Value;
            }
        }
    }

    private static bool TryGetTransactionDocument(Transaction tx, string collectionName, BsonValue id, out BsonDocument? document)
    {
        var operations = tx.GetOperationsSnapshot();
        return TryGetTransactionDocument(operations, collectionName, id, out document);
    }

    private static bool TryGetTransactionDocument(
        IReadOnlyList<TransactionOperation> operations,
        string collectionName,
        BsonValue id,
        out BsonDocument? document)
    {
        document = null;
        if (operations.Count == 0) return false;

        for (int i = operations.Count - 1; i >= 0; i--)
        {
            var op = operations[i];
            if (op.CollectionName != collectionName) continue;
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;
            if (!BsonValuesEqual(op.DocumentId, id)) continue;

            if (op.OperationType == TransactionOperationType.Delete)
            {
                document = null;
                return true;
            }

            if (op.NewDocument != null)
            {
                document = op.NewDocument;
                return true;
            }

            document = null;
            return true;
        }

        return false;
    }
}
