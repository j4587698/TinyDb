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
        _currentTransaction.Value = t;
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

    internal WriteAheadLog.WalTransactionScope BeginTransactionDurabilityScope(Guid transactionId)
    {
        return _writeAheadLog.BeginTransaction(transactionId);
    }

    internal void RollbackTransactionDurabilityScope(WriteAheadLog.WalTransactionScope durabilityScope)
    {
        if (durabilityScope == null) throw new ArgumentNullException(nameof(durabilityScope));

        durabilityScope.Rollback((pageId, beforeImage) => _pageManager.RestorePage(pageId, beforeImage));
        ResetRuntimeStateAfterDurabilityRollback();
    }

    private void ResetRuntimeStateAfterDurabilityRollback()
    {
        lock (_lock)
        {
            _pageManager.ClearCache();
            ReadHeader();
            _pageManager.Initialize(
                _header.TotalPages,
                _header.FirstFreePage,
                _header.FreePageCount,
                _header.HasFreePageCount);

            _collectionStates.Clear();
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

    private WriteAheadLog.WalTransactionScope? BeginImplicitWalTransaction()
    {
        if (!_writeAheadLog.IsEnabled || _writeAheadLog.IsInTransactionScope)
        {
            return null;
        }

        return _writeAheadLog.BeginTransaction(Guid.NewGuid(), flushOnCommit: false);
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

    private WriteAheadLog.WalTransactionScope? EnterImplicitWalTransactionContext(
        PendingImplicitWalTransaction pendingTransaction)
    {
        return pendingTransaction.HasTransaction
            ? _writeAheadLog.EnterTransactionContext(pendingTransaction.TransactionId, flushOnCommit: false)
            : null;
    }

    private void RollbackImplicitWalTransaction(
        WriteAheadLog.WalTransactionScope? durabilityScope,
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
        if (_currentTransaction.Value is not Transaction transaction)
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
        private readonly ITransaction? _previous;
        private bool _disposed;

        public CurrentTransactionScope(TinyDbEngine engine, ITransaction? previous)
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

            _engine._currentTransaction.Value = _previous;
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


    internal void ClearCurrentTransaction() => _currentTransaction.Value = null;

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
        var dict = new Dictionary<BsonValue, BsonDocument?>(BsonValueComparer.EqualityComparer);

        foreach (var document in ds)
        {
            if (document.TryGetValue("_id", out var id) && id != null && !id.IsNull)
            {
                dict[id] = document;
            }
        }

        foreach (var op in tx.GetOperationsSnapshot().Where(o => o.CollectionName == col))
        {
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;

            if (op.OperationType == TransactionOperationType.Delete)
            {
                dict[op.DocumentId] = null;
            }
            else if (op.NewDocument != null)
            {
                dict[op.DocumentId] = op.NewDocument;
            }
        }

        return dict.Values.Where(document => document != null).Select(document => document!);
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
