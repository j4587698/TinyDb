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
    /// 执行检查点。
    /// 将所有脏页面刷新到磁盘，并截断 WAL 日志。
    /// </summary>
    public async Task CheckpointAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        EnsureInitialized();

        // 1. 刷新所有脏页面
        // 由于 SavePage 已包含 WAL 检查，这一步会确保相关日志先刷新
        await _pageManager.FlushDirtyPagesAsync(cancellationToken).ConfigureAwait(false);

        // 2. 截断 WAL
        await _writeAheadLog.TruncateAsync(cancellationToken).ConfigureAwait(false);
    }


    /// <summary>
    /// 将所有挂起的更改刷新到磁盘。
    /// </summary>
    public void Flush()
    {
        ThrowIfDisposed();
        FlushCore();
    }


    private void FlushCore()
    {
        if (!_isInitialized || _options.ReadOnly) return;
        _flushScheduler.Flush();
        FlushIdentitySequenceExactValues();
        _collectionMetaStore.SaveCollections(true);
        WriteHeader();
        _diskStream.Flush();
    }


    /// <summary>
    /// 获取数据库统计信息。
    /// </summary>
    /// <returns>统计信息对象。</returns>
    public DatabaseStatistics GetStatistics()
    {
        ThrowIfDisposed();
        EnsureInitialized();
        var pmStats = _pageManager.GetStatistics();
        return new DatabaseStatistics
        {
            FilePath = _filePath,
            DatabaseName = _header.DatabaseName,
            Version = _header.DatabaseVersion,
            CreatedAt = new DateTime(_header.CreatedAt, DateTimeKind.Utc),
            ModifiedAt = new DateTime(_header.ModifiedAt, DateTimeKind.Utc),
            PageSize = _header.PageSize,
            TotalPages = _header.TotalPages,
            UsedPages = _header.UsedPages,
            CollectionCount = GetCollectionNames().Count(),
            EnableJournaling = _options.EnableJournaling,
            IsReadOnly = _options.ReadOnly,
            FileSize = _diskStream.Size,
            FreePages = pmStats.FreePages,
            CachedPages = pmStats.CachedPages,
            CacheHitRatio = pmStats.CacheHitRatio
        };
    }


    /// <summary>
    /// 检查是否启用了写前日志 (WAL)。
    /// </summary>
    /// <returns>如果启用了 WAL 则为 true。</returns>
    public bool GetWalEnabled() => _writeAheadLog.IsEnabled;


    internal void Log(TinyDbLogLevel level, string message, Exception? ex = null)
    {
        TinyDbLogging.SafeLog(_log, level, message, ex);
    }


    internal void MarkCorrupted(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (Interlocked.CompareExchange(ref _corruptionException, exception, null) == null)
        {
            _pageManager.MarkCorrupted(exception);
            _flushScheduler.MarkCorrupted(exception);
            Log(TinyDbLogLevel.Critical, "TinyDbEngine marked corrupted. Dispose and reopen the database to recover from WAL.", exception);
        }
    }




    private static TimeSpan NormalizeInterval(TimeSpan i) => i == Timeout.InfiniteTimeSpan ? TimeSpan.Zero : i;


    private void EnsureInitialized() { if (!_isInitialized) throw new InvalidOperationException(); }


    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(TinyDbEngine));
        if (Volatile.Read(ref _corruptionException) is { } corruptionException)
        {
            throw new InvalidOperationException(
                "TinyDbEngine is corrupted after a failed compensation rollback. Dispose and reopen the database to recover from WAL.",
                corruptionException);
        }
    }


    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            Exception? flushException = null;
            Exception? metadataException = null;
            Exception? cleanupException = null;

            try
            {
                if (_isInitialized && !_options.ReadOnly && Volatile.Read(ref _corruptionException) == null)
                {
                    _collectionMetaStore.SaveCollections(false);
                }
            }
            catch (Exception ex)
            {
                metadataException = new InvalidOperationException("Failed to save collection metadata during dispose.", ex);
            }

            try
            {
                if (Volatile.Read(ref _corruptionException) == null)
                {
                    FlushCore();
                }
            }
            catch (Exception ex)
            {
                flushException = ex;
            }

            try
            {
                foreach (var c in _collections.Values) c.Dispose();
                _collections.Clear();
                _collectionStates.Clear();
                _transactionManager.Dispose();
                DisposeIndexManagers();
            }
            catch (Exception ex)
            {
                cleanupException = new InvalidOperationException("Dispose cleanup failed.", ex);
            }
            finally
            {
                DisposeComponents();
                _isInitialized = false;
            }

            if (flushException != null)
            {
                var flushRelated = new List<Exception> { flushException };
                if (metadataException != null) flushRelated.Add(metadataException);
                if (cleanupException != null) flushRelated.Add(cleanupException);
                if (flushRelated.Count > 1)
                {
                    throw new AggregateException("One or more errors occurred during dispose.", flushRelated);
                }

                ExceptionDispatchInfo.Capture(flushException).Throw();
            }

            if (metadataException != null || cleanupException != null)
            {
                var disposeRelated = new List<Exception>();
                if (metadataException != null) disposeRelated.Add(metadataException);
                if (cleanupException != null) disposeRelated.Add(cleanupException);
                throw new AggregateException("One or more errors occurred during dispose.", disposeRelated);
            }
        }
    }
}
