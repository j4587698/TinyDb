using System;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;

namespace TinyDb.Storage;

/// <summary>
/// 后台刷盘调度器，负责协调 WAL 与数据页的刷新。
/// </summary>
public sealed class FlushScheduler : IDisposable, IAsyncDisposable
{
    private readonly PageManager _pageManager;
    private readonly WriteAheadLog _wal;
    private readonly TimeSpan _backgroundInterval;
    private readonly Action<TinyDbLogLevel, string, Exception?> _log;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task? _backgroundTask;
    private readonly object _flushLock = new();
    private static readonly TimeSpan GroupCommitDelay = TimeSpan.FromMilliseconds(1);
    private int _disposed;
    private Exception? _backgroundFailure;
    private int _foregroundFlushFailureObserved;
    
    private TaskCompletionSource _journalBatchTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _journalRequests = 0;
    private bool _journalWorkerRunning = false;
    private Task? _journalWorkerTask;

    private TaskCompletionSource _syncedBatchTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _syncedRequests = 0;
    private bool _syncedWorkerRunning = false;
    private Task? _syncedWorkerTask;

    /// <summary>
    /// 初始化 FlushScheduler 类的新实例。
    /// </summary>
    /// <param name="pageManager">页面管理器。</param>
    /// <param name="wal">写前日志。</param>
    /// <param name="backgroundInterval">后台刷新间隔。</param>
    public FlushScheduler(
        PageManager pageManager,
        WriteAheadLog wal,
        TimeSpan backgroundInterval,
        Action<TinyDbLogLevel, string, Exception?>? logger = null)
    {
        _pageManager = pageManager ?? throw new ArgumentNullException(nameof(pageManager));
        _wal = wal ?? throw new ArgumentNullException(nameof(wal));
        _backgroundInterval = backgroundInterval;
        _log = logger ?? TinyDbLogging.NoopLogger;

        if (_backgroundInterval > TimeSpan.Zero || _wal.IsEnabled)
        {
            _backgroundTask = Task.Run(BackgroundLoopAsync);
        }
    }

    private void Log(TinyDbLogLevel level, string message, Exception? ex = null)
    {
        TinyDbLogging.SafeLog(_log, level, message, ex);
    }

    private async Task BackgroundLoopAsync()
    {
        var effectiveInterval = _backgroundInterval > TimeSpan.Zero
            ? _backgroundInterval
            : TimeSpan.FromMilliseconds(100);

        while (!_cts.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(effectiveInterval, _cts.Token).ConfigureAwait(false);
                await FlushPendingAsync(_cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                // Expected during shutdown.
            }
            catch (Exception ex)
            {
                if (!_cts.IsCancellationRequested || ex is not ObjectDisposedException)
                {
                    RecordBackgroundFailure(ex);
                }

                Log(TinyDbLogLevel.Warning, "Background flush loop failed.", ex);
                break;
            }
        }
    }

    public async Task EnsureDurabilityAsync(WriteConcern concern, CancellationToken cancellationToken = default)
    {
        ThrowIfBackgroundFailed();

        switch (concern)
        {
            case WriteConcern.None:
                return;
            case WriteConcern.Journaled:
                await EnsureJournalFlushAsync(cancellationToken).ConfigureAwait(false);
                return;
            case WriteConcern.Synced:
                await EnsureSyncedFlushAsync(cancellationToken).ConfigureAwait(false);
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(concern), concern, null);
        }
    }

    public void EnsureDurability(WriteConcern concern)
    {
        ThrowIfBackgroundFailed();

        switch (concern)
        {
            case WriteConcern.None:
                return;
            case WriteConcern.Journaled:
                EnsureJournalFlush();
                return;
            case WriteConcern.Synced:
                EnsureSyncedFlush();
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(concern), concern, null);
        }
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfBackgroundFailed();
        return _wal.SynchronizeAsync(ct => _pageManager.FlushDirtyPagesAsync(ct), cancellationToken);
    }

    public void Flush()
    {
        ThrowIfBackgroundFailed();
        _wal.Synchronize(() => _pageManager.FlushDirtyPages());
    }

    private Task EnsureJournalFlushAsync(CancellationToken cancellationToken)
    {
        if (!_wal.IsEnabled)
        {
            return _pageManager.FlushDirtyPagesAsync(cancellationToken);
        }

        if (!_wal.HasPendingEntries)
        {
            return Task.CompletedTask;
        }

        Task batchTask;

        lock (_flushLock)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                throw new ObjectDisposedException(nameof(FlushScheduler));
            }

            ThrowIfBackgroundFailed();

            if (_journalBatchTcs.Task.IsCompleted)
            {
                _journalBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _journalRequests++;
            batchTask = _journalBatchTcs.Task;

            if (!_journalWorkerRunning)
            {
                _journalWorkerRunning = true;
                _journalWorkerTask = Task.Run(RunJournalWorkerAsync);
            }
        }

        return cancellationToken.CanBeCanceled ? batchTask.WaitAsync(cancellationToken) : batchTask;
    }

    private void EnsureJournalFlush()
    {
        if (!_wal.IsEnabled)
        {
            _pageManager.FlushDirtyPages();
            return;
        }

        if (!_wal.HasPendingEntries)
        {
            return;
        }

        _wal.FlushLog();
    }

    private Task EnsureSyncedFlushAsync(CancellationToken cancellationToken)
    {
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages())
        {
            return Task.CompletedTask;
        }

        Task batchTask;

        lock (_flushLock)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                throw new ObjectDisposedException(nameof(FlushScheduler));
            }

            ThrowIfBackgroundFailed();

            if (_syncedBatchTcs.Task.IsCompleted)
            {
                _syncedBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _syncedRequests++;
            batchTask = _syncedBatchTcs.Task;

            if (!_syncedWorkerRunning)
            {
                _syncedWorkerRunning = true;
                _syncedWorkerTask = Task.Run(RunSyncedWorkerAsync);
            }
        }

        return cancellationToken.CanBeCanceled ? batchTask.WaitAsync(cancellationToken) : batchTask;
    }

    private void EnsureSyncedFlush()
    {
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages())
        {
            return;
        }

        bool joinAsyncBatch;
        lock (_flushLock)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                throw new ObjectDisposedException(nameof(FlushScheduler));
            }

            ThrowIfBackgroundFailed();
            joinAsyncBatch = _syncedWorkerRunning || _syncedRequests > 0;
            if (!joinAsyncBatch)
            {
                _wal.Synchronize(() => _pageManager.FlushDirtyPages(), truncateLog: false);
                return;
            }
        }

        EnsureSyncedFlushAsync(CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
    }

    private async Task RunSyncedWorkerAsync()
    {
        while (true)
        {
            TaskCompletionSource tcsToComplete;
            bool delayForBatch;

            try
            {
                await Task.Yield();
            }
            catch (OperationCanceledException)
            {
                CancelSyncedBatch();
                return;
            }

            lock (_flushLock)
            {
                delayForBatch = _syncedRequests > 1;
            }

            if (delayForBatch)
            {
                try
                {
                    await Task.Delay(GroupCommitDelay, _cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    CancelSyncedBatch();
                    return;
                }
            }

            lock (_flushLock)
            {
                if (_syncedRequests <= 0)
                {
                    _syncedWorkerRunning = false;
                    return;
                }

                tcsToComplete = _syncedBatchTcs;
                _syncedBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _syncedRequests = 0;
            }

            try
            {
                await _wal.SynchronizeAsync(
                    ct => _pageManager.FlushDirtyPagesAsync(ct),
                    truncateLog: false,
                    _cts.Token).ConfigureAwait(false);
                tcsToComplete.TrySetResult();
            }
            catch (OperationCanceledException)
            {
                tcsToComplete.TrySetCanceled();
                CancelSyncedBatch();
                return;
            }
            catch (Exception ex)
            {
                Volatile.Write(ref _foregroundFlushFailureObserved, 1);
                tcsToComplete.TrySetException(ex);
            }
        }
    }

    private void CancelSyncedBatch()
    {
        TaskCompletionSource? pendingToCancel = null;
        lock (_flushLock)
        {
            if (_syncedRequests > 0)
            {
                pendingToCancel = _syncedBatchTcs;
                _syncedBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _syncedRequests = 0;
            }

            _syncedWorkerRunning = false;
        }

        pendingToCancel?.TrySetCanceled();
    }

    private async Task RunJournalWorkerAsync()
    {
        while (true)
        {
            TaskCompletionSource tcsToComplete;

            lock (_flushLock)
            {
                if (_journalRequests <= 0)
                {
                    _journalWorkerRunning = false;
                    return;
                }

                tcsToComplete = _journalBatchTcs;
                _journalBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _journalRequests = 0;
            }

            try
            {
                await _wal.FlushLogAsync(_cts.Token).ConfigureAwait(false);
                tcsToComplete.TrySetResult();
            }
            catch (OperationCanceledException)
            {
                tcsToComplete.TrySetCanceled();

                TaskCompletionSource? pendingToCancel = null;
                lock (_flushLock)
                {
                    if (_journalRequests > 0)
                    {
                        pendingToCancel = _journalBatchTcs;
                        _journalBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                        _journalRequests = 0;
                    }

                    _journalWorkerRunning = false;
                }

                pendingToCancel?.TrySetCanceled();
                return;
            }
            catch (Exception ex)
            {
                RecordBackgroundFailure(ex);
                tcsToComplete.TrySetException(ex);
            }
        }
    }

    public async Task FlushPendingAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfBackgroundFailed();
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages()) return;
        await _wal.SynchronizeAsync(ct => _pageManager.FlushDirtyPagesAsync(ct), cancellationToken).ConfigureAwait(false);
    }

    public void FlushPending()
    {
        ThrowIfBackgroundFailed();
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages()) return;
        _wal.Synchronize(() => _pageManager.FlushDirtyPages());
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _cts.Cancel();

        Exception? journalWaitException = null;
        Exception? syncedWaitException = null;
        Exception? backgroundWaitException = null;
        Exception? flushException = null;

        try
        {
            _journalWorkerTask?.ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            journalWaitException = new InvalidOperationException("Journal worker stop failed.", ex);
        }

        try
        {
            _syncedWorkerTask?.ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            syncedWaitException = new InvalidOperationException("Synced worker stop failed.", ex);
        }

        try
        {
            _backgroundTask?.ConfigureAwait(false).GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            backgroundWaitException = new InvalidOperationException("Background worker stop failed.", ex);
        }

        try
        {
            FlushForDispose();
        }
        catch (Exception ex)
        {
            flushException = ex;
        }
        finally
        {
            _cts.Dispose();
        }

        if (flushException == null &&
            Volatile.Read(ref _backgroundFailure) is { } backgroundFailure &&
            Volatile.Read(ref _foregroundFlushFailureObserved) == 0)
        {
            flushException = new InvalidOperationException("A background flush operation failed.", backgroundFailure);
        }

        if (journalWaitException != null || syncedWaitException != null || backgroundWaitException != null || flushException != null)
        {
            var exceptions = new List<Exception>();
            if (journalWaitException != null) exceptions.Add(journalWaitException);
            if (syncedWaitException != null) exceptions.Add(syncedWaitException);
            if (backgroundWaitException != null) exceptions.Add(backgroundWaitException);
            if (flushException != null) exceptions.Add(flushException);
            throw new AggregateException("One or more errors occurred during FlushScheduler dispose.", exceptions);
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _cts.Cancel();

        Exception? journalWaitException = null;
        Exception? syncedWaitException = null;
        Exception? backgroundWaitException = null;
        Exception? flushException = null;

        try
        {
            if (_journalWorkerTask != null)
            {
                await _journalWorkerTask.ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            journalWaitException = new InvalidOperationException("Journal worker stop failed.", ex);
        }

        try
        {
            if (_syncedWorkerTask != null)
            {
                await _syncedWorkerTask.ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            syncedWaitException = new InvalidOperationException("Synced worker stop failed.", ex);
        }

        try
        {
            if (_backgroundTask != null)
            {
                await _backgroundTask.ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            backgroundWaitException = new InvalidOperationException("Background worker stop failed.", ex);
        }

        try
        {
            await FlushForDisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            flushException = ex;
        }
        finally
        {
            _cts.Dispose();
        }

        if (flushException == null &&
            Volatile.Read(ref _backgroundFailure) is { } backgroundFailure &&
            Volatile.Read(ref _foregroundFlushFailureObserved) == 0)
        {
            flushException = new InvalidOperationException("A background flush operation failed.", backgroundFailure);
        }

        if (journalWaitException != null || syncedWaitException != null || backgroundWaitException != null || flushException != null)
        {
            var exceptions = new List<Exception>();
            if (journalWaitException != null) exceptions.Add(journalWaitException);
            if (syncedWaitException != null) exceptions.Add(syncedWaitException);
            if (backgroundWaitException != null) exceptions.Add(backgroundWaitException);
            if (flushException != null) exceptions.Add(flushException);
            throw new AggregateException("One or more errors occurred during FlushScheduler dispose.", exceptions);
        }
    }

    private void RecordBackgroundFailure(Exception exception)
    {
        Interlocked.CompareExchange(ref _backgroundFailure, exception, null);
    }

    private void FlushForDispose()
    {
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages()) return;
        _wal.Synchronize(() => _pageManager.FlushDirtyPages());
    }

    private async Task FlushForDisposeAsync()
    {
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages()) return;
        await _wal.SynchronizeAsync(ct => _pageManager.FlushDirtyPagesAsync(ct), CancellationToken.None).ConfigureAwait(false);
    }

    private void ThrowIfBackgroundFailed()
    {
        var failure = Volatile.Read(ref _backgroundFailure);
        if (failure != null)
        {
            throw new InvalidOperationException("A background flush operation failed.", failure);
        }
    }
}
