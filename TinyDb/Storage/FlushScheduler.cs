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
    private readonly SemaphoreSlim _journalSignal = new(0);
    private readonly SemaphoreSlim _syncedSignal = new(0);
    private static readonly TimeSpan GroupCommitDelay = TimeSpan.FromMilliseconds(1);
    private static readonly TimeSpan WorkerStopTimeout = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan BackgroundFailureInitialDelay = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan BackgroundFailureMaxDelay = TimeSpan.FromSeconds(5);
    private int _disposed;
    private int _ctsDisposed;
    private int _signalsDisposed;
    private Exception? _backgroundFailure;
    private Exception? _corruptionException;
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

    internal void MarkCorrupted(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        Interlocked.CompareExchange(ref _corruptionException, exception, null);
    }

    private bool IsCorrupted => Volatile.Read(ref _corruptionException) != null;

    private async Task BackgroundLoopAsync()
    {
        var effectiveInterval = _backgroundInterval > TimeSpan.Zero
            ? _backgroundInterval
            : TimeSpan.FromMilliseconds(100);
        var failureDelay = TimeSpan.Zero;

        while (!_cts.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(effectiveInterval, _cts.Token).ConfigureAwait(false);
                await FlushPendingAsync(_cts.Token).ConfigureAwait(false);
                failureDelay = TimeSpan.Zero;
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
                    Log(TinyDbLogLevel.Warning, "Background flush attempt failed.", ex);
                }

                failureDelay = NextBackgroundFailureDelay(failureDelay);
                try
                {
                    await Task.Delay(failureDelay, _cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (_cts.IsCancellationRequested)
                {
                    // Expected during shutdown.
                }
            }
        }
    }

    private static TimeSpan NextBackgroundFailureDelay(TimeSpan currentDelay)
    {
        if (currentDelay <= TimeSpan.Zero)
        {
            return BackgroundFailureInitialDelay;
        }

        var nextMilliseconds = Math.Min(currentDelay.TotalMilliseconds * 2, BackgroundFailureMaxDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(nextMilliseconds);
    }

    public async Task EnsureDurabilityAsync(WriteConcern concern, CancellationToken cancellationToken = default)
    {
        AllowForegroundRetryAfterBackgroundFailure();

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
        AllowForegroundRetryAfterBackgroundFailure();

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
        AllowForegroundRetryAfterBackgroundFailure();
        return _wal.SynchronizeAsync(
            (ctx, ct) => _pageManager.FlushDirtyPagesAsync(ctx, ct),
            truncateLog: true,
            cancellationToken);
    }

    public void Flush()
    {
        AllowForegroundRetryAfterBackgroundFailure();
        _wal.Synchronize(ctx => _pageManager.FlushDirtyPages(ctx), truncateLog: true);
    }

    private Task EnsureJournalFlushAsync(CancellationToken cancellationToken)
    {
        var batchTask = EnqueueJournalFlush();
        return cancellationToken.CanBeCanceled ? batchTask.WaitAsync(cancellationToken) : batchTask;
    }

    private void EnsureJournalFlush()
    {
        EnqueueJournalFlush().GetAwaiter().GetResult();
    }

    private Task EnsureSyncedFlushAsync(CancellationToken cancellationToken)
    {
        var batchTask = EnqueueSyncedFlush();
        return cancellationToken.CanBeCanceled ? batchTask.WaitAsync(cancellationToken) : batchTask;
    }

    private Task EnqueueJournalFlush()
    {
        if (!_wal.IsEnabled || !_wal.HasPendingEntries)
        {
            return Task.CompletedTask;
        }

        Task batchTask;
        bool shouldSignal;
        lock (_flushLock)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                throw new ObjectDisposedException(nameof(FlushScheduler));
            }

            AllowForegroundRetryAfterBackgroundFailure();

            if (_journalBatchTcs.Task.IsCompleted)
            {
                _journalBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            shouldSignal = _journalRequests == 0;
            _journalRequests++;
            batchTask = _journalBatchTcs.Task;

            if (!_journalWorkerRunning)
            {
                _journalWorkerRunning = true;
                _journalWorkerTask = StartDedicatedWorker(RunJournalWorker, "TinyDb journal flush worker");
            }
        }

        if (shouldSignal)
        {
            _journalSignal.Release();
        }

        return batchTask;
    }

    private Task EnqueueSyncedFlush()
    {
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages())
        {
            return Task.CompletedTask;
        }

        Task batchTask;
        bool shouldSignal;
        lock (_flushLock)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                throw new ObjectDisposedException(nameof(FlushScheduler));
            }

            AllowForegroundRetryAfterBackgroundFailure();

            if (_syncedBatchTcs.Task.IsCompleted)
            {
                _syncedBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            shouldSignal = _syncedRequests == 0;
            _syncedRequests++;
            batchTask = _syncedBatchTcs.Task;

            if (!_syncedWorkerRunning)
            {
                _syncedWorkerRunning = true;
                _syncedWorkerTask = StartDedicatedWorker(RunSyncedWorker, "TinyDb synced flush worker");
            }
        }

        if (shouldSignal)
        {
            _syncedSignal.Release();
        }

        return batchTask;
    }

    private void EnsureSyncedFlush()
    {
        EnqueueSyncedFlush().GetAwaiter().GetResult();
    }

    private static Task StartDedicatedWorker(Action worker, string name)
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thread = new Thread(() =>
        {
            try
            {
                worker();
                completion.TrySetResult();
            }
            catch (Exception ex)
            {
                completion.TrySetException(ex);
            }
        })
        {
            IsBackground = true,
            Name = name
        };

        thread.Start();
        return completion.Task;
    }

    private void RunSyncedWorker()
    {
        while (true)
        {
            try
            {
                _syncedSignal.Wait(_cts.Token);
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                CancelSyncedBatch();
                return;
            }

            while (true)
            {
                TaskCompletionSource tcsToComplete;
                bool delayForBatch;

                if (_cts.IsCancellationRequested)
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
                    if (_cts.Token.WaitHandle.WaitOne(GroupCommitDelay))
                    {
                        CancelSyncedBatch();
                        return;
                    }
                }

                lock (_flushLock)
                {
                    if (_syncedRequests <= 0)
                    {
                        break;
                    }

                    tcsToComplete = _syncedBatchTcs;
                    _syncedBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    _syncedRequests = 0;
                }

                try
                {
                    _wal.Synchronize(ctx => _pageManager.FlushDirtyPages(ctx), truncateLog: false, _cts.Token);
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
                    RecordBackgroundFailure(ex);
                    tcsToComplete.TrySetException(ex);
                    FaultPendingSyncedBatch(ex);
                    return;
                }
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

    private void FaultPendingSyncedBatch(Exception exception)
    {
        TaskCompletionSource? pendingToFault = null;
        lock (_flushLock)
        {
            if (_syncedRequests > 0)
            {
                pendingToFault = _syncedBatchTcs;
                _syncedBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _syncedRequests = 0;
            }

            _syncedWorkerRunning = false;
        }

        pendingToFault?.TrySetException(exception);
    }

    private void RunJournalWorker()
    {
        while (true)
        {
            try
            {
                _journalSignal.Wait(_cts.Token);
            }
            catch (OperationCanceledException) when (_cts.IsCancellationRequested)
            {
                CancelJournalBatch();
                return;
            }

            while (true)
            {
                TaskCompletionSource tcsToComplete;

                if (_cts.IsCancellationRequested)
                {
                    CancelJournalBatch();
                    return;
                }

                lock (_flushLock)
                {
                    if (_journalRequests <= 0)
                    {
                        break;
                    }

                    tcsToComplete = _journalBatchTcs;
                    _journalBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    _journalRequests = 0;
                }

                try
                {
                    _wal.FlushLog(writeContext: null, _cts.Token);
                    tcsToComplete.TrySetResult();
                }
                catch (OperationCanceledException)
                {
                    tcsToComplete.TrySetCanceled();
                    CancelJournalBatch();
                    return;
                }
                catch (Exception ex)
                {
                    RecordBackgroundFailure(ex);
                    tcsToComplete.TrySetException(ex);
                    FaultPendingJournalBatch(ex);
                    return;
                }
            }
        }
    }

    private void CancelJournalBatch()
    {
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
    }

    private void FaultPendingJournalBatch(Exception exception)
    {
        TaskCompletionSource? pendingToFault = null;
        lock (_flushLock)
        {
            if (_journalRequests > 0)
            {
                pendingToFault = _journalBatchTcs;
                _journalBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _journalRequests = 0;
            }

            _journalWorkerRunning = false;
        }

        pendingToFault?.TrySetException(exception);
    }

    public async Task FlushPendingAsync(CancellationToken cancellationToken = default)
    {
        AllowForegroundRetryAfterBackgroundFailure();
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages()) return;
        await _wal.SynchronizeAsync(
            (ctx, ct) => _pageManager.FlushDirtyPagesAsync(ctx, ct),
            truncateLog: true,
            cancellationToken).ConfigureAwait(false);
    }

    public void FlushPending()
    {
        AllowForegroundRetryAfterBackgroundFailure();
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages()) return;
        _wal.Synchronize(ctx => _pageManager.FlushDirtyPages(ctx), truncateLog: true);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        try
        {
            _cts.Cancel();

            var workerTasks = CancelPendingBatchesForDisposeAndGetWorkerTasks();

            Exception? flushException = null;
            var workerWaitErrors = new List<Exception>();

            try
            {
                WaitForWorkersAndDisposeCancellationTokenSource(workerTasks, workerWaitErrors);
                FlushForDispose();
            }
            catch (Exception ex)
            {
                flushException = ex;
            }

            if (flushException == null &&
                !IsCorrupted &&
                Volatile.Read(ref _backgroundFailure) is { } backgroundFailure &&
                Volatile.Read(ref _foregroundFlushFailureObserved) == 0)
            {
                flushException = new InvalidOperationException("A background flush operation failed.", backgroundFailure);
            }

            if (flushException != null || workerWaitErrors.Count > 0)
            {
                var exceptions = new List<Exception>();
                if (flushException != null) exceptions.Add(flushException);
                exceptions.AddRange(workerWaitErrors);
                throw new AggregateException("One or more errors occurred during FlushScheduler dispose.", exceptions);
            }
        }
        finally
        {
            DisposeSignals();
        }
    }

    private (Task Task, string FailureMessage)[] CancelPendingBatchesForDisposeAndGetWorkerTasks()
    {
        TaskCompletionSource? journalBatchToCancel = null;
        TaskCompletionSource? syncedBatchToCancel = null;
        (Task Task, string FailureMessage)[] workerTasks;

        lock (_flushLock)
        {
            if (_journalRequests > 0)
            {
                journalBatchToCancel = _journalBatchTcs;
                _journalBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _journalRequests = 0;
            }

            if (_syncedRequests > 0)
            {
                syncedBatchToCancel = _syncedBatchTcs;
                _syncedBatchTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _syncedRequests = 0;
            }

            workerTasks = GetWorkerTasks();
        }

        journalBatchToCancel?.TrySetCanceled();
        syncedBatchToCancel?.TrySetCanceled();
        return workerTasks;
    }

    private void WaitForWorkersAndDisposeCancellationTokenSource(
        (Task Task, string FailureMessage)[] workerTasks,
        List<Exception> workerWaitErrors)
    {
        try
        {
            foreach (var workerTask in workerTasks)
            {
                try
                {
                    if (!workerTask.Task.Wait(WorkerStopTimeout))
                    {
                        throw new TimeoutException();
                    }
                }
                catch (TimeoutException ex)
                {
                    var timeoutException = new TimeoutException("Flush worker did not stop before dispose timeout.", ex);
                    Log(TinyDbLogLevel.Warning, timeoutException.Message, timeoutException);
                }
                catch (Exception ex)
                {
                    workerWaitErrors.Add(new InvalidOperationException(workerTask.FailureMessage, ex));
                }
            }
        }
        finally
        {
            DisposeCancellationTokenSource();
        }
    }

    private (Task Task, string FailureMessage)[] GetWorkerTasks()
    {
        var tasks = new List<(Task Task, string FailureMessage)>(3);
        if (_journalWorkerTask != null) tasks.Add((_journalWorkerTask, "Journal worker stop failed."));
        if (_syncedWorkerTask != null) tasks.Add((_syncedWorkerTask, "Synced worker stop failed."));
        if (_backgroundTask != null) tasks.Add((_backgroundTask, "Background worker stop failed."));
        return tasks.ToArray();
    }

    private void DisposeCancellationTokenSource()
    {
        if (Interlocked.Exchange(ref _ctsDisposed, 1) == 0)
        {
            _cts.Dispose();
        }
    }

    private void DisposeSignals()
    {
        if (Interlocked.Exchange(ref _signalsDisposed, 1) == 0)
        {
            _journalSignal.Dispose();
            _syncedSignal.Dispose();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        try
        {
            _cts.Cancel();
            var workerTasks = CancelPendingBatchesForDisposeAndGetWorkerTasks();

            var workerWaitExceptions = new List<Exception>();
            Exception? flushException = null;

            foreach (var workerTask in workerTasks)
            {
                try
                {
                    await workerTask.Task.ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    workerWaitExceptions.Add(new InvalidOperationException(workerTask.FailureMessage, ex));
                }
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
                DisposeCancellationTokenSource();
            }

            if (flushException == null &&
                !IsCorrupted &&
                Volatile.Read(ref _backgroundFailure) is { } backgroundFailure &&
                Volatile.Read(ref _foregroundFlushFailureObserved) == 0)
            {
                flushException = new InvalidOperationException("A background flush operation failed.", backgroundFailure);
            }

            if (workerWaitExceptions.Count > 0 || flushException != null)
            {
                var exceptions = new List<Exception>();
                exceptions.AddRange(workerWaitExceptions);
                if (flushException != null) exceptions.Add(flushException);
                throw new AggregateException("One or more errors occurred during FlushScheduler dispose.", exceptions);
            }
        }
        finally
        {
            DisposeSignals();
        }
    }

    private void RecordBackgroundFailure(Exception exception)
    {
        Interlocked.CompareExchange(ref _backgroundFailure, exception, null);
    }

    private void FlushForDispose()
    {
        if (IsCorrupted) return;
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages()) return;
        _wal.Synchronize(ctx => _pageManager.FlushDirtyPages(ctx), truncateLog: true);
    }

    private async Task FlushForDisposeAsync()
    {
        if (IsCorrupted) return;
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages()) return;
        await _wal.SynchronizeAsync(
            (ctx, ct) => _pageManager.FlushDirtyPagesAsync(ctx, ct),
            truncateLog: true,
            CancellationToken.None).ConfigureAwait(false);
    }

    private void AllowForegroundRetryAfterBackgroundFailure()
    {
        // Background failures are retained for Dispose/DisposeAsync. Runtime flush
        // requests must be able to retry instead of being permanently poisoned by
        // an earlier background attempt.
    }
}
