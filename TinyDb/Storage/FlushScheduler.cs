using System;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;

namespace TinyDb.Storage;

/// <summary>
/// 后台刷盘调度器，负责协调 WAL 与数据页的刷新。
/// </summary>
public sealed class FlushScheduler : IDisposable
{
    private readonly PageManager _pageManager;
    private readonly WriteAheadLog _wal;
    private readonly TimeSpan _backgroundInterval;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task? _backgroundTask;
    private readonly object _flushLock = new();
    private bool _disposed;
    
    private TaskCompletionSource _journalBatchTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _journalRequests = 0;
    private bool _journalWorkerRunning = false;
    private Task? _journalWorkerTask;

    /// <summary>
    /// 初始化 FlushScheduler 类的新实例。
    /// </summary>
    /// <param name="pageManager">页面管理器。</param>
    /// <param name="wal">写前日志。</param>
    /// <param name="backgroundInterval">后台刷新间隔。</param>
    public FlushScheduler(PageManager pageManager, WriteAheadLog wal, TimeSpan backgroundInterval)
    {
        _pageManager = pageManager ?? throw new ArgumentNullException(nameof(pageManager));
        _wal = wal ?? throw new ArgumentNullException(nameof(wal));
        _backgroundInterval = backgroundInterval;

        if (_backgroundInterval > TimeSpan.Zero || _wal.IsEnabled)
        {
            _backgroundTask = Task.Run(BackgroundLoopAsync);
        }
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
            catch (OperationCanceledException) { }
            catch { }
        }
    }

    public async Task EnsureDurabilityAsync(WriteConcern concern, CancellationToken cancellationToken = default)
    {
        switch (concern)
        {
            case WriteConcern.None:
                return;
            case WriteConcern.Journaled:
                await EnsureJournalFlushAsync(cancellationToken).ConfigureAwait(false);
                return;
            case WriteConcern.Synced:
                await _wal.SynchronizeAsync(ct => _pageManager.FlushDirtyPagesAsync(ct), cancellationToken).ConfigureAwait(false);
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(concern), concern, null);
        }
    }

    public void EnsureDurability(WriteConcern concern)
    {
        switch (concern)
        {
            case WriteConcern.None:
                return;
            case WriteConcern.Journaled:
                EnsureJournalFlush();
                return;
            case WriteConcern.Synced:
                _wal.Synchronize(() => _pageManager.FlushDirtyPages());
                return;
            default:
                throw new ArgumentOutOfRangeException(nameof(concern), concern, null);
        }
    }

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return _wal.SynchronizeAsync(ct => _pageManager.FlushDirtyPagesAsync(ct), cancellationToken);
    }

    public void Flush()
    {
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
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(FlushScheduler));
            }

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
                tcsToComplete.TrySetException(ex);
            }
        }
    }

    public async Task FlushPendingAsync(CancellationToken cancellationToken = default)
    {
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages()) return;
        await _wal.SynchronizeAsync(ct => _pageManager.FlushDirtyPagesAsync(ct), cancellationToken).ConfigureAwait(false);
    }

    public void FlushPending()
    {
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages()) return;
        _wal.Synchronize(() => _pageManager.FlushDirtyPages());
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _cts.Cancel();
        try { _journalWorkerTask?.Wait(); } catch { }
        try { _backgroundTask?.Wait(); } catch { }
        try { Flush(); } catch { }
        _cts.Dispose();
    }
}
