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
    private Task _journalFlushTask = Task.CompletedTask;
    private readonly TimeSpan _journalCoalesceDelay;
    private bool _disposed;

    public FlushScheduler(PageManager pageManager, WriteAheadLog wal, TimeSpan backgroundInterval, TimeSpan journalDelay)
    {
        _pageManager = pageManager ?? throw new ArgumentNullException(nameof(pageManager));
        _wal = wal ?? throw new ArgumentNullException(nameof(wal));
        _backgroundInterval = backgroundInterval;
        _journalCoalesceDelay = journalDelay;

        if ((_backgroundInterval > TimeSpan.Zero) || _wal.IsEnabled)
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
            catch (OperationCanceledException)
            {
                // 正常退出
            }
            catch
            {
            }
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

    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return _wal.SynchronizeAsync(ct => _pageManager.FlushDirtyPagesAsync(ct), cancellationToken);
    }

    private Task EnsureJournalFlushAsync(CancellationToken cancellationToken)
    {
        if (!_wal.IsEnabled)
        {
            return _pageManager.FlushDirtyPagesAsync(cancellationToken);
        }

        lock (_flushLock)
        {
            if (_journalFlushTask.IsCompleted)
            {
                _journalFlushTask = FlushJournalBatchAsync(cancellationToken);
            }
            return _journalFlushTask;
        }
    }

    private async Task FlushJournalBatchAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (_journalCoalesceDelay > TimeSpan.Zero && _journalCoalesceDelay != Timeout.InfiniteTimeSpan)
            {
                await Task.Delay(_journalCoalesceDelay, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // ignore cancellation during delay
        }

        try
        {
            await _wal.FlushLogAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            lock (_flushLock)
            {
                _journalFlushTask = Task.CompletedTask;
            }
        }
    }

    public async Task FlushPendingAsync(CancellationToken cancellationToken = default)
    {
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages())
        {
            return;
        }

        await _wal.SynchronizeAsync(ct => _pageManager.FlushDirtyPagesAsync(ct), cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cts.Cancel();
        try
        {
            _backgroundTask?.Wait();
        }
        catch (AggregateException)
        {
        }

        try
        {
            FlushAsync().GetAwaiter().GetResult();
        }
        catch
        {
        }

        _cts.Dispose();
    }
}
