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

    /// <summary>
    /// 初始化 FlushScheduler 类的新实例。
    /// </summary>
    /// <param name="pageManager">页面管理器。</param>
    /// <param name="wal">写前日志。</param>
    /// <param name="backgroundInterval">后台刷新间隔。</param>
    /// <param name="journalDelay">日志合并延迟。</param>
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

    /// <summary>
    /// 后台循环，定期执行刷新操作。
    /// </summary>
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

    /// <summary>
    /// 确保数据持久化。
    /// </summary>
    /// <param name="concern">写入关注级别。</param>
    /// <param name="cancellationToken">取消令牌。</param>
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

    /// <summary>
    /// 异步刷新所有挂起的数据和日志。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return _wal.SynchronizeAsync(ct => _pageManager.FlushDirtyPagesAsync(ct), cancellationToken);
    }

    /// <summary>
    /// 确保日志已刷新。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
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

    /// <summary>
    /// 批量刷新日志。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
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

    /// <summary>
    /// 刷新挂起的条目（如果存在）。
    /// </summary>
    /// <param name="cancellationToken">取消令牌。</param>
    public async Task FlushPendingAsync(CancellationToken cancellationToken = default)
    {
        if (!_wal.HasPendingEntries && !_pageManager.HasDirtyPages())
        {
            return;
        }

        await _wal.SynchronizeAsync(ct => _pageManager.FlushDirtyPagesAsync(ct), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// 释放资源。
    /// </summary>
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
