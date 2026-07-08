using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class WriteAheadLog
{

    public async Task FlushToLSNAsync(long targetLSN, CancellationToken cancellationToken = default)
    {
        await FlushToLSNAsync(targetLSN, writeContext: null, cancellationToken).ConfigureAwait(false);
    }


    internal async Task FlushToLSNAsync(
        long targetLSN,
        WriteLockContext? writeContext,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || targetLSN < ReadFlushedLSN()) return;

        if (HasActiveWriteContext(writeContext))
        {
            await FlushToLSNCoreAsync(targetLSN, cancellationToken).ConfigureAwait(false);
            return;
        }

        await RunWithWriteLockAsync(async _ =>
        {
            await FlushToLSNCoreAsync(targetLSN, cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }


    public void FlushToLSN(long targetLSN)
    {
        FlushToLSN(targetLSN, writeContext: null);
    }


    internal void FlushToLSN(long targetLSN, WriteLockContext? writeContext)
    {
        if (!IsEnabled || targetLSN < ReadFlushedLSN()) return;

        if (HasActiveWriteContext(writeContext))
        {
            FlushToLSNCore(targetLSN);
            return;
        }

        RunWithWriteLock(_ => FlushToLSNCore(targetLSN));
    }


    private void FlushToLSNCore(long targetLSN)
    {
        if (targetLSN >= ReadFlushedLSN())
        {
            _stream!.Flush(true);
            SetFlushedLSN(_stream.Position);
        }
    }


    private async Task FlushToLSNCoreAsync(long targetLSN, CancellationToken cancellationToken)
    {
        if (targetLSN >= ReadFlushedLSN())
        {
            await _stream!.FlushAsync(cancellationToken).ConfigureAwait(false);
            SetFlushedLSN(_stream.Position);
        }
    }


    public async Task FlushLogAsync(CancellationToken cancellationToken = default)
    {
        await FlushLogAsync(writeContext: null, cancellationToken).ConfigureAwait(false);
    }


    internal async Task FlushLogAsync(
        WriteLockContext? writeContext,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled || !HasPendingEntriesCore) return;

        if (HasActiveWriteContext(writeContext))
        {
            await FlushLogCoreAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        await RunWithWriteLockAsync(async _ =>
        {
            await FlushLogCoreAsync(cancellationToken).ConfigureAwait(false);
        }, cancellationToken).ConfigureAwait(false);
    }


    public void FlushLog()
    {
        FlushLog(writeContext: null);
    }


    internal void FlushLog(WriteLockContext? writeContext)
    {
        FlushLog(writeContext, CancellationToken.None);
    }


    internal void FlushLog(WriteLockContext? writeContext, CancellationToken cancellationToken)
    {
        if (!IsEnabled || !HasPendingEntriesCore) return;
        cancellationToken.ThrowIfCancellationRequested();

        if (HasActiveWriteContext(writeContext))
        {
            FlushLogCore();
            return;
        }

        RunWithWriteLock(_ => FlushLogCore(), cancellationToken);
    }


    private void FlushLogCore()
    {
        if (HasPendingEntriesCore)
        {
            _stream!.Flush(true);
            SetFlushedLSN(_stream.Position);
        }
    }


    private async Task FlushLogCoreAsync(CancellationToken cancellationToken)
    {
        if (HasPendingEntriesCore)
        {
            await _stream!.FlushAsync(cancellationToken).ConfigureAwait(false);
            SetFlushedLSN(_stream.Position);
        }
    }


    public void Synchronize(Action flushData) => Synchronize(flushData, truncateLog: true);


    internal void Synchronize(Action flushData, bool truncateLog)
    {
        if (flushData == null) throw new ArgumentNullException(nameof(flushData));
        Synchronize(_ => flushData(), truncateLog);
    }


    internal void Synchronize(Action<WriteLockContext> flushData, bool truncateLog)
    {
        Synchronize(flushData, truncateLog, CancellationToken.None);
    }


    internal void Synchronize(Action<WriteLockContext> flushData, bool truncateLog, CancellationToken cancellationToken)
    {
        if (flushData == null) throw new ArgumentNullException(nameof(flushData));
        cancellationToken.ThrowIfCancellationRequested();

        if (!IsEnabled)
        {
            var disabledContext = new WriteLockContext(this);
            try
            {
                RunWithCurrentThreadWriteContext(disabledContext, () => flushData(disabledContext));
            }
            finally
            {
                disabledContext.Deactivate();
            }
            return;
        }

        _mutex.Wait(cancellationToken);
        var context = new WriteLockContext(this);
        try
        {
            var stream = _stream!;

            if (HasPendingEntriesCore)
            {
                stream.Flush(true);
                SetFlushedLSN(stream.Position);
            }

            RunWithCurrentThreadWriteContext(context, () => flushData(context));

            if (HasPendingEntriesCore)
            {
                if (truncateLog || stream.Length >= DeferredTruncateThresholdBytes)
                {
                    stream.SetLength(0);
                    stream.Seek(0, SeekOrigin.End);
                    stream.Flush(true);
                }

                SetHasPendingEntries(false);
                SetFlushedLSN(stream.Position);
            }
        }
        finally
        {
            context.Deactivate();
            _mutex.Release();
        }
    }


    public Task SynchronizeAsync(Func<CancellationToken, Task> flushDataAsync, CancellationToken cancellationToken = default)
    {
        return SynchronizeAsync(flushDataAsync, truncateLog: true, cancellationToken);
    }


    internal async Task SynchronizeAsync(
        Func<CancellationToken, Task> flushDataAsync,
        bool truncateLog,
        CancellationToken cancellationToken = default)
    {
        if (flushDataAsync == null) throw new ArgumentNullException(nameof(flushDataAsync));
        await SynchronizeAsync((_, ct) => flushDataAsync(ct), truncateLog, cancellationToken).ConfigureAwait(false);
    }

    internal async Task SynchronizeAsync(
        Func<WriteLockContext, CancellationToken, Task> flushDataAsync,
        bool truncateLog,
        CancellationToken cancellationToken = default)
    {
        if (flushDataAsync == null) throw new ArgumentNullException(nameof(flushDataAsync));

        if (!IsEnabled)
        {
            var disabledContext = new WriteLockContext(this);
            try
            {
                await RunWithCurrentThreadWriteContextAsync(
                    disabledContext,
                    () => flushDataAsync(disabledContext, cancellationToken)).ConfigureAwait(false);
            }
            finally
            {
                disabledContext.Deactivate();
            }
            return;
        }

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        var context = new WriteLockContext(this);
        try
        {
            var stream = _stream!;

            if (HasPendingEntriesCore)
            {
                stream.Flush(true);
                SetFlushedLSN(stream.Position);
            }

            await RunWithCurrentThreadWriteContextAsync(
                context,
                () => flushDataAsync(context, cancellationToken)).ConfigureAwait(false);

            if (HasPendingEntriesCore)
            {
                if (truncateLog || stream.Length >= DeferredTruncateThresholdBytes)
                {
                    stream.SetLength(0);
                    stream.Seek(0, SeekOrigin.End);
                    stream.Flush(true);
                }

                SetHasPendingEntries(false);
                SetFlushedLSN(stream.Position);
            }
        }
        finally
        {
            context.Deactivate();
            _mutex.Release();
        }
    }


    public async Task TruncateAsync(CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;

        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var stream = _stream!;
            stream.SetLength(0);
            stream.Seek(0, SeekOrigin.End);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            SetHasPendingEntries(false);
            SetFlushedLSN(stream.Position);
        }
        finally
        {
            _mutex.Release();
        }
    }


    public void Truncate()
    {
        if (!IsEnabled) return;

        _mutex.Wait();
        try
        {
            var stream = _stream!;
            stream.SetLength(0);
            stream.Seek(0, SeekOrigin.End);
            stream.Flush(true);
            SetHasPendingEntries(false);
            SetFlushedLSN(stream.Position);
        }
        finally
        {
            _mutex.Release();
        }
    }

}
