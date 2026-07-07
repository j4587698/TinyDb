using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class PageManager
{
    public void RegisterWAL(Func<long, Task> flushLogToLsnAsync)
    {
        if (flushLogToLsnAsync == null) throw new ArgumentNullException(nameof(flushLogToLsnAsync));
        _flushLogToLsnAsync = (lsn, _, _) => flushLogToLsnAsync(lsn);
    }

    public void RegisterWAL(Func<long, CancellationToken, Task> flushLogToLsnAsync)
    {
        if (flushLogToLsnAsync == null) throw new ArgumentNullException(nameof(flushLogToLsnAsync));
        _flushLogToLsnAsync = (lsn, _, ct) => flushLogToLsnAsync(lsn, ct);
    }

    internal void RegisterWAL(Func<long, WriteAheadLog.WriteLockContext?, CancellationToken, Task> flushLogToLsnAsync)
    {
        _flushLogToLsnAsync = flushLogToLsnAsync ?? throw new ArgumentNullException(nameof(flushLogToLsnAsync));
    }

    public void RegisterWAL(Action<long> flushLogToLsn)
    {
        if (flushLogToLsn == null) throw new ArgumentNullException(nameof(flushLogToLsn));

        _flushLogToLsn = (lsn, _) => flushLogToLsn(lsn);
    }

    public void RegisterWAL(Action<Page> appendLogPage, Action<long> flushLogToLsn)
    {
        if (appendLogPage == null) throw new ArgumentNullException(nameof(appendLogPage));
        RegisterWAL((page, _) => appendLogPage(page), flushLogToLsn, null);
    }

    public void RegisterWAL(Action<Page, byte[]?> appendLogPage, Action<long> flushLogToLsn, Func<bool>? requiresBeforeImage = null)
    {
        if (appendLogPage == null) throw new ArgumentNullException(nameof(appendLogPage));
        if (flushLogToLsn == null) throw new ArgumentNullException(nameof(flushLogToLsn));

        _appendLogPage = (page, beforeImage, _) => appendLogPage(page, beforeImage);
        _appendDeferredLogPage = (page, beforeImage, _) => appendLogPage(page, beforeImage);
        _flushLogToLsn = (lsn, _) => flushLogToLsn(lsn);
        _requiresWalBeforeImage = requiresBeforeImage;
    }

    internal void RegisterWAL(
        Action<Page, byte[]?, WriteAheadLog.WriteLockContext?> appendLogPage,
        Action<long, WriteAheadLog.WriteLockContext?> flushLogToLsn,
        Func<bool>? requiresBeforeImage = null)
    {
        _appendLogPage = appendLogPage ?? throw new ArgumentNullException(nameof(appendLogPage));
        _appendDeferredLogPage = appendLogPage;
        _flushLogToLsn = flushLogToLsn ?? throw new ArgumentNullException(nameof(flushLogToLsn));
        _requiresWalBeforeImage = requiresBeforeImage;
    }

    internal void RegisterDeferredWAL(Action<Page, byte[]?> appendDeferredLogPage)
    {
        if (appendDeferredLogPage == null) throw new ArgumentNullException(nameof(appendDeferredLogPage));
        _appendDeferredLogPage = (page, beforeImage, _) => appendDeferredLogPage(page, beforeImage);
    }

    internal void RegisterDeferredWAL(Action<Page, byte[]?, WriteAheadLog.WriteLockContext?> appendDeferredLogPage)
    {
        _appendDeferredLogPage = appendDeferredLogPage ?? throw new ArgumentNullException(nameof(appendDeferredLogPage));
    }

    internal void MarkDeferredWalPageLogged(uint pageId, long lsn)
    {
        ThrowIfDisposed();

        var hadDeferredWrite = _deferredWalPages.TryGetValue(pageId, out _);
        if (hadDeferredWrite)
        {
            _deferredWalPages[pageId] = lsn;
        }

        if (_pageCache.TryGetValue(pageId, out var page) && (hadDeferredWrite || page.Header.LSN < 0))
        {
            page.UpdateLsnForWal(lsn);
            page.UpdateChecksum();
            if (page.IsDirty)
            {
                TrackDirtyPage(page);
            }
        }
    }

    internal void RegisterWAL(Func<Page, byte[]?, CancellationToken, Task> appendLogPageAsync)
    {
        if (appendLogPageAsync == null) throw new ArgumentNullException(nameof(appendLogPageAsync));
        _appendLogPageAsync = (page, beforeImage, _, ct) => appendLogPageAsync(page, beforeImage, ct);
    }

    internal void RegisterWAL(Func<Page, byte[]?, WriteAheadLog.WriteLockContext?, CancellationToken, Task> appendLogPageAsync)
    {
        _appendLogPageAsync = appendLogPageAsync ?? throw new ArgumentNullException(nameof(appendLogPageAsync));
    }

    /// <summary>
}
