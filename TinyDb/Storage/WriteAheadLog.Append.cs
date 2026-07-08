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

    public async Task AppendPageAsync(Page page, CancellationToken cancellationToken = default)
    {
        await AppendPageAsync(page, beforeImage: null, writeContext: null, cancellationToken).ConfigureAwait(false);
    }


    internal async Task AppendPageAsync(Page page, byte[]? beforeImage, CancellationToken cancellationToken = default)
    {
        await AppendPageAsync(page, beforeImage, writeContext: null, cancellationToken).ConfigureAwait(false);
    }


    internal async Task AppendPageAsync(
        Page page,
        byte[]? beforeImage,
        WriteLockContext? writeContext,
        CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;
        if (page == null) throw new ArgumentNullException(nameof(page));

        if (HasActiveWriteContext(writeContext))
        {
            await AppendPageCoreAsync(page, beforeImage, cancellationToken).ConfigureAwait(false);
            return;
        }

        await RunWithWriteLockAsync(
            _ => AppendPageCoreAsync(page, beforeImage, cancellationToken),
            cancellationToken).ConfigureAwait(false);
    }


    private async Task AppendPageCoreAsync(Page page, byte[]? beforeImage, CancellationToken cancellationToken)
    {
        if (TryGetCurrentTransactionContext(out var transactionContext))
        {
            var transactionId = transactionContext.TransactionId;
            var afterImage = PreparePageRecord(page);
            TrackTransactionPage(transactionContext, page.PageID, beforeImage, afterImage, needsWalWrite: false);
            var transactionData = CreateTransactionPageRecord(transactionId, beforeImage, afterImage);
            await WriteEntryAsync(EntryTypeTransactionPage, page.PageID, transactionData, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            var data = PreparePageRecord(page);
            await WriteEntryAsync(EntryTypePage, page.PageID, data, cancellationToken).ConfigureAwait(false);
        }

        SetHasPendingEntries(true);
    }


    public void AppendPage(Page page)
    {
        AppendPage(page, beforeImage: null);
    }


    internal void AppendPageDeferred(Page page, byte[]? beforeImage)
    {
        if (!IsEnabled) return;
        if (page == null) throw new ArgumentNullException(nameof(page));

        if (TryGetCurrentTransactionContext(out var transactionContext))
        {
            BufferDeferredTransactionPage(transactionContext, page, beforeImage);
            SetHasPendingEntries(true);
            return;
        }

        AppendPage(page, beforeImage);
    }


    public void AppendPage(Page page, byte[]? beforeImage)
    {
        AppendPage(page, beforeImage, writeContext: null);
    }


    internal void AppendPage(Page page, byte[]? beforeImage, WriteLockContext? writeContext)
    {
        if (!IsEnabled) return;
        if (page == null) throw new ArgumentNullException(nameof(page));

        if (HasActiveWriteContext(writeContext))
        {
            AppendPageCore(page, beforeImage);
            return;
        }

        RunWithWriteLock(_ => AppendPageCore(page, beforeImage));
    }


    private void AppendPageCore(Page page, byte[]? beforeImage)
    {
        if (TryGetCurrentTransactionContext(out var transactionContext))
        {
            var transactionId = transactionContext.TransactionId;
            var afterImage = PreparePageRecord(page);
            TrackTransactionPage(transactionContext, page.PageID, beforeImage, afterImage, needsWalWrite: false);
            var data = CreateTransactionPageRecord(transactionId, beforeImage, afterImage);
            WriteEntry(EntryTypeTransactionPage, page.PageID, data);
        }
        else
        {
            var data = PreparePageRecord(page);
            WriteEntry(EntryTypePage, page.PageID, data);
        }

        SetHasPendingEntries(true);
    }


    private static void BufferDeferredTransactionPage(TransactionContext transactionContext, Page page, byte[]? beforeImage)
    {
        if (!transactionContext.IsActive)
        {
            throw new InvalidOperationException("WAL transaction page buffer mismatch.");
        }

        page.UpdateLsnForWal(PendingDeferredTransactionLsn);
        transactionContext.Pages.AddOrReplace(page.PageID, beforeImage, page.Snapshot(includeUnusedTail: true), needsWalWrite: true);
    }


    private static void TrackTransactionPage(
        TransactionContext transactionContext,
        uint pageId,
        byte[]? beforeImage,
        byte[] afterImage,
        bool needsWalWrite)
    {
        if (!transactionContext.IsActive)
        {
            throw new InvalidOperationException("WAL transaction page buffer mismatch.");
        }

        transactionContext.Pages.AddOrReplace(pageId, beforeImage, afterImage, needsWalWrite);
    }

}
