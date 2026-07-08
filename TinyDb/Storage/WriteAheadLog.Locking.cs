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

    private bool HasActiveWriteContext(WriteLockContext? context)
    {
        return context?.IsActiveFor(this) == true ||
               s_currentWriteContext?.IsActiveFor(this) == true;
    }


    private static void RunWithCurrentThreadWriteContext(WriteLockContext context, Action action)
    {
        var previousContext = s_currentWriteContext;
        s_currentWriteContext = context;
        try
        {
            action();
        }
        finally
        {
            s_currentWriteContext = previousContext;
        }
    }


    private static async Task RunWithCurrentThreadWriteContextAsync(WriteLockContext context, Func<Task> action)
    {
        var previousContext = s_currentWriteContext;
        s_currentWriteContext = context;
        Task task;
        try
        {
            task = action();
        }
        finally
        {
            s_currentWriteContext = previousContext;
        }

        await task.ConfigureAwait(false);
    }


    private void RunWithWriteLock(Action<WriteLockContext> action)
    {
        _mutex.Wait();
        var context = new WriteLockContext(this);
        try
        {
            RunWithCurrentThreadWriteContext(context, () => action(context));
        }
        finally
        {
            context.Deactivate();
            _mutex.Release();
        }
    }


    private async Task RunWithWriteLockAsync(Func<WriteLockContext, Task> action, CancellationToken cancellationToken)
    {
        await _mutex.WaitAsync(cancellationToken).ConfigureAwait(false);
        var context = new WriteLockContext(this);
        try
        {
            await RunWithCurrentThreadWriteContextAsync(context, () => action(context)).ConfigureAwait(false);
        }
        finally
        {
            context.Deactivate();
            _mutex.Release();
        }
    }

    internal bool RequiresBeforeImage => IsEnabled && _currentTransactionId.Value.HasValue;

    internal bool IsInTransactionScope => IsEnabled && _currentTransactionId.Value.HasValue;

    internal Action<uint, long>? DeferredTransactionPageLogged { get; set; }

    private sealed class PendingTransactionPage
    {
        public PendingTransactionPage(uint pageId, byte[]? beforeImage, byte[] afterImage, bool needsWalWrite)
        {
            PageId = pageId;
            BeforeImage = beforeImage;
            AfterImage = afterImage;
            NeedsWalWrite = needsWalWrite;
        }

        public uint PageId { get; }
        public byte[]? BeforeImage { get; }
        public byte[] AfterImage { get; set; }
        public bool NeedsWalWrite { get; set; }
    }

    private sealed class TransactionPageBuffer
    {
        private readonly Dictionary<uint, PendingTransactionPage> _pages = new();
        private readonly List<uint> _order = new();

        public TransactionPageBuffer(Guid transactionId)
        {
            TransactionId = transactionId;
        }

        public Guid TransactionId { get; }

        public void AddOrReplace(uint pageId, byte[]? beforeImage, byte[] afterImage, bool needsWalWrite)
        {
            if (!_pages.TryGetValue(pageId, out var pending))
            {
                _pages.Add(pageId, new PendingTransactionPage(pageId, beforeImage, afterImage, needsWalWrite));
                _order.Add(pageId);
                return;
            }

            pending.AfterImage = afterImage;
            pending.NeedsWalWrite = needsWalWrite;
        }

        public IEnumerable<(uint PageId, byte[] AfterImage, byte[]? BeforeImage)> GetPagesPendingWalWriteInFirstTouchOrder()
        {
            foreach (var pageId in _order)
            {
                var pending = _pages[pageId];
                if (!pending.NeedsWalWrite) continue;
                yield return (pending.PageId, pending.AfterImage, pending.BeforeImage);
            }
        }

        public IEnumerable<(uint PageId, byte[]? BeforeImage)> GetPagesInReverseFirstTouchOrder()
        {
            for (var i = _order.Count - 1; i >= 0; i--)
            {
                var pending = _pages[_order[i]];
                yield return (pending.PageId, pending.BeforeImage);
            }
        }
    }

}
