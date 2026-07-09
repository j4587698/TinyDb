using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Storage;

namespace TinyDb.Tests.Utils;

internal static class UnsafeAccessors
{
    internal static class FlushSchedulerAccessor
    {
        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_journalBatchTcs")]
        internal static extern ref TaskCompletionSource JournalBatchTcs(FlushScheduler scheduler);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_journalRequests")]
        internal static extern ref int JournalRequests(FlushScheduler scheduler);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_syncedRequests")]
        internal static extern ref int SyncedRequests(FlushScheduler scheduler);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_syncedWorkerRunning")]
        internal static extern ref bool SyncedWorkerRunning(FlushScheduler scheduler);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_journalSignal")]
        internal static extern ref SemaphoreSlim JournalSignal(FlushScheduler scheduler);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_syncedSignal")]
        internal static extern ref SemaphoreSlim SyncedSignal(FlushScheduler scheduler);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_cts")]
        internal static extern ref CancellationTokenSource Cts(FlushScheduler scheduler);
    }

    internal static class WriteAheadLogAccessor
    {
        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_mutex")]
        internal static extern ref SemaphoreSlim Mutex(WriteAheadLog wal);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_flushedLSN")]
        internal static extern ref long FlushedLsn(WriteAheadLog wal);
    }

    internal static class PageManagerAccessor
    {
        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_stateLock")]
        internal static extern ref object StateLock(PageManager pageManager);
    }
}
