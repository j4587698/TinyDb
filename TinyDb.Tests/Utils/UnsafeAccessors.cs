using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Storage;

namespace TinyDb.Tests.Utils;

internal static class UnsafeAccessors
{
    internal static class TinyDbEngineAccessor
    {
        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_collectionStates")]
        internal static extern ref ConcurrentDictionary<string, CollectionState> CollectionStates(TinyDbEngine engine);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_pageManager")]
        internal static extern ref PageManager PageManager(TinyDbEngine engine);
    }

    internal static class FlushSchedulerAccessor
    {
        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_journalBatchTcs")]
        internal static extern ref TaskCompletionSource JournalBatchTcs(FlushScheduler scheduler);

        [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_journalRequests")]
        internal static extern ref int JournalRequests(FlushScheduler scheduler);

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
}

