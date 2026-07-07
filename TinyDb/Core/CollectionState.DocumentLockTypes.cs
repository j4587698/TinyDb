using System.Collections.Generic;
using System.Threading;
using TinyDb.Bson;

namespace TinyDb.Core;

internal sealed partial class CollectionState
{
    private sealed class KeyedDocumentLock
    {
        public KeyedDocumentLock(BsonValue key)
        {
            Key = key;
        }

        public BsonValue Key { get; }
        public SemaphoreSlim Semaphore { get; } = new(1, 1);
        public object ReferenceSyncRoot { get; } = new();
        public int ReferenceCount { get; set; }
        public bool IsRemoved { get; set; }
        public DocumentLockOwner? Owner { get; set; }
        public int OwnerThreadId { get; set; }
        public int? OwnerTaskId { get; set; }
        public int OwnerDepth { get; set; }
    }

    private sealed class DocumentLockOwner
    {
        public DocumentLockOwner(int? taskId)
        {
            CreatorTaskId = taskId;
        }

        public int? CreatorTaskId { get; }
    }

    private sealed class BsonValueSortComparer : IComparer<BsonValue>
    {
        public static BsonValueSortComparer Instance { get; } = new();

        public int Compare(BsonValue? x, BsonValue? y)
        {
            return BsonValueComparer.Compare(x, y);
        }
    }

}
