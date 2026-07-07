using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;

namespace TinyDb.Core;

internal sealed partial class CollectionState
{
    private static readonly AsyncLocal<DocumentLockOwner?> CurrentDocumentLockOwner = new();

    private int _cacheInitialized;
    private readonly ConcurrentDictionary<BsonValue, KeyedDocumentLock> _documentLocks = new(BsonValueComparer.EqualityComparer);
    private readonly ConcurrentDictionary<uint, KeyedPageLock> _pageMutationLocks = new();

    public DataPageState PageState { get; } = new();
    public object CommitGate { get; } = new();

    // 抽象层：可以是内存索引，也可以是磁盘索引
    public IDocumentIndex Index { get; set; } = default!;

    // 追踪集合拥有的页面ID，优化全表扫描
    public ConcurrentDictionary<uint, byte> OwnedPages { get; } = new();

    public bool IsCacheInitialized => Volatile.Read(ref _cacheInitialized) == 1;
    public void MarkCacheInitialized() => Volatile.Write(ref _cacheInitialized, 1);

    private sealed class EmptyLockScope : IDisposable
    {
        public static EmptyLockScope Instance { get; } = new();

        public void Dispose()
        {
        }
    }


}

internal sealed class DataPageState
{
    public long PageId;
    public readonly object SyncRoot = new();
}
