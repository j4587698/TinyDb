using System;
using System.Collections.Concurrent;
using System.Threading;

namespace TinyDb.Core;

internal sealed class CollectionState
{
    private int _cacheInitialized;
    public DataPageState PageState { get; } = new();
    
    // 抽象层：可以是内存索引，也可以是磁盘索引
    public IDocumentIndex Index { get; set; } = default!;
    
    // 追踪集合拥有的页面ID，优化全表扫描
    public ConcurrentDictionary<uint, byte> OwnedPages { get; } = new();
    
    public object CacheLock { get; } = new();
    public bool IsCacheInitialized => Volatile.Read(ref _cacheInitialized) == 1;
    public void MarkCacheInitialized() => Volatile.Write(ref _cacheInitialized, 1);
}

internal sealed class DataPageState
{
    public uint PageId;
    public readonly object SyncRoot = new();
}