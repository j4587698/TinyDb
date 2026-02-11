using System.Collections.Concurrent;
using System.Reflection;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public sealed class PageManagerEvictionCoverageTests
{
    [Test]
    public async Task EvictLeastRecentlyUsed_WhenAllCandidatesPinned_ShouldNotEvict()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 1);

        var pinned = pm.NewPage(PageType.Data);
        pinned.Pin();

        _ = pm.NewPage(PageType.Data); // triggers eviction scan but cannot evict pinned page

        await Assert.That(pinned.PinCount).IsGreaterThan(0);
        pinned.Unpin();
    }

    [Test]
    public async Task EvictLeastRecentlyUsed_WhenLruContainsStaleEntry_ShouldSkipMissingCacheEntry()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 2);

        var p1 = pm.NewPage(PageType.Data);
        var stale = pm.NewPage(PageType.Data);

        var pageCacheField = typeof(PageManager).GetField("_pageCache", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(pageCacheField).IsNotNull();
        var pageCache = (ConcurrentDictionary<uint, Page>)pageCacheField!.GetValue(pm)!;

        pageCache.TryRemove(stale.PageID, out _);

        _ = pm.NewPage(PageType.Data); // fills cache back to maxCacheSize
        _ = pm.NewPage(PageType.Data); // triggers eviction scan and sees stale entry in LRU

        await Assert.That(pm.CachedPages).IsGreaterThan(0);
    }
}

