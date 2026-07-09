using System.Collections.Concurrent;
using System.Reflection;
using TinyDb.Storage;
using TinyDb.Utils;
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

        var pinnedPages = new List<Page>();
        try
        {
            for (var i = 0; i < pm.MaxCacheSize + 4096; i++)
            {
                var page = pm.NewPage(PageType.Data);
                page.Pin();
                pinnedPages.Add(page);
            }

            await Assert.That(() => pm.NewPage(PageType.Data))
                .Throws<InvalidOperationException>();
            await Assert.That(pm.CachedPages).IsLessThanOrEqualTo(pm.MaxCacheSize + 4096);
        }
        finally
        {
            foreach (var page in pinnedPages)
            {
                page.Unpin();
            }
        }
    }

    [Test]
    public async Task PageManagerLru_WhenOverflowLimitReached_ShouldRejectUntrackedOverflowInsteadOfAutoEvicting()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 1);
        var pinnedPages = new List<Page>();
        using var overflowPage = new Page(uint.MaxValue, 4096, PageType.Data);

        try
        {
            for (var i = 0; i < pm.MaxCacheSize + 4096; i++)
            {
                var page = pm.NewPage(PageType.Data);
                page.Pin();
                pinnedPages.Add(page);
            }

            var lruField = typeof(PageManager).GetField("_lruCache", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(lruField).IsNotNull();
            var lru = (LRUCache<uint, Page>)lruField!.GetValue(pm)!;

            await Assert.That(() => lru.Put(overflowPage.PageID, overflowPage)).Throws<InvalidOperationException>();
            await Assert.That(lru.Count).IsEqualTo(pm.CachedPages);
        }
        finally
        {
            foreach (var page in pinnedPages)
            {
                page.Unpin();
            }
        }
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
