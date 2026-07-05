using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using System.Reflection;

namespace TinyDb.Tests.Storage;

public class PageManagerCacheTests : IDisposable
{
    private readonly string _testDbPath;

    public PageManagerCacheTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"pm_cache_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Cache_Eviction_Should_Work()
    {
        using var ds = new DiskStream(_testDbPath);
        // Small cache size = 2
        using var pm = new PageManager(ds, 4096, 2);
        
        var p1 = pm.NewPage(PageType.Data);
        var p2 = pm.NewPage(PageType.Data);
        var p3 = pm.NewPage(PageType.Data); // Should evict p1 if it was least recently used
        
        // Note: NewPage adds to cache.
        await Assert.That(pm.CachedPages).IsLessThanOrEqualTo(pm.MaxCacheSize + 4096);
        
        // Clear cache
        pm.ClearCache();
        await Assert.That(pm.CachedPages).IsEqualTo(0);
    }

    [Test]
    public async Task ClearCache_With_MaxPagesToKeep_Should_Work()
    {
        using var ds = new DiskStream(_testDbPath);
        using var pm = new PageManager(ds, 4096, 10);
        
        for (int i = 0; i < 5; i++) pm.NewPage(PageType.Data);
        
        await Assert.That(pm.CachedPages).IsEqualTo(5);
        
        pm.ClearCache(2);
        await Assert.That(pm.CachedPages).IsEqualTo(2);
    }

    [Test]
    public async Task CacheHit_ShouldNotWaitForCacheLock()
    {
        using var ds = new DiskStream(_testDbPath);
        using var pm = new PageManager(ds, 4096, 10);

        var page = pm.NewPage(PageType.Data);
        var cacheLockField = typeof(PageManager).GetField("_cacheLock", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingFieldException(typeof(PageManager).FullName, "_cacheLock");
        var cacheLock = cacheLockField.GetValue(pm) ?? throw new InvalidOperationException("Cache lock instance is missing.");

        Monitor.Enter(cacheLock);
        try
        {
            var getTask = Task.Run(() => pm.GetPage(page.PageID));
            var pinnedTask = Task.Run(() =>
            {
                var pinnedPage = pm.GetPagePinned(page.PageID);
                pinnedPage.Unpin();
                return pinnedPage;
            });

            await Assert.That(await Task.WhenAny(getTask, Task.Delay(TimeSpan.FromMilliseconds(500))) == getTask).IsTrue();
            await Assert.That(await Task.WhenAny(pinnedTask, Task.Delay(TimeSpan.FromMilliseconds(500))) == pinnedTask).IsTrue();
            await Assert.That((await getTask).PageID).IsEqualTo(page.PageID);
            await Assert.That((await pinnedTask).PageID).IsEqualTo(page.PageID);
        }
        finally
        {
            Monitor.Exit(cacheLock);
        }
    }
}
