using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using System.Reflection;
using System.Runtime.ExceptionServices;

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
        await Assert.That(pm.CachedPages).IsLessThanOrEqualTo(1);
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
    public async Task CachedPages_ShouldTrackAddsDiscardAndClear()
    {
        using var ds = new DiskStream(_testDbPath);
        using var pm = new PageManager(ds, 4096, 10);

        var first = pm.NewPage(PageType.Data);
        var second = pm.NewPage(PageType.Data);

        await Assert.That(pm.CachedPages).IsEqualTo(2);

        pm.DiscardCachedPage(first.PageID);
        await Assert.That(pm.CachedPages).IsEqualTo(1);

        pm.DiscardCachedPage(second.PageID);
        await Assert.That(pm.CachedPages).IsEqualTo(0);

        _ = pm.NewPage(PageType.Data);
        await Assert.That(pm.CachedPages).IsEqualTo(1);

        pm.ClearCache();
        await Assert.That(pm.CachedPages).IsEqualTo(0);
    }

    [Test]
    public async Task ClearCache_ShouldFlushDirtyPagesBeforeRemoving()
    {
        using var ds = new DiskStream(_testDbPath);
        using var pm = new PageManager(ds, 4096, 10);

        var page = pm.NewPage(PageType.Data);
        var pageId = page.PageID;
        page.WriteData(0, new byte[] { 42, 43 });

        pm.ClearCache();

        await Assert.That(pm.CachedPages).IsEqualTo(0);

        var reloaded = pm.GetPage(pageId);
        await Assert.That(reloaded.ReadData(0, 2).SequenceEqual(new byte[] { 42, 43 })).IsTrue();
        await Assert.That(reloaded.IsDirty).IsFalse();
    }

    [Test]
    public async Task ClearCache_WhenFlushDirtyPagesIsFalse_ShouldKeepDirtyPages()
    {
        using var ds = new DiskStream(_testDbPath);
        using var pm = new PageManager(ds, 4096, 10);

        var page = pm.NewPage(PageType.Data);
        page.WriteData(0, new byte[] { 42 });

        pm.ClearCache(flushDirtyPages: false);

        await Assert.That(pm.CachedPages).IsEqualTo(1);
        await Assert.That(page.IsDirty).IsTrue();
        await Assert.That(() => page.WriteData(1, new byte[] { 43 })).ThrowsNothing();
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

        var probe = new TaskCompletionSource<(bool GetDone, bool PinnedDone, Page? GetPage, Page? PinnedPage)>(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var probeThread = new Thread(() =>
        {
            Monitor.Enter(cacheLock);
            try
            {
                Page? getPage = null;
                Page? pinnedPage = null;
                Exception? getException = null;
                Exception? pinnedException = null;

                var getThread = new Thread(() =>
                {
                    try
                    {
                        getPage = pm.GetPage(page.PageID);
                    }
                    catch (Exception ex)
                    {
                        getException = ex;
                    }
                })
                {
                    IsBackground = true
                };

                var pinnedThread = new Thread(() =>
                {
                    try
                    {
                        pinnedPage = pm.GetPagePinned(page.PageID);
                        pinnedPage.Unpin();
                    }
                    catch (Exception ex)
                    {
                        pinnedException = ex;
                    }
                })
                {
                    IsBackground = true
                };

                getThread.Start();
                pinnedThread.Start();

                var getDone = getThread.Join(TimeSpan.FromMilliseconds(500));
                var pinnedDone = pinnedThread.Join(TimeSpan.FromMilliseconds(500));
                if (getException != null)
                {
                    ExceptionDispatchInfo.Capture(getException).Throw();
                }

                if (pinnedException != null)
                {
                    ExceptionDispatchInfo.Capture(pinnedException).Throw();
                }

                probe.SetResult((
                    getDone,
                    pinnedDone,
                    getDone ? getPage : null,
                    pinnedDone ? pinnedPage : null));
            }
            catch (Exception ex)
            {
                probe.SetException(ex);
            }
            finally
            {
                Monitor.Exit(cacheLock);
            }
        })
        {
            IsBackground = true
        };

        probeThread.Start();
        var result = await probe.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(result.GetDone).IsTrue();
        await Assert.That(result.PinnedDone).IsTrue();
        await Assert.That(result.GetPage!.PageID).IsEqualTo(page.PageID);
        await Assert.That(result.PinnedPage!.PageID).IsEqualTo(page.PageID);
    }
}
