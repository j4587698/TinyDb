using TinyDb.Storage;
using TUnit.Assertions;

namespace TinyDb.Tests.Storage;

public class PageManagerBranchCoverageTests
{
    [Test]
    public async Task GetStatistics_WhenFileIsEmpty_ShouldReportZeroUsedPages()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 10);

        var stats = pm.GetStatistics();

        await Assert.That(stats.TotalPages).IsEqualTo(0u);
        await Assert.That(stats.UsedPages).IsEqualTo(0u);
    }

    [Test]
    public async Task GetPageAsync_WhenPageIdIsZero_ShouldThrow()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 10);

        await Assert.That(async () => await pm.GetPageAsync(0))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task SavePage_WhenPageIsNull_ShouldThrow()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 10);

        await Assert.That(() => pm.SavePage(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task SavePageAsync_WhenPageIsNull_ShouldThrow()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 10);

        await Assert.That(() => pm.SavePageAsync(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task RestorePage_WhenPageIdIsZero_ShouldThrow()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 10);

        await Assert.That(() => pm.RestorePage(0, new byte[4096]))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task RestorePage_WhenPageDataIsNull_ShouldThrow()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 10);

        await Assert.That(() => pm.RestorePage(1, null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ClearCache_WhenKeepIsLessThanCacheCount_ShouldRemoveLruPages()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 10);

        pm.NewPage(PageType.Data);
        pm.NewPage(PageType.Data);

        await Assert.That(pm.CachedPages).IsEqualTo(2);

        pm.ClearCache(maxPagesToKeep: 1);

        await Assert.That(pm.CachedPages).IsEqualTo(1);
    }

    [Test]
    public async Task ClearCache_WhenKeepIsGreaterOrEqualToCacheCount_ShouldNotRemoveAnyPages()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 10);

        pm.NewPage(PageType.Data);
        pm.NewPage(PageType.Data);

        await Assert.That(pm.CachedPages).IsEqualTo(2);

        pm.ClearCache(maxPagesToKeep: 2);

        await Assert.That(pm.CachedPages).IsEqualTo(2);
    }

    [Test]
    public async Task SavePage_WhenPageDisposed_ShouldReturnWithoutThrowing()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 10);

        var page = pm.NewPage(PageType.Data);
        page.Dispose();

        pm.SavePage(page);

        await Assert.That(pm.CachedPages).IsEqualTo(1);
    }

    [Test]
    public async Task SavePageAsync_WhenPageDisposed_ShouldReturnWithoutThrowing()
    {
        using var ds = new MockDiskStream();
        using var pm = new PageManager(ds, pageSize: 4096, maxCacheSize: 10);

        var page = pm.NewPage(PageType.Data);
        page.Dispose();

        await pm.SavePageAsync(page);

        await Assert.That(pm.CachedPages).IsEqualTo(1);
    }
}
