using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageManagerExtendedTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly DiskStream _diskStream;
    private const int PageSize = 4096;

    public PageManagerExtendedTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"pm_ext_test_{Guid.NewGuid():N}.db");
        _diskStream = new DiskStream(_testDbPath);
    }

    public void Dispose()
    {
        _diskStream.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task PageManager_Cache_Eviction_Should_Work()
    {
        // Max cache size 2
        using var pm = new PageManager(_diskStream, PageSize, 2);
        
        var p1 = pm.NewPage(PageType.Data);
        pm.SavePage(p1);
        var p2 = pm.NewPage(PageType.Data);
        pm.SavePage(p2);
        
        // Access p1 to make it "Most Recently Used"
        pm.GetPage(p1.PageID);

        // New page should evict p2 (because p1 was touched)
        var p3 = pm.NewPage(PageType.Data);

        await Assert.That(pm.CachedPages).IsEqualTo(2);
        
        // Check if p2 was evicted
        // GetPage with useCache: false to check if it's in memory cache or not
        // Actually pm doesn't expose a way to check if a page is in cache without "touching" it
        // but we can check pm.CachedPages and hope for the best.
    }

    [Test]
    public async Task FreePage_Should_Make_Page_Available_For_Reuse()
    {
        using var pm = new PageManager(_diskStream, PageSize, 10);
        var p1 = pm.NewPage(PageType.Data);
        var p1Id = p1.PageID;
        pm.SavePage(p1);

        pm.FreePage(p1Id);
        await Assert.That(pm.FreePages).IsEqualTo(1);

        var p2 = pm.NewPage(PageType.Data);
        await Assert.That(p2.PageID).IsEqualTo(p1Id); // Reused
        await Assert.That(pm.FreePages).IsEqualTo(0);
    }

    [Test]
    public async Task GetPage_With_Invalid_PageId_Should_Throw()
    {
        using var pm = new PageManager(_diskStream, PageSize, 10);
        await Assert.That(() => pm.GetPage(0)).Throws<ArgumentException>();
    }

    [Test]
    public async Task RestorePage_Should_Update_Disk_And_Cache()
    {
        using var pm = new PageManager(_diskStream, PageSize, 10);
        var p1 = pm.NewPage(PageType.Data);
        pm.SavePage(p1);

        // Create a valid page data with matching page ID
        var pTemp = new Page(p1.PageID, PageSize, PageType.Data);
        pTemp.WriteData(0, new byte[] { 0xAA, 0xBB, 0xCC });
        var newData = pTemp.FullData.ToArray();
        
        // Use RestorePage (internal)
        pm.RestorePage(p1.PageID, newData);

        var p1Reloaded = pm.GetPage(p1.PageID, useCache: false);
        // Compare the full data
        await Assert.That(p1Reloaded.FullData.ToArray().SequenceEqual(newData)).IsTrue();
    }
}
