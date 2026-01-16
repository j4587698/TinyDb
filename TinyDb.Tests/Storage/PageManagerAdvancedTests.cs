using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

[NotInParallel]
public class PageManagerAdvancedTests
{
    private string _testFile = null!;
    private DiskStream _diskStream = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"pm_adv_{Guid.NewGuid():N}.db");
        _diskStream = new DiskStream(_testFile);
    }

    [After(Test)]
    public void Cleanup()
    {
        _diskStream.Dispose();
        if (File.Exists(_testFile)) File.Delete(_testFile);
    }

    [Test]
    public async Task PageManager_Async_Operations_ShouldWork()
    {
        using var pm = new PageManager(_diskStream, 8192, 100);
        
        // Async Get
        var page = await pm.GetPageAsync(1);
        await Assert.That(page).IsNotNull();
        await Assert.That(page.PageID).IsEqualTo(1u);
        
        page.WriteData(0, new byte[] { 1, 2, 3 });
        
        // Async Save
        await pm.SavePageAsync(page);
        
        // Async Flush
        await pm.FlushDirtyPagesAsync();
        
        // Invalidate and reload
        pm.ClearCache();
        var page2 = await pm.GetPageAsync(1);
        await Assert.That(page2.ReadData(0, 3).SequenceEqual(new byte[] { 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task PageManager_Statistics_ShouldWork()
    {
        using var pm = new PageManager(_diskStream, 8192, 100);
        await pm.GetPageAsync(1);
        await pm.GetPageAsync(2);
        
        var stats = pm.GetStatistics();
        await Assert.That(stats.CachedPages).IsEqualTo(2);
        await Assert.That(stats.ToString()).Contains("2/100 cached");
    }

    [Test]
    public async Task PageManager_FreeList_ShouldWork()
    {
        using var pm = new PageManager(_diskStream, 8192, 100);
        
        // 1. Allocate a page
        var page1 = pm.NewPage(PageType.Data);
        uint id1 = page1.PageID;
        
        // 2. Free it
        pm.FreePage(id1);
        await Assert.That(pm.FirstFreePageID).IsNotEqualTo(0u);
        
        // 3. Allocate again - should reuse id1
        var page2 = pm.NewPage(PageType.Data);
        await Assert.That(page2.PageID).IsEqualTo(id1);
        await Assert.That(pm.FirstFreePageID).IsEqualTo(0u);
    }
}
