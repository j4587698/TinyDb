using TinyDb.Storage;
using System.Linq;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageManagerRestoreTests : IDisposable
{
    private readonly string _testDbPath;

    public PageManagerRestoreTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"pm_restore_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task RestorePage_Should_Write_To_Disk_And_Invalidate_Cache()
    {
        using var ds = new DiskStream(_testDbPath);
        using var pm = new PageManager(ds, 4096);
        
        var page = pm.NewPage(PageType.Data);
        page.WriteData(0, new byte[] { 1, 2, 3 });
        pm.SavePage(page);
        
        // Corrupt in memory
        page.WriteData(0, new byte[] { 9, 9, 9 });
        
        var restoreData = new byte[4096];
        var header = new PageHeader();
        header.Initialize(PageType.Data, page.PageID);
        var headerBytes = header.ToByteArray();
        Array.Copy(headerBytes, restoreData, headerBytes.Length);
        
        restoreData[PageHeader.Size] = 1;
        restoreData[PageHeader.Size + 1] = 2;
        restoreData[PageHeader.Size + 2] = 3;
        
        // Use reflection to call internal RestorePage
        var method = typeof(PageManager).GetMethod("RestorePage", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        method!.Invoke(pm, new object[] { page.PageID, restoreData });
        
        var restoredPage = pm.GetPage(page.PageID);
        var data = restoredPage.ReadData(0, 3);
        await Assert.That(data.SequenceEqual(new byte[] { 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task RestorePage_WhenDataIsShorter_ShouldPadToPageSize()
    {
        using var ds = new DiskStream(_testDbPath);
        using var pm = new PageManager(ds, 4096);

        var page = pm.NewPage(PageType.Data);
        pm.SavePage(page);

        var restoreData = new byte[PageHeader.Size + 3];
        var header = new PageHeader();
        header.Initialize(PageType.Data, page.PageID);
        var headerBytes = header.ToByteArray();
        Array.Copy(headerBytes, restoreData, headerBytes.Length);

        restoreData[PageHeader.Size] = 7;
        restoreData[PageHeader.Size + 1] = 8;
        restoreData[PageHeader.Size + 2] = 9;

        var method = typeof(PageManager).GetMethod("RestorePage", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        method!.Invoke(pm, new object[] { page.PageID, restoreData });

        var restored = pm.GetPage(page.PageID);
        var data = restored.ReadData(0, 3);
        await Assert.That(data.SequenceEqual(new byte[] { 7, 8, 9 })).IsTrue();
    }

    [Test]
    public async Task RestorePage_WhenDataIsLonger_ShouldThrow()
    {
        using var ds = new DiskStream(_testDbPath);
        using var pm = new PageManager(ds, 4096);

        var page = pm.NewPage(PageType.Data);
        pm.SavePage(page);

        var tooLong = new byte[4097];

        var method = typeof(PageManager).GetMethod("RestorePage", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        await Assert.That(() => method!.Invoke(pm, new object[] { page.PageID, tooLong }))
            .Throws<System.Reflection.TargetInvocationException>();
    }
}
