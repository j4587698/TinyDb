using System;
using System.Linq;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

public class PageFullTests
{
    [Test]
    public async Task Page_Checksum_ShouldWork()
    {
        var page = new Page(1, 4096);
        page.WriteData(0, new byte[] { 1, 2, 3 });
        page.UpdateChecksum();
        
        await Assert.That(page.VerifyIntegrity()).IsTrue();
        
        // Manual corruption of data
        var raw = page.Snapshot();
        raw[PageHeader.Size] = 0xFF;
        
        var corruptedPage = new Page(1, raw);
        await Assert.That(corruptedPage.VerifyIntegrity()).IsFalse();
    }

    [Test]
    public async Task Page_DataManagement_ShouldWork()
    {
        var page = new Page(1, 4096, PageType.Data);
        var data = new byte[] { 10, 20, 30 };
        
        page.WriteData(10, data);
        var back = page.ReadData(10, 3);
        await Assert.That(back.SequenceEqual(data)).IsTrue();
        
        await Assert.That(page.DataSize).IsGreaterThan(0);
        
        // Manually update stats because WriteData doesn't do it automatically
        // Total data written is at offset 10 with length 3, so used bytes is at least 13
        // Assuming DataSize is roughly 4096 - HeaderSize
        var usedBytes = 13;
        var freeBytes = (ushort)(page.DataSize - usedBytes);
        page.UpdateStats(freeBytes, 1);

        // Test Snapshot(false)
        var compact = page.Snapshot(false);
        // compact size should be Header + Data used (depends on implementation)
        await Assert.That(compact.Length).IsLessThan(4096);
    }

    [Test]
    public async Task Page_Update_ShouldWork()
    {
        var page = new Page(1, 4096, PageType.Data);
        
        var header = new PageHeader { PageID = 1, PageType = PageType.Index };
        page.UpdateHeader(header);
        await Assert.That(page.Header.PageType).IsEqualTo(PageType.Index);
        
        page.UpdateStats(500, 10);
        await Assert.That(page.Header.FreeBytes).IsEqualTo((ushort)500);
        await Assert.That(page.Header.ItemCount).IsEqualTo((ushort)10);
        
        page.SetLinks(5, 6);
        await Assert.That(page.Header.PrevPageID).IsEqualTo(5u);
        await Assert.That(page.Header.NextPageID).IsEqualTo(6u);
    }

    [Test]
    public async Task Page_ClearData_ShouldWork()
    {
        var page = new Page(1, 4096, PageType.Data);
        page.WriteData(0, new byte[] { 1, 2, 3 });
        page.ClearData();
        
        var back = page.ReadData(0, 3);
        await Assert.That(back.All(b => b == 0)).IsTrue();
    }
}
