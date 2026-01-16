using System;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

public class PageExtendedTests
{
    [Test]
    public async Task Page_Constructor_Validations()
    {
        await Assert.That(() => new Page(1, 10)) // Too small
            .Throws<ArgumentException>();
            
        await Assert.That(() => new Page(1, null!))
            .Throws<ArgumentNullException>();
            
        await Assert.That(() => new Page(1, new byte[10])) // Too small data
            .Throws<ArgumentException>();
            
        // Valid page from data
        var p1 = new Page(1, 4096);
        p1.UpdateStats(100, 5);
        var data = p1.Snapshot(true);
        
        var p2 = new Page(1, data);
        await Assert.That((int)p2.PageID).IsEqualTo(1);
        await Assert.That(p2.PageSize).IsEqualTo(4096);
        
        // ID mismatch
        await Assert.That(() => new Page(2, data))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Page_Methods_Coverage()
    {
        using var page = new Page(10, 4096, PageType.Data);
        
        // Clone
        using var clone = page.Clone();
        await Assert.That((int)clone.PageID).IsEqualTo((int)page.PageID);
        await Assert.That(clone.PageType).IsEqualTo(page.PageType);
        
        // Snapshot
        var snapFull = page.Snapshot(true);
        await Assert.That(snapFull).Count().IsEqualTo(4096);
        
        var snapPartial = page.Snapshot(false);
        // DataSize is approx 4096 - HeaderSize. FreeBytes is 4096-HeaderSize initially.
        // If we update stats...
        page.UpdateStats(100, 2); // Free 100 bytes. Used = DataSize - 100.
        // Snapshot should be Header + Used.
        
        // VerifyIntegrity
        page.UpdateChecksum();
        await Assert.That(page.VerifyIntegrity()).IsTrue();
        
        // UpdateChecksum
        page.UpdateChecksum();
        await Assert.That(page.VerifyIntegrity()).IsTrue();
        
        // GetUsageInfo
        var info = page.GetUsageInfo();
        await Assert.That((int)info.PageID).IsEqualTo(10);
        await Assert.That(info.PageType).IsEqualTo(PageType.Data);
        await Assert.That(info.ItemCount).IsEqualTo(2);
        
        // PageUsageInfo ToString
        var str = info.ToString();
        await Assert.That(str).Contains("Page[10]");
        await Assert.That(str).Contains("Data");
        
        // Write/Read Data
        var data = new byte[] { 1, 2, 3 };
        page.WriteData(0, data);
        var read = page.ReadData(0, 3);
        await Assert.That(read.SequenceEqual(data)).IsTrue();
        
        // Read out of bounds
        await Assert.That(page.ReadData(-1, 1)).IsEmpty();
        await Assert.That(page.ReadData(0, -1)).IsEmpty();
        await Assert.That(page.ReadData(10000, 1)).IsEmpty();
        await Assert.That(page.ReadData(0, 10000)).IsEmpty();
        
        // GetDataSpan
        var span = page.GetDataSpan(0, 3);
        await Assert.That(span.ToArray().SequenceEqual(data)).IsTrue();
        
        await Assert.That(() => page.GetDataSpan(-1, 1)).Throws<ArgumentOutOfRangeException>();
        
        // ClearData
        page.ClearData();
        await Assert.That((int)page.Header.ItemCount).IsEqualTo(0);
        await Assert.That(page.PageType).IsEqualTo(PageType.Empty);
    }
    
    [Test]
    public async Task Page_UpdateMethods()
    {
        using var page = new Page(5, 4096);
        
        // UpdateHeader
        var newHeader = new PageHeader();
        newHeader.Initialize(PageType.Index, 5);
        page.UpdateHeader(newHeader);
        await Assert.That(page.PageType).IsEqualTo(PageType.Index);
        await Assert.That(page.IsDirty).IsTrue();
        
        // UpdatePageType
        page.MarkClean();
        page.UpdatePageType(PageType.Data);
        await Assert.That(page.PageType).IsEqualTo(PageType.Data);
        await Assert.That(page.IsDirty).IsTrue();
        
        // SetLinks
        page.MarkClean();
        page.SetLinks(4, 6);
        await Assert.That((int)page.Header.PrevPageID).IsEqualTo(4);
        await Assert.That((int)page.Header.NextPageID).IsEqualTo(6);
        await Assert.That(page.IsDirty).IsTrue();
    }
}