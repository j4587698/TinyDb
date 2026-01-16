using System;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

public class PageHeaderFullTests
{
    [Test]
    public async Task PageHeader_Clone_ShouldWork()
    {
        var header = new PageHeader { PageID = 1, PageType = PageType.Data };
        header.PrevPageID = 10;
        header.NextPageID = 20;
        header.ItemCount = 5;
        header.FreeBytes = 1000;
        
        var clone = header.Clone();
        await Assert.That(clone.PageID).IsEqualTo(header.PageID);
        await Assert.That(clone.PrevPageID).IsEqualTo(header.PrevPageID);
        await Assert.That(clone.NextPageID).IsEqualTo(header.NextPageID);
        await Assert.That(clone.ItemCount).IsEqualTo(header.ItemCount);
        await Assert.That(clone.FreeBytes).IsEqualTo(header.FreeBytes);
    }

    [Test]
    public async Task PageHeader_ToString_ShouldWork()
    {
        var header = new PageHeader { PageID = 42, PageType = PageType.Index };
        await Assert.That(header.ToString()).Contains("ID=42");
        await Assert.That(header.ToString()).Contains("Index");
    }

    [Test]
    public async Task PageHeader_IsValid_ShouldWork()
    {
        var header = new PageHeader { PageID = 1, PageType = PageType.Data, CreatedAt = DateTime.UtcNow.Ticks, ModifiedAt = DateTime.UtcNow.Ticks };
        await Assert.That(header.IsValid()).IsTrue();
        
        var invalid = new PageHeader { PageID = 0, PageType = PageType.Empty };
        await Assert.That(invalid.IsValid()).IsFalse();
    }
}
