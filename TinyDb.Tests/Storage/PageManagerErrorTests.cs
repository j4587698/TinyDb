using TinyDb.Core;
using TinyDb.Storage;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageManagerErrorTests
{
    [Test]
    public async Task GetPage_Should_Handle_Read_Error_By_Returning_Empty_Page()
    {
        using var mockStream = new MockDiskStream();
        using var pm = new PageManager(mockStream);
        
        // Setup initial page
        var p = pm.NewPage(PageType.Data);
        pm.SavePage(p);
        pm.ClearCache(); // Force read from disk
        
        mockStream.ShouldThrowOnRead = true;
        
        // PageManager.GetPage catches Exception and returns new empty page
        var page = pm.GetPage(p.PageID);
        await Assert.That(page.PageType).IsEqualTo(PageType.Empty);
        await Assert.That(page.Header.ItemCount).IsEqualTo((ushort)0);
    }

    [Test]
    public async Task Engine_Should_Throw_On_Write_Error()
    {
        using var mockStream = new MockDiskStream();
        // Use internal constructor with dependency injection
        using var engine = new TinyDbEngine("mock.db", new TinyDbOptions(), mockStream);
        
        // Insert works
        engine.InsertDocument("col", new BsonDocument().Set("_id", 1));
        
        // Simulate write error
        mockStream.ShouldThrowOnWrite = true;
        
        // Insert should throw IOException when it tries to write page (e.g. allocating new page)
        // Note: AllocatePage calls SavePage/WritePage to init new page?
        // Or SavePage is called later?
        // AllocatePage writes if file grows? Yes, CreateNewPage writes.
        
        await Assert.That(() => engine.InsertDocument("col", new BsonDocument().Set("_id", 2)))
            .Throws<IOException>();
    }
}
