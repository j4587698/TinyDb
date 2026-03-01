using System.IO;
using TinyDb.Core;
using TinyDb.Storage;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageManagerErrorTests
{
    [Test]
    public async Task GetPage_WhenReadFails_ShouldThrowInvalidDataException()
    {
        using var mockStream = new MockDiskStream();
        using var pm = new PageManager(mockStream);
        
        // Setup initial page
        var p = pm.NewPage(PageType.Data);
        pm.SavePage(p);
        pm.ClearCache(); // Force read from disk
        
        mockStream.ShouldThrowOnRead = true;
        
        await Assert.That(() => pm.GetPage(p.PageID)).Throws<InvalidDataException>();
    }

    [Test]
    public async Task Engine_Should_Throw_On_Write_Error()
    {
        using var mockStream = new MockDiskStream();
        var engine = new TinyDbEngine("mock.db", new TinyDbOptions(), mockStream);
        try
        {
            engine.InsertDocument("col", new BsonDocument().Set("_id", 1));

            mockStream.ShouldThrowOnWrite = true;

            await Assert.That(() => engine.InsertDocument("col", new BsonDocument().Set("_id", 2)))
                .Throws<IOException>();
        }
        finally
        {
            mockStream.ShouldThrowOnWrite = false;
            engine.Dispose();
        }
    }
}
