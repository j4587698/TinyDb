using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageManagerResizeTests : IDisposable
{
    private readonly string _testDbPath;

    public PageManagerResizeTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"pm_resize_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task AllocatePage_Should_Resize_File_When_Needed()
    {
        using (var engine = new TinyDbEngine(_testDbPath))
        {
            var colName = "col";
            var largeData = new string('a', 4000); 
            
            // Insert enough to grow file beyond initial size
            for(int i=0; i<50; i++)
            {
                var doc = new TinyDb.Bson.BsonDocument()
                    .Set("_id", i)
                    .Set("Data", largeData);
                engine.InsertDocument(colName, doc);
            }
            
            // Explicit flush
            engine.Flush();
        }
        
        var fileInfo = new FileInfo(_testDbPath);
        // 50 pages * 8192 (default) = ~400KB. 
        // Even with packing, 50 * 4KB = 200KB.
        await Assert.That(fileInfo.Length).IsGreaterThan(100000);
    }
}
