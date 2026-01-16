using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

[NotInParallel]
public class LargeDocumentExhaustiveTests
{
    private string _testFile = null!;
    private DiskStream _diskStream = null!;
    private PageManager _pageManager = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"ld_ex_{Guid.NewGuid():N}.db");
        _diskStream = new DiskStream(_testFile);
        _pageManager = new PageManager(_diskStream, 4096); // Small page size to force splitting
    }

    [After(Test)]
    public void Cleanup()
    {
        _pageManager.Dispose();
        _diskStream.Dispose();
        if (File.Exists(_testFile)) File.Delete(_testFile);
    }

    [Test]
    public async Task LargeDocument_MultiplePages_ShouldWork()
    {
        var storage = new LargeDocumentStorage(_pageManager, 4096);
        
        // Document larger than 2 pages
        var largeData = new byte[10000];
        new Random().NextBytes(largeData);
        var doc = new BsonDocument().Set("data", new BsonBinary(largeData));
        
        // 1. Write
        var indexPageId = storage.StoreLargeDocument(doc, "testCol");
        await Assert.That(indexPageId).IsGreaterThan(0u);
        
        // 2. Read
        var bytesBack = storage.ReadLargeDocument(indexPageId);
        var back = BsonSerializer.DeserializeDocument(bytesBack);
        await Assert.That(back).IsNotNull();
        var backData = (byte[])(BsonBinary)back!["data"];
        await Assert.That(backData.SequenceEqual(largeData)).IsTrue();
        
        // 3. Update (Delete then Store)
        storage.DeleteLargeDocument(indexPageId);
        
        var largerData = new byte[15000];
        new Random().NextBytes(largerData);
        var largerDoc = new BsonDocument().Set("data", new BsonBinary(largerData));
        
        var indexPageId2 = storage.StoreLargeDocument(largerDoc, "testCol");
        var bytesBack2 = storage.ReadLargeDocument(indexPageId2);
        var back2 = BsonSerializer.DeserializeDocument(bytesBack2);
        var backData2 = (byte[])(BsonBinary)back2!["data"];
        await Assert.That(backData2.SequenceEqual(largerData)).IsTrue();
        
        // 4. Delete
        storage.DeleteLargeDocument(indexPageId2);
    }

    [Test]
    public async Task LargeDocument_Statistics_ShouldWork()
    {
        var storage = new LargeDocumentStorage(_pageManager, 4096);
        var doc = new BsonDocument().Set("a", new string('x', 5000));
        var id = storage.StoreLargeDocument(doc, "test");
        
        var stats = storage.GetStatistics(id);
        await Assert.That(stats).IsNotNull();
        await Assert.That(stats.PageCount).IsGreaterThanOrEqualTo(1);
    }
}
