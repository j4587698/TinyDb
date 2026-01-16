using TinyDb.Storage;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class LargeDocumentStorageTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly DiskStream _diskStream;
    private readonly PageManager _pageManager;
    private const int PageSize = 4096;

    public LargeDocumentStorageTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"large_doc_{Guid.NewGuid():N}.db");
        _diskStream = new DiskStream(_testDbPath);
        _pageManager = new PageManager(_diskStream, PageSize);
    }

    public void Dispose()
    {
        _diskStream.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task LargeDocument_RoundTrip_Should_Work()
    {
        var storage = new LargeDocumentStorage(_pageManager, PageSize);
        
        // Create a document larger than one page (4096 bytes)
        var largeData = new byte[10000];
        new Random(42).NextBytes(largeData);
        var doc = new BsonDocument().Set("data", new BsonBinary(largeData));
        
        // Act
        var indexPageId = storage.StoreLargeDocument(doc, "TestCollection");
        await Assert.That(indexPageId).IsGreaterThan(0u);
        
        var isValid = storage.ValidateLargeDocument(indexPageId);
        await Assert.That(isValid).IsTrue();
        
        var stats = storage.GetStatistics(indexPageId);
        // Data payload size per page is 4096 - 41 - 8 = 4047.
        // Bson overhead for doc with 10000 bytes binary is ~10015 bytes.
        // 10015 / 4047 = 2.47 -> 3 pages.
        await Assert.That(stats.PageCount).IsEqualTo(3); 
        
        var replayedData = storage.ReadLargeDocument(indexPageId);
        var replayedDoc = BsonSerializer.DeserializeDocument(replayedData);
        
        // Assert
        var replayedBinary = (BsonBinary)replayedDoc["data"];
        await Assert.That(replayedBinary.Bytes.SequenceEqual(largeData)).IsTrue();
        
        // Delete
        storage.DeleteLargeDocument(indexPageId);
        await Assert.That(storage.ValidateLargeDocument(indexPageId)).IsFalse();
    }

    [Test]
    public async Task ReadLargeDocument_With_Wrong_PageType_Should_Throw()
    {
        var storage = new LargeDocumentStorage(_pageManager, PageSize);
        var regularPage = _pageManager.NewPage(PageType.Data);
        _pageManager.SavePage(regularPage);

        await Assert.That(() => storage.ReadLargeDocument(regularPage.PageID))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ValidateLargeDocument_With_Corrupt_Chain_Should_Return_False()
    {
        var storage = new LargeDocumentStorage(_pageManager, PageSize);
        var largeData = new byte[5000];
        var doc = new BsonDocument().Set("d", new BsonBinary(largeData));
        var indexPageId = storage.StoreLargeDocument(doc, "C");

        var stats = storage.GetStatistics(indexPageId);
        var firstDataPageId = stats.FirstDataPageId;

        // Corrupt the first data page by changing its type
        var firstPage = _pageManager.GetPage(firstDataPageId);
        firstPage.UpdatePageType(PageType.Data);
        _pageManager.SavePage(firstPage);

        await Assert.That(storage.ValidateLargeDocument(indexPageId)).IsFalse();
    }
}
