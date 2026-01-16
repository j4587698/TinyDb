using System;
using System.IO;
using TinyDb.Core;
using TinyDb.Storage;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

[NotInParallel]
public class LargeDocumentStorageExtendedTests
{
    private string _testFile = null!;
    private IDiskStream _diskStream = null!;
    private PageManager _pageManager = null!;
    private LargeDocumentStorage _storage = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"large_doc_ext_{Guid.NewGuid():N}.db");
        _diskStream = new DiskStream(_testFile, FileAccess.ReadWrite, FileShare.ReadWrite);
        _pageManager = new PageManager(_diskStream, 4096, 100);
        _storage = new LargeDocumentStorage(_pageManager, 4096);
    }

    [After(Test)]
    public void Cleanup()
    {
        _pageManager?.Dispose();
        _diskStream?.Dispose();
        if (File.Exists(_testFile))
            File.Delete(_testFile);
    }

    [Test]
    public async Task ReadLargeDocument_With_Wrong_PageType_Should_Throw()
    {
        // Arrange: Create a normal data page
        var page = _pageManager.NewPage(PageType.Data);
        _pageManager.SavePage(page);

        // Act & Assert: Try to read it as a large document index
        await Assert.That(() => _storage.ReadLargeDocument(page.PageID))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task ValidateLargeDocument_With_Corrupt_Chain_Should_Return_False()
    {
        // Arrange: Store a large document
        var largeData = new byte[10000];
        new Random(42).NextBytes(largeData);
        var indexPageId = _storage.StoreLargeDocument(largeData, "test");

        // Corrupt the chain: Get the first data page and change its NextPageId to 0
        var indexPage = _pageManager.GetPage(indexPageId);
        // Header starts at offset 0
        // DocumentIdentifier(4) + TotalLength(4) + PageCount(4) + FirstDataPageId(4)
        var firstDataPageId = BitConverter.ToUInt32(indexPage.ReadData(12, 4));
        
        var firstDataPage = _pageManager.GetPage(firstDataPageId);
        // DataPageHeader: PageNumber(4) + NextPageId(4)
        // Set NextPageId to 0 (premature end)
        firstDataPage.WriteData(4, new byte[] { 0, 0, 0, 0 });
        _pageManager.SavePage(firstDataPage);

        // Act
        var isValid = _storage.ValidateLargeDocument(indexPageId);

        // Assert
        await Assert.That(isValid).IsFalse();
    }

    [Test]
    public async Task ReadLargeDocument_With_Mismatched_PageNumber_Should_Throw()
    {
        // Arrange
        var largeData = new byte[10000];
        var indexPageId = _storage.StoreLargeDocument(largeData, "test");

        var indexPage = _pageManager.GetPage(indexPageId);
        var firstDataPageId = BitConverter.ToUInt32(indexPage.ReadData(12, 4));
        
        var firstDataPage = _pageManager.GetPage(firstDataPageId);
        // Change PageNumber to something else (e.g., 5 instead of 0)
        firstDataPage.WriteData(0, BitConverter.GetBytes(5));
        _pageManager.SavePage(firstDataPage);

        // Act & Assert
        await Assert.That(() => _storage.ReadLargeDocument(indexPageId))
            .Throws<InvalidOperationException>();
    }
}
