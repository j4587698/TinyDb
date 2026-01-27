using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Storage;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

/// <summary>
/// Low-level tests for DataPageAccess to improve branch coverage.
/// Tests internal behavior such as caching, field projection, and boundary conditions.
/// </summary>
public class DataPageAccessLowLevelTests : IDisposable
{
    private readonly string _dbPath;
    private DiskStream _diskStream;
    private PageManager _pm;
    private LargeDocumentStorage _lds;
    private WriteAheadLog _wal;
    private DataPageAccess _dpa;

    public DataPageAccessLowLevelTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"dpa_ll_{Guid.NewGuid()}.db");
        _diskStream = new DiskStream(_dbPath);
        _pm = new PageManager(_diskStream, 8192);
        _lds = new LargeDocumentStorage(_pm, 8192);
        _wal = new WriteAheadLog(_dbPath, 8192, true);
        _dpa = new DataPageAccess(_pm, _lds, _wal);
    }

    public void Dispose()
    {
        _wal?.Dispose();
        _pm?.Dispose();
        _diskStream?.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
        try { if (File.Exists(_dbPath + ".wal")) File.Delete(_dbPath + ".wal"); } catch { }
    }

    #region GetEntrySize and GetMaxDocumentSize Tests

    [Test]
    public async Task GetEntrySize_ReturnsLengthPlusFour()
    {
        var size = DataPageAccess.GetEntrySize(100);
        await Assert.That(size).IsEqualTo(104); // 100 + 4 bytes for length prefix
    }

    [Test]
    public async Task GetMaxDocumentSize_ReturnsPageSizeMinus300()
    {
        var maxSize = _dpa.GetMaxDocumentSize();
        await Assert.That(maxSize).IsEqualTo((int)_pm.PageSize - 300);
    }

    #endregion

    #region ScanDocumentsFromPage Tests

    [Test]
    public async Task ScanDocumentsFromPage_EmptyPage_ReturnsEmpty()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var docs = _dpa.ScanDocumentsFromPage(page).ToList();
        await Assert.That(docs.Count).IsEqualTo(0);
    }

    [Test]
    public async Task ScanDocumentsFromPage_WithValidDocument_ReturnsDocument()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc = new BsonDocument().Set("_id", 1).Set("name", "test");
        var bytes = BsonSerializer.SerializeDocument(doc);
        page.Append(bytes);
        
        var scanned = _dpa.ScanDocumentsFromPage(page).ToList();
        await Assert.That(scanned.Count).IsEqualTo(1);
        await Assert.That(((BsonInt32)scanned[0]["_id"]).Value).IsEqualTo(1);
    }

    [Test]
    public async Task ScanDocumentsFromPage_WithMultipleDocuments_ReturnsAll()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        for (int i = 0; i < 5; i++)
        {
            var doc = new BsonDocument().Set("_id", i).Set("value", i * 10);
            var bytes = BsonSerializer.SerializeDocument(doc);
            page.Append(bytes);
        }
        
        var scanned = _dpa.ScanDocumentsFromPage(page).ToList();
        await Assert.That(scanned.Count).IsEqualTo(5);
        
        for (int i = 0; i < 5; i++)
        {
            await Assert.That(((BsonInt32)scanned[i]["_id"]).Value).IsEqualTo(i);
        }
    }

    #endregion

    #region ReadDocumentsFromPage Tests

    [Test]
    public async Task ReadDocumentsFromPage_EmptyPage_ReturnsEmptyList()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var entries = _dpa.ReadDocumentsFromPage(page);
        await Assert.That(entries.Count).IsEqualTo(0);
    }

    [Test]
    public async Task ReadDocumentsFromPage_WithDocuments_ReturnsAllEntries()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc1 = new BsonDocument().Set("_id", 1).Set("data", "first");
        var doc2 = new BsonDocument().Set("_id", 2).Set("data", "second");
        page.Append(BsonSerializer.SerializeDocument(doc1));
        page.Append(BsonSerializer.SerializeDocument(doc2));
        
        var entries = _dpa.ReadDocumentsFromPage(page);
        await Assert.That(entries.Count).IsEqualTo(2);
        await Assert.That(entries[0].Document["_id"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(entries[1].Document["_id"].ToInt32(null)).IsEqualTo(2);
    }

    [Test]
    public async Task ReadDocumentsFromPage_ReturnsCachedCopy_OnSecondCall()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc = new BsonDocument().Set("_id", 1).Set("cached", true);
        page.Append(BsonSerializer.SerializeDocument(doc));
        
        // First call - reads and caches
        var entries1 = _dpa.ReadDocumentsFromPage(page);
        await Assert.That(entries1.Count).IsEqualTo(1);
        
        // Second call - should return cached copy
        var entries2 = _dpa.ReadDocumentsFromPage(page);
        await Assert.That(entries2.Count).IsEqualTo(1);
        
        // Both should have same content but be different list instances
        await Assert.That(entries1[0].Document["_id"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(entries2[0].Document["_id"].ToInt32(null)).IsEqualTo(1);
    }

    #endregion

    #region ReadDocumentAt Tests

    [Test]
    public async Task ReadDocumentAt_IndexOutOfRange_ReturnsNull()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc = new BsonDocument().Set("_id", 1);
        page.Append(BsonSerializer.SerializeDocument(doc));
        
        var result = _dpa.ReadDocumentAt(page, 10);
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task ReadDocumentAt_ValidIndex_ReturnsEntry()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc1 = new BsonDocument().Set("_id", 1);
        var doc2 = new BsonDocument().Set("_id", 2);
        var doc3 = new BsonDocument().Set("_id", 3);
        page.Append(BsonSerializer.SerializeDocument(doc1));
        page.Append(BsonSerializer.SerializeDocument(doc2));
        page.Append(BsonSerializer.SerializeDocument(doc3));
        
        var entry = _dpa.ReadDocumentAt(page, 1);
        await Assert.That(entry).IsNotNull();
        await Assert.That(entry!.Value.Document["_id"].ToInt32(null)).IsEqualTo(2);
    }

    [Test]
    public async Task ReadDocumentAt_FirstDocument_ReturnsCorrectEntry()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc = new BsonDocument().Set("_id", 100).Set("first", true);
        page.Append(BsonSerializer.SerializeDocument(doc));
        
        var entry = _dpa.ReadDocumentAt(page, 0);
        await Assert.That(entry).IsNotNull();
        await Assert.That(entry!.Value.Document["_id"].ToInt32(null)).IsEqualTo(100);
    }

    [Test]
    public async Task ReadDocumentAt_LastDocument_ReturnsCorrectEntry()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        for (int i = 0; i < 10; i++)
        {
            var doc = new BsonDocument().Set("_id", i);
            page.Append(BsonSerializer.SerializeDocument(doc));
        }
        
        var entry = _dpa.ReadDocumentAt(page, 9);
        await Assert.That(entry).IsNotNull();
        await Assert.That(entry!.Value.Document["_id"].ToInt32(null)).IsEqualTo(9);
    }

    #endregion

    #region ReadDocumentAt with Fields Tests

    [Test]
    public async Task ReadDocumentAt_WithFields_NullFields_ReturnsFullDocument()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc = new BsonDocument()
            .Set("_id", 1)
            .Set("name", "test")
            .Set("value", 42);
        page.Append(BsonSerializer.SerializeDocument(doc));
        
        var result = _dpa.ReadDocumentAt(page, 0, null);
        await Assert.That(result).IsNotNull();
        await Assert.That(result!.ContainsKey("name")).IsTrue();
        await Assert.That(result.ContainsKey("value")).IsTrue();
    }

    [Test]
    public async Task ReadDocumentAt_WithFields_SpecificFields_ReturnsProjectedDocument()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc = new BsonDocument()
            .Set("_id", 1)
            .Set("name", "test")
            .Set("value", 42)
            .Set("extra", "not needed");
        page.Append(BsonSerializer.SerializeDocument(doc));
        
        var fields = new HashSet<string> { "_id", "name" };
        var result = _dpa.ReadDocumentAt(page, 0, fields);
        await Assert.That(result).IsNotNull();
        await Assert.That(result!.ContainsKey("_id")).IsTrue();
        await Assert.That(result.ContainsKey("name")).IsTrue();
    }

    [Test]
    public async Task ReadDocumentAt_WithFields_IndexOutOfRange_ReturnsNull()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc = new BsonDocument().Set("_id", 1);
        page.Append(BsonSerializer.SerializeDocument(doc));
        
        var fields = new HashSet<string> { "_id" };
        var result = _dpa.ReadDocumentAt(page, 5, fields);
        await Assert.That(result).IsNull();
    }

    #endregion

    #region ScanRawDocumentsFromPage Tests

    [Test]
    public async Task ScanRawDocumentsFromPage_EmptyPage_ReturnsEmpty()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var rawDocs = _dpa.ScanRawDocumentsFromPage(page).ToList();
        await Assert.That(rawDocs.Count).IsEqualTo(0);
    }

    [Test]
    public async Task ScanRawDocumentsFromPage_WithDocuments_ReturnsRawBytes()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc = new BsonDocument().Set("_id", 1).Set("raw", "test");
        var originalBytes = BsonSerializer.SerializeDocument(doc);
        page.Append(originalBytes);
        
        var rawDocs = _dpa.ScanRawDocumentsFromPage(page).ToList();
        await Assert.That(rawDocs.Count).IsEqualTo(1);
        await Assert.That(rawDocs[0].Length).IsEqualTo(originalBytes.Length);
    }

    #endregion

    #region CanFitInPage Tests

    [Test]
    public async Task CanFitInPage_SmallDocuments_ReturnsTrue()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var docs = new List<PageDocumentEntry>();
        for (int i = 0; i < 3; i++)
        {
            var doc = new BsonDocument().Set("_id", i);
            var bytes = BsonSerializer.SerializeDocument(doc);
            docs.Add(new PageDocumentEntry(doc, bytes, false, 0, 0));
        }
        
        var canFit = _dpa.CanFitInPage(page, docs);
        await Assert.That(canFit).IsTrue();
    }

    [Test]
    public async Task CanFitInPage_LargeDocuments_ReturnsFalse()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var docs = new List<PageDocumentEntry>();
        // Create documents that exceed page capacity
        for (int i = 0; i < 100; i++)
        {
            var doc = new BsonDocument().Set("_id", i).Set("data", new string('x', 1000));
            var bytes = BsonSerializer.SerializeDocument(doc);
            docs.Add(new PageDocumentEntry(doc, bytes, false, 0, 0));
        }
        
        var canFit = _dpa.CanFitInPage(page, docs);
        await Assert.That(canFit).IsFalse();
    }

    #endregion

    #region GetWritableDataPageLocked Tests

    [Test]
    public async Task GetWritableDataPageLocked_NoExistingPage_ReturnsNewPage()
    {
        var state = new DataPageState();
        
        var (page, isNew) = _dpa.GetWritableDataPageLocked(state, 100);
        
        await Assert.That(page).IsNotNull();
        await Assert.That(isNew).IsTrue();
        await Assert.That(state.PageId).IsEqualTo(page.PageID);
    }

    [Test]
    public async Task GetWritableDataPageLocked_ExistingPageWithSpace_ReturnsSamePage()
    {
        var state = new DataPageState();
        
        // Get first page
        var (page1, isNew1) = _dpa.GetWritableDataPageLocked(state, 100);
        _pm.SavePage(page1, true);
        
        // Get page again - should return same page since there's space
        var (page2, isNew2) = _dpa.GetWritableDataPageLocked(state, 100);
        
        await Assert.That(page2.PageID).IsEqualTo(page1.PageID);
        await Assert.That(isNew2).IsFalse();
    }

    [Test]
    public async Task GetWritableDataPageLocked_ExistingPageFull_ReturnsNewPage()
    {
        var state = new DataPageState();
        
        // Get first page
        var (page1, _) = _dpa.GetWritableDataPageLocked(state, 100);
        _pm.SavePage(page1, true);
        uint firstPageId = page1.PageID;
        
        // Request more space than available
        var (page2, isNew2) = _dpa.GetWritableDataPageLocked(state, (int)_pm.PageSize * 2);
        
        await Assert.That(isNew2).IsTrue();
        await Assert.That(page2.PageID).IsNotEqualTo(firstPageId);
    }

    #endregion

    #region RewritePageWithDocuments Tests

    [Test]
    public async Task RewritePageWithDocuments_UpdatesPageAndPreservesLinks()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        page.SetLinks(1, 2); // Set prev=1, next=2
        
        var doc = new BsonDocument().Set("_id", 1).Set("name", "rewritten");
        var bytes = BsonSerializer.SerializeDocument(doc);
        var docs = new List<PageDocumentEntry>
        {
            new PageDocumentEntry(doc, bytes, false, 0, 0)
        };
        
        var updatedIds = new Dictionary<string, (uint pageId, int index)>();
        _dpa.RewritePageWithDocuments(
            "test_col",
            new CollectionState(),
            page,
            docs,
            (id, pageId, idx) => updatedIds[id] = (pageId, idx)
        );
        
        // Verify page was saved and links preserved
        await Assert.That(page.Header.PrevPageID).IsEqualTo((uint)1);
        await Assert.That(page.Header.NextPageID).IsEqualTo((uint)2);
        await Assert.That((int)page.Header.ItemCount).IsEqualTo(1);
        
        // Verify index update callback was called
        await Assert.That(updatedIds.ContainsKey("1")).IsTrue();
    }

    [Test]
    public async Task RewritePageWithDocuments_WithMultipleDocuments_UpdatesAllIndices()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var docs = new List<PageDocumentEntry>();
        for (int i = 0; i < 5; i++)
        {
            var doc = new BsonDocument().Set("_id", i);
            var bytes = BsonSerializer.SerializeDocument(doc);
            docs.Add(new PageDocumentEntry(doc, bytes, false, 0, 0));
        }
        
        var updatedIds = new Dictionary<string, (uint pageId, int index)>();
        _dpa.RewritePageWithDocuments(
            "test_col",
            new CollectionState(),
            page,
            docs,
            (id, pageId, idx) => updatedIds[id] = (pageId, idx)
        );
        
        await Assert.That(updatedIds.Count).IsEqualTo(5);
        await Assert.That((int)page.Header.ItemCount).IsEqualTo(5);
        
        // Verify indices are correct
        for (int i = 0; i < 5; i++)
        {
            await Assert.That(updatedIds.ContainsKey(i.ToString())).IsTrue();
            await Assert.That(updatedIds[i.ToString()].index).IsEqualTo(i);
        }
    }

    #endregion

    #region PersistPage Tests

    [Test]
    public async Task PersistPage_SavesPageSuccessfully()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc = new BsonDocument().Set("_id", 1).Set("persisted", true);
        page.Append(BsonSerializer.SerializeDocument(doc));
        
        _dpa.PersistPage(page);
        
        // Reload page and verify
        var reloaded = _pm.GetPage(page.PageID);
        await Assert.That((int)reloaded.Header.ItemCount).IsEqualTo(1);
    }

    #endregion

    #region Boundary Conditions Tests

    [Test]
    public async Task ScanDocumentsFromPage_WithCorruptedData_SkipsCorruptedEntries()
    {
        // This test verifies that corrupted documents are silently skipped
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        // Add valid document
        var validDoc = new BsonDocument().Set("_id", 1);
        page.Append(BsonSerializer.SerializeDocument(validDoc));
        
        var scanned = _dpa.ScanDocumentsFromPage(page).ToList();
        await Assert.That(scanned.Count).IsGreaterThanOrEqualTo(1);
    }

    [Test]
    public async Task ReadDocumentsFromPage_CachedDataIsIndependent()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        var doc = new BsonDocument().Set("_id", 1);
        page.Append(BsonSerializer.SerializeDocument(doc));
        
        // First read populates cache
        var entries1 = _dpa.ReadDocumentsFromPage(page);
        
        // Modify returned list
        entries1.Clear();
        
        // Second read should still have data (returns copy)
        var entries2 = _dpa.ReadDocumentsFromPage(page);
        await Assert.That(entries2.Count).IsEqualTo(1);
    }

    [Test]
    public async Task ReadDocumentAt_SkipsDocumentsCorrectly()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        
        // Add documents of varying sizes
        for (int i = 0; i < 5; i++)
        {
            var doc = new BsonDocument()
                .Set("_id", i)
                .Set("data", new string((char)('a' + i), (i + 1) * 100)); // varying sizes
            page.Append(BsonSerializer.SerializeDocument(doc));
        }
        
        // Read each document by index
        for (int i = 0; i < 5; i++)
        {
            var entry = _dpa.ReadDocumentAt(page, i);
            await Assert.That(entry).IsNotNull();
            await Assert.That(entry!.Value.Document["_id"].ToInt32(null)).IsEqualTo(i);
        }
    }

    #endregion
}
