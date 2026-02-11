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
        await Assert.That(result!).IsNotNull();
        await Assert.That(result!.ContainsKey("_id")).IsTrue();
        await Assert.That(result!.ContainsKey("name")).IsTrue();
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
        await Assert.That(result == null).IsTrue();
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
            (id, pageId, idx) => updatedIds[id.ToString() ?? string.Empty] = (pageId, idx)
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
            (id, pageId, idx) => updatedIds[id.ToString() ?? string.Empty] = (pageId, idx)
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

        // Add corrupted document bytes (invalid BSON)
        page.Append(new byte[] { 1, 2, 3 });
        
        var scanned = _dpa.ScanDocumentsFromPage(page).ToList();
        await Assert.That(scanned.Count).IsEqualTo(1);
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

    [Test]
    public async Task ReadDocumentsFromPage_WithInvalidBson_SkipsInvalidEntryAndContinues()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);

        // Invalid BSON entry
        page.Append(new byte[] { 1, 2, 3 });

        // Valid entry after invalid one
        var validDoc = new BsonDocument().Set("_id", 7).Set("name", "ok");
        page.Append(BsonSerializer.SerializeDocument(validDoc));

        var entries = _dpa.ReadDocumentsFromPage(page);
        await Assert.That(entries.Count).IsEqualTo(1);
        await Assert.That(entries[0].Document["_id"].ToInt32(null)).IsEqualTo(7);
    }

    [Test]
    public async Task ReadDocumentAt_WithInvalidBson_ReturnsNull()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);

        page.Append(new byte[] { 1, 2, 3 });

        var entry = _dpa.ReadDocumentAt(page, 0);
        await Assert.That(entry).IsNull();
    }

    [Test]
    public async Task ReadDocumentAt_SpanTooShortDuringSkip_ReturnsNull()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);

        // Corrupt header: claim 2 entries but keep no valid data span.
        page.Header.ItemCount = 2;

        var entry = _dpa.ReadDocumentAt(page, 1);
        await Assert.That(entry).IsNull();
    }

    [Test]
    public async Task ReadDocumentAt_TargetLengthExceedsSpan_ReturnsNull()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);

        // Corrupt header to expose a 4-byte span (len prefix) but no content.
        page.Header.ItemCount = 1;
        page.Header.FreeBytes = (ushort)(page.DataCapacity - 4);

        // Write a target length larger than the available span.
        page.WriteData(0, BitConverter.GetBytes(10));

        var entry = _dpa.ReadDocumentAt(page, 0);
        await Assert.That(entry).IsNull();
    }

    [Test]
    public async Task ReadDocumentAt_SpanTooShortForLengthPrefix_ReturnsNull()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);

        // Corrupt header: claim 1 entry but keep no valid data span.
        page.Header.ItemCount = 1;

        var entry = _dpa.ReadDocumentAt(page, 0);
        await Assert.That(entry).IsNull();
    }

    [Test]
    public async Task ReadDocumentAt_WithFields_SpanTooShortDuringSkip_ReturnsNull()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);

        page.Header.ItemCount = 2;

        var fields = new HashSet<string> { "_id" };
        var doc = _dpa.ReadDocumentAt(page, 1, fields);
        await Assert.That(doc is null).IsTrue();
    }

    [Test]
    public async Task ReadDocumentAt_WithFields_SpanAllowsSkipButNotTarget_ReturnsNull()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);

        // Expose a 4-byte span so skip loop can read a length (0 by default).
        page.Header.ItemCount = 2;
        page.Header.FreeBytes = (ushort)(page.DataCapacity - 4);

        var fields = new HashSet<string> { "_id" };
        var doc = _dpa.ReadDocumentAt(page, 1, fields);
        await Assert.That(doc is null).IsTrue();
    }

    [Test]
    public async Task ReadDocumentAt_WithFields_TargetLengthExceedsSpan_ReturnsNull()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);

        page.Header.ItemCount = 1;
        page.Header.FreeBytes = (ushort)(page.DataCapacity - 4);
        page.WriteData(0, BitConverter.GetBytes(10));

        var fields = new HashSet<string> { "_id" };
        var doc = _dpa.ReadDocumentAt(page, 0, fields);
        await Assert.That(doc is null).IsTrue();
    }

    [Test]
    public async Task ReadDocumentAt_WithFields_InvalidBson_ReturnsNull()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        page.Append(new byte[] { 1, 2, 3 });

        var fields = new HashSet<string> { "_id" };
        var doc = _dpa.ReadDocumentAt(page, 0, fields);
        await Assert.That(doc is null).IsTrue();
    }

    [Test]
    public async Task ReadDocumentAt_WithFields_LargeDocument_NullFields_ReturnsFullLargeDocument()
    {
        var largeDoc = new BsonDocument()
            .Set("_id", 1)
            .Set("name", "big")
            .Set("payload", new string('x', 5000));

        var indexPageId = _lds.StoreLargeDocument(largeDoc, "col");
        var largeBytes = BsonSerializer.SerializeDocument(largeDoc);

        var meta = new BsonDocument()
            .Set("_isLargeDocument", true)
            .Set("_largeDocumentIndex", (long)indexPageId)
            .Set("_largeDocumentSize", (long)largeBytes.Length);

        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        page.Append(BsonSerializer.SerializeDocument(meta));

        var doc = _dpa.ReadDocumentAt(page, 0, null);
        await Assert.That(doc).IsNotNull();
        await Assert.That(doc!.ContainsKey("payload")).IsTrue();
        await Assert.That(doc["name"].ToString()).IsEqualTo("big");
    }

    [Test]
    public async Task ReadDocumentAt_WithFields_LargeDocument_WithProjection_ReturnsProjectedLargeDocument()
    {
        var largeDoc = new BsonDocument()
            .Set("_id", 1)
            .Set("name", "big")
            .Set("payload", new string('x', 5000));

        var indexPageId = _lds.StoreLargeDocument(largeDoc, "col");
        var largeBytes = BsonSerializer.SerializeDocument(largeDoc);

        var meta = new BsonDocument()
            .Set("_isLargeDocument", true)
            .Set("_largeDocumentIndex", (long)indexPageId)
            .Set("_largeDocumentSize", (long)largeBytes.Length);

        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);
        page.Append(BsonSerializer.SerializeDocument(meta));

        var fields = new HashSet<string> { "_id", "name" };
        var doc = _dpa.ReadDocumentAt(page, 0, fields);
        await Assert.That(doc).IsNotNull();
        await Assert.That(doc!.ContainsKey("_id")).IsTrue();
        await Assert.That(doc.ContainsKey("name")).IsTrue();
        await Assert.That(doc.ContainsKey("payload")).IsFalse();
        await Assert.That(doc["name"].ToString()).IsEqualTo("big");
    }

    [Test]
    public async Task RewritePageWithDocuments_WhenEntryIdIsNull_DoesNotInvokeUpdateCallback()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);

        var rawBytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1).Set("x", 1));

        var docWithNullId = new BsonDocument().Set("_id", null!);
        var docs = new List<PageDocumentEntry>
        {
            new PageDocumentEntry(docWithNullId, rawBytes, false, 0, 0)
        };

        var called = false;
        _dpa.RewritePageWithDocuments(
            "test_col",
            new CollectionState(),
            page,
            docs,
            (_, _, _) => called = true);

        await Assert.That(called).IsFalse();
    }

    [Test]
    public async Task RewritePageWithDocuments_PassesIdThroughWithoutToString()
    {
        var page = _pm.NewPage(PageType.Data);
        page.ResetBytes(0);

        var rawBytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1).Set("x", 1));
        var docWithNullToStringId = new BsonDocument().Set("_id", new NullToStringBsonValue());

        var docs = new List<PageDocumentEntry>
        {
            new PageDocumentEntry(docWithNullToStringId, rawBytes, false, 0, 0)
        };

        BsonValue? captured = null;
        _dpa.RewritePageWithDocuments(
            "test_col",
            new CollectionState(),
            page,
            docs,
            (id, _, _) => captured = id);

        await Assert.That(ReferenceEquals(captured, docWithNullToStringId["_id"])).IsTrue();
    }

    private sealed class NullToStringBsonValue : BsonValue
    {
        public override BsonType BsonType => BsonType.String;
        public override object? RawValue => null;
        public override int CompareTo(BsonValue? other) => 0;
        public override bool Equals(BsonValue? other) => ReferenceEquals(this, other);
        public override int GetHashCode() => 0;
        public override string ToString() => null!;

        public override TypeCode GetTypeCode() => TypeCode.Object;
        public override bool ToBoolean(IFormatProvider? provider) => throw new NotSupportedException();
        public override byte ToByte(IFormatProvider? provider) => throw new NotSupportedException();
        public override char ToChar(IFormatProvider? provider) => throw new NotSupportedException();
        public override DateTime ToDateTime(IFormatProvider? provider) => throw new NotSupportedException();
        public override decimal ToDecimal(IFormatProvider? provider) => throw new NotSupportedException();
        public override double ToDouble(IFormatProvider? provider) => throw new NotSupportedException();
        public override short ToInt16(IFormatProvider? provider) => throw new NotSupportedException();
        public override int ToInt32(IFormatProvider? provider) => throw new NotSupportedException();
        public override long ToInt64(IFormatProvider? provider) => throw new NotSupportedException();
        public override sbyte ToSByte(IFormatProvider? provider) => throw new NotSupportedException();
        public override float ToSingle(IFormatProvider? provider) => throw new NotSupportedException();
        public override string ToString(IFormatProvider? provider) => null!;
        public override object ToType(Type conversionType, IFormatProvider? provider) => throw new NotSupportedException();
        public override ushort ToUInt16(IFormatProvider? provider) => throw new NotSupportedException();
        public override uint ToUInt32(IFormatProvider? provider) => throw new NotSupportedException();
        public override ulong ToUInt64(IFormatProvider? provider) => throw new NotSupportedException();
    }

    #endregion
}
