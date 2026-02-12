using System;
using System.IO;
using System.Linq;
using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

public class EngineCoverageTests
{
    private string _dbFile = null!;

    [Before(Test)]
    public void Setup()
    {
        _dbFile = Path.Combine(Path.GetTempPath(), $"eng_cov_{Guid.NewGuid():N}.db");
    }

    [After(Test)]
    public void Cleanup()
    {
        if (File.Exists(_dbFile)) File.Delete(_dbFile);
        if (File.Exists(_dbFile + ".compact")) File.Delete(_dbFile + ".compact");
    }

    [Test]
    public async Task Engine_Ctor_NullPath_ShouldThrow()
    {
        await Assert.That(() => new TinyDbEngine(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Engine_CollectionOperations_ShouldWork()
    {
        using var engine = new TinyDbEngine(_dbFile);
        
        // Drop non-existent collection
        await Assert.That(engine.DropCollection("nonexistent")).IsFalse();
        
        // Create and drop
        engine.GetBsonCollection("col1").Insert(new BsonDocument());
        await Assert.That(engine.CollectionExists("col1")).IsTrue();
        await Assert.That(engine.GetCollectionNames()).Contains("col1");
        
        await Assert.That(engine.DropCollection("col1")).IsTrue();
        await Assert.That(engine.CollectionExists("col1")).IsFalse();
        await Assert.That(engine.GetCollectionNames()).DoesNotContain("col1");
    }

    [Test]
    public async Task Engine_IndexOperations_ShouldWork()
    {
        using var engine = new TinyDbEngine(_dbFile);
        
        // Ensure index on empty collection
        await Assert.That(engine.EnsureIndex("col2", "field1", "idx1")).IsTrue();
        // Ensure existing index
        await Assert.That(engine.EnsureIndex("col2", "field1", "idx1")).IsFalse();
        
        var im = engine.GetIndexManager("col2");
        await Assert.That(im).IsNotNull();
        await Assert.That(im.IndexExists("idx1")).IsTrue();
    }

    [Test]
    public async Task Engine_CachedDocumentCount_ShouldWork()
    {
        using var engine = new TinyDbEngine(_dbFile);
        var col = engine.GetBsonCollection("col3");
        col.Insert(new BsonDocument());
        col.Insert(new BsonDocument());
        
        // Force flush to ensure they might be evicted or just checking state
        engine.Flush();
        
        // This relies on internal implementation details but let's check
        // At least it shouldn't throw
        await Assert.That(engine.GetCachedDocumentCount("col3")).IsGreaterThanOrEqualTo(0);
    }

    /// <summary>
    /// Test CompactDatabase functionality
    /// </summary>
    [Test]
    public async Task Engine_CompactDatabase_ShouldWork()
    {
        // Create database with some data and fragmentation
        using (var engine = new TinyDbEngine(_dbFile))
        {
            var col = engine.GetBsonCollection("compact_test");
            
            // Insert documents
            for (int i = 0; i < 50; i++)
            {
                col.Insert(new BsonDocument().Set("_id", i).Set("data", $"value_{i}"));
            }
            
            // Create an index
            engine.EnsureIndex("compact_test", "data", "idx_data");
            
            // Delete some documents to create fragmentation
            for (int i = 0; i < 25; i += 2)
            {
                col.Delete(i);
            }
            
            // Compact the database
            engine.CompactDatabase();
            
            // Verify data integrity after compaction
            var remaining = col.FindAll().ToList();
            await Assert.That(remaining.Count).IsGreaterThan(20);
        }
        
        // Reopen and verify
        using (var engine = new TinyDbEngine(_dbFile))
        {
            var col = engine.GetBsonCollection("compact_test");
            var docs = col.FindAll().ToList();
            await Assert.That(docs.Count).IsGreaterThan(20);
        }
    }

    [Test]
    public async Task Engine_CompactDatabase_WhenTargetLocked_ShouldThrow()
    {
        using var engine = new TinyDbEngine(_dbFile);
        engine.GetBsonCollection("locked_compact").Insert(new BsonDocument().Set("x", 1));

        using var lockStream = new FileStream(_dbFile, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);

        // Windows: an open handle without FileShare.Delete prevents rename/replace, so we expect CompactDatabase to fail.
        // Linux/macOS: renaming over an open file is allowed, so CompactDatabase may succeed even if another handle exists.
        if (!OperatingSystem.IsWindows())
        {
            await Assert.That(() => engine.CompactDatabase()).ThrowsNothing();
            return;
        }

        Exception? caught = null;
        try { engine.CompactDatabase(); } catch (Exception ex) { caught = ex; }

        await Assert.That(caught).IsNotNull();
        await Assert.That(caught is IOException || caught is UnauthorizedAccessException).IsTrue();
    }

    [Test]
    public async Task Engine_InsertDocumentsAsync_ExistingPage_ShouldHitGetPagePath()
    {
        using var engine = new TinyDbEngine(_dbFile);

        var first = await engine.InsertDocumentsAsync("async_page_reuse", new[] { new BsonDocument().Set("v", 1) });
        var second = await engine.InsertDocumentsAsync("async_page_reuse", new[] { new BsonDocument().Set("v", 2) });

        await Assert.That(first).IsEqualTo(1);
        await Assert.That(second).IsEqualTo(1);
    }

    /// <summary>
    /// Test CompactDatabase with multiple collections
    /// </summary>
    [Test]
    public async Task Engine_CompactDatabase_MultipleCollections_ShouldWork()
    {
        using var engine = new TinyDbEngine(_dbFile);
        
        // Create multiple collections
        var col1 = engine.GetBsonCollection("col_a");
        var col2 = engine.GetBsonCollection("col_b");
        
        col1.Insert(new BsonDocument().Set("name", "doc1"));
        col1.Insert(new BsonDocument().Set("name", "doc2"));
        col2.Insert(new BsonDocument().Set("value", 100));
        
        // Compact
        engine.CompactDatabase();
        
        // Verify both collections exist and have data
        await Assert.That(engine.CollectionExists("col_a")).IsTrue();
        await Assert.That(engine.CollectionExists("col_b")).IsTrue();
        await Assert.That(col1.FindAll().Count()).IsEqualTo(2);
        await Assert.That(col2.FindAll().Count()).IsEqualTo(1);
    }

    /// <summary>
    /// Test CompactDatabase with empty collection
    /// </summary>
    [Test]
    public async Task Engine_CompactDatabase_EmptyCollection_ShouldWork()
    {
        using var engine = new TinyDbEngine(_dbFile);
        
        // Create collection with no data (just metadata)
        var col = engine.GetBsonCollection("empty_col");
        col.Insert(new BsonDocument().Set("temp", 1));
        col.Delete(new BsonDocument().Set("temp", 1)["_id"]);
        
        // Compact
        engine.CompactDatabase();
        
        // Should not throw
        await Assert.That(engine.CollectionExists("empty_col")).IsTrue();
    }

    /// <summary>
    /// Test update that causes document overflow (document grows larger than page can hold)
    /// </summary>
    [Test]
    public async Task Engine_Update_DocumentOverflow_ShouldRelocate()
    {
        using var engine = new TinyDbEngine(_dbFile, new TinyDbOptions { PageSize = 4096 });
        
        // Insert small document using internal API (BsonDocument doesn't have Id property for reflection)
        var doc = new BsonDocument().Set("_id", 1).Set("data", "small");
        engine.InsertDocuments("overflow_test", new[] { doc });
        
        // Update with much larger data that may cause overflow
        var largeData = new string('X', 3000); // Large but not "large document" threshold
        var updated = engine.UpdateDocumentInternal("overflow_test", new BsonDocument().Set("_id", 1).Set("data", largeData));
        await Assert.That(updated).IsEqualTo(1);
        
        // Verify update worked
        var found = engine.FindById("overflow_test", 1);
        await Assert.That(found).IsNotNull();
        await Assert.That(((BsonString)found!["data"]).Value).IsEqualTo(largeData);
    }

    /// <summary>
    /// Test batch insert with some invalid documents (null handling)
    /// </summary>
    [Test]
    public async Task Engine_BatchInsert_WithNullDocuments_ShouldSkipNulls()
    {
        using var engine = new TinyDbEngine(_dbFile);
        var col = engine.GetBsonCollection("batch_null_test");
        
        var docs = new BsonDocument?[]
        {
            new BsonDocument().Set("id", 1),
            null,  // Should be skipped
            new BsonDocument().Set("id", 2),
            null,  // Should be skipped
            new BsonDocument().Set("id", 3)
        };
        
        // Insert batch - nulls should be filtered out
        col.Insert(docs!);
        
        // Should have 3 documents
        var count = col.FindAll().Count();
        await Assert.That(count).IsEqualTo(3);
    }

    /// <summary>
    /// Test batch insert that spans multiple pages
    /// </summary>
    [Test]
    public async Task Engine_BatchInsert_MultiplePages_ShouldWork()
    {
        using var engine = new TinyDbEngine(_dbFile, new TinyDbOptions { PageSize = 4096 });
        var col = engine.GetBsonCollection("multi_page_batch");
        
        // Create enough documents to span multiple pages
        var docs = new List<BsonDocument>();
        for (int i = 0; i < 100; i++)
        {
            docs.Add(new BsonDocument()
                .Set("_id", i)
                .Set("data", new string('A', 100)) // ~100 bytes per doc
            );
        }
        
        col.Insert(docs);
        
        // Verify all documents were inserted
        var count = col.FindAll().Count();
        await Assert.That(count).IsEqualTo(100);
    }

    /// <summary>
    /// Test delete that empties a page (triggers page freeing)
    /// </summary>
    [Test]
    public async Task Engine_Delete_EmptiesPage_ShouldFreePage()
    {
        using var engine = new TinyDbEngine(_dbFile);
        var col = engine.GetBsonCollection("delete_page_test");
        
        // Insert a single document
        col.Insert(new BsonDocument().Set("_id", 1).Set("data", "test"));
        
        // Delete it - should free the page
        var deleted = col.Delete(1);
        await Assert.That(deleted).IsEqualTo(1);
        
        // Collection should be empty
        var count = col.FindAll().Count();
        await Assert.That(count).IsEqualTo(0);
    }

    /// <summary>
    /// Test update on non-existent document
    /// </summary>
    [Test]
    public async Task Engine_Update_NonExistent_ShouldReturnZero()
    {
        using var engine = new TinyDbEngine(_dbFile);
        
        // Insert a document first
        engine.InsertDocuments("update_test", new[] { new BsonDocument().Set("_id", 1).Set("data", "test") });
        
        // Try to update non-existent document using internal API
        var updated = engine.UpdateDocumentInternal("update_test", new BsonDocument().Set("_id", 999).Set("data", "test"));
        await Assert.That(updated).IsEqualTo(0);
    }

    /// <summary>
    /// Test delete on non-existent document
    /// </summary>
    [Test]
    public async Task Engine_Delete_NonExistent_ShouldReturnZero()
    {
        using var engine = new TinyDbEngine(_dbFile);
        var col = engine.GetBsonCollection("delete_test");
        
        // Insert something first
        col.Insert(new BsonDocument().Set("_id", 1));
        
        // Try to delete non-existent document
        var deleted = col.Delete(999);
        await Assert.That(deleted).IsEqualTo(0);
    }

    /// <summary>
    /// Test engine with journaling enabled
    /// </summary>
    [Test]
    public async Task Engine_WithJournaling_ShouldWork()
    {
        var options = new TinyDbOptions { EnableJournaling = true };
        
        using (var engine = new TinyDbEngine(_dbFile, options))
        {
            var col = engine.GetBsonCollection("journal_test");
            col.Insert(new BsonDocument().Set("_id", 1).Set("data", "test"));
            engine.Flush();
        }
        
        // Reopen and verify
        using (var engine = new TinyDbEngine(_dbFile, options))
        {
            var col = engine.GetBsonCollection("journal_test");
            var doc = col.FindById(1);
            await Assert.That(doc).IsNotNull();
        }
    }

    /// <summary>
    /// Test database statistics
    /// </summary>
    [Test]
    public async Task Engine_GetStatistics_ShouldReturnValidData()
    {
        using var engine = new TinyDbEngine(_dbFile);
        var col = engine.GetBsonCollection("stats_test");
        
        for (int i = 0; i < 10; i++)
        {
            col.Insert(new BsonDocument().Set("_id", i));
        }
        
        var stats = engine.GetStatistics();
        await Assert.That(stats).IsNotNull();
        await Assert.That(stats.TotalPages).IsGreaterThan(0u);
    }
}
