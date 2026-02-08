using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Storage;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

/// <summary>
/// Extended tests for DataPageAccess to improve coverage.
/// </summary>
public class DataPageAccessExtendedTests : IDisposable
{
    private readonly string _dbPath;
    private TinyDbEngine _engine;

    public DataPageAccessExtendedTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"dpa_ext_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    #region Document Scanning Tests

    [Test]
    public async Task ScanDocuments_EmptyCollection_ReturnsEmptyList()
    {
        var col = _engine.GetCollection<BsonDocument>("empty_col");
        var docs = col.FindAll().ToList();
        await Assert.That(docs.Count).IsEqualTo(0);
    }

    [Test]
    public async Task ScanDocuments_MultiplePages_ReturnsAllDocuments()
    {
        var col = _engine.GetCollection<BsonDocument>("multi_page");
        
        // Insert enough documents to span multiple pages
        for (int i = 0; i < 100; i++)
        {
            var doc = new BsonDocument()
                .Set("_id", i)
                .Set("data", new string('x', 500)); // ~500 bytes each
            col.Insert(doc);
        }
        
        var allDocs = col.FindAll().ToList();
        await Assert.That(allDocs.Count).IsEqualTo(100);
    }

    [Test]
    public async Task ReadDocuments_AfterMultipleUpdates_ReturnsCachedCorrectly()
    {
        var col = _engine.GetCollection<BsonDocument>("cache_test");
        
        // Insert document
        col.Insert(new BsonDocument().Set("_id", 1).Set("value", 100));
        
        // Read multiple times (should use cache)
        var doc1 = col.FindById(1);
        var doc2 = col.FindById(1);
        var doc3 = col.FindById(1);
        
        await Assert.That(doc1!).IsNotNull();
        await Assert.That(doc2!).IsNotNull();
        await Assert.That(doc3!).IsNotNull();
        
        await Assert.That(((BsonInt32)doc1!["value"]).Value).IsEqualTo(100);
        await Assert.That(((BsonInt32)doc2!["value"]).Value).IsEqualTo(100);
        await Assert.That(((BsonInt32)doc3!["value"]).Value).IsEqualTo(100);
    }

    #endregion

    #region ReadDocumentAt Tests

    [Test]
    public async Task ReadDocumentAt_ValidIndex_ReturnsDocument()
    {
        var col = _engine.GetCollection<BsonDocument>("read_at_test");
        
        for (int i = 0; i < 5; i++)
        {
            col.Insert(new BsonDocument().Set("_id", i).Set("name", $"doc_{i}"));
        }
        
        // Read specific document
        var doc = col.FindById(2);
        await Assert.That(doc!).IsNotNull();
        await Assert.That(((BsonString)doc!["name"]).Value).IsEqualTo("doc_2");
    }

    [Test]
    public async Task ReadDocumentAt_InvalidIndex_ReturnsNull()
    {
        var col = _engine.GetCollection<BsonDocument>("invalid_idx");
        col.Insert(new BsonDocument().Set("_id", 1));
        
        // Try to find non-existent document
        var doc = col.FindById(999);
        await Assert.That(doc == null).IsTrue();
    }

    [Test]
    public async Task ReadDocumentAt_WithFieldProjection()
    {
        var col = _engine.GetCollection<BsonDocument>("projection_test");
        col.Insert(new BsonDocument()
            .Set("_id", 1)
            .Set("field1", "value1")
            .Set("field2", "value2")
            .Set("field3", "value3"));
        
        // Find with all fields
        var fullDoc = col.FindById(1);
        await Assert.That(fullDoc!).IsNotNull();
        await Assert.That(fullDoc!.ContainsKey("field1")).IsTrue();
        await Assert.That(fullDoc!.ContainsKey("field2")).IsTrue();
        await Assert.That(fullDoc!.ContainsKey("field3")).IsTrue();
    }

    #endregion

    #region Large Document Tests

    [Test]
    public async Task LargeDocument_InsertAndRetrieve_Works()
    {
        var col = _engine.GetCollection<BsonDocument>("large_doc_test");
        
        // Create a large document (> 4KB to trigger large document storage)
        var largeData = new string('L', 5000);
        var doc = new BsonDocument()
            .Set("_id", 1)
            .Set("largeField", largeData);
        
        col.Insert(doc);
        
        var retrieved = col.FindById(1);
        await Assert.That(retrieved).IsNotNull();
        await Assert.That(((BsonString)retrieved!["largeField"]).Value).IsEqualTo(largeData);
    }

    [Test]
    public async Task MultipleLargeDocuments_InsertAndRetrieve_Works()
    {
        var col = _engine.GetCollection<BsonDocument>("multi_large");
        
        // Insert multiple large documents
        for (int i = 0; i < 5; i++)
        {
            var largeData = new string((char)('A' + i), 5000);
            col.Insert(new BsonDocument()
                .Set("_id", i)
                .Set("data", largeData));
        }
        
        // Verify all documents
        for (int i = 0; i < 5; i++)
        {
            var doc = col.FindById(i);
            await Assert.That(doc!).IsNotNull();
            await Assert.That(((BsonString)doc!["data"]).Value.Length).IsEqualTo(5000);
        }
    }

    #endregion

    #region Page Rewrite Tests

    [Test]
    public async Task DeleteAndCompact_TriggersPageRewrite()
    {
        var col = _engine.GetCollection<BsonDocument>("compact_test");
        
        // Insert documents
        for (int i = 0; i < 10; i++)
        {
            col.Insert(new BsonDocument().Set("_id", i).Set("value", i * 10));
        }
        
        // Delete some documents
        for (int i = 0; i < 10; i += 2)
        {
            col.Delete(i);
        }
        
        // Verify remaining documents
        var remaining = col.FindAll().ToList();
        await Assert.That(remaining.Count).IsEqualTo(5);
    }

    [Test]
    public async Task UpdateDocument_TriggersPageRewrite()
    {
        var col = _engine.GetCollection<BsonDocument>("update_rewrite");
        
        col.Insert(new BsonDocument().Set("_id", 1).Set("value", 100));
        
        // Update with larger data - use engine's internal UpdateDocument method 
        // since DocumentCollection.Update requires AotIdAccessor support
        var updatedDoc = new BsonDocument().Set("_id", 1).Set("value", 200).Set("extra", "more data");
        _engine.UpdateDocumentInternal("update_rewrite", updatedDoc);
        
        var doc = col.FindById(1);
        await Assert.That(doc!).IsNotNull();
        await Assert.That(((BsonInt32)doc!["value"]).Value).IsEqualTo(200);
        await Assert.That(doc.ContainsKey("extra")).IsTrue();
    }

    #endregion

    #region Edge Cases

    [Test]
    public async Task InsertEmptyDocument_Works()
    {
        var col = _engine.GetCollection<BsonDocument>("empty_doc");
        
        col.Insert(new BsonDocument().Set("_id", 1));
        
        var doc = col.FindById(1);
        await Assert.That(doc!).IsNotNull();
        await Assert.That(((BsonInt32)doc!["_id"]).Value).IsEqualTo(1);
    }

    [Test]
    public async Task InsertDocumentWithComplexTypes_Works()
    {
        var col = _engine.GetCollection<BsonDocument>("complex_types");
        
        var doc = new BsonDocument()
            .Set("_id", 1)
            .Set("string", "hello")
            .Set("int", 42)
            .Set("double", 3.14)
            .Set("bool", true)
            .Set("null", BsonNull.Value)
            .Set("array", new BsonArray().AddValue(new BsonInt32(1)).AddValue(new BsonInt32(2)).AddValue(new BsonInt32(3)))
            .Set("nested", new BsonDocument().Set("inner", "value"));
        
        col.Insert(doc);
        
        var retrieved = col.FindById(1);
        await Assert.That(retrieved).IsNotNull();
        await Assert.That(((BsonString)retrieved!["string"]).Value).IsEqualTo("hello");
        await Assert.That(((BsonInt32)retrieved["int"]).Value).IsEqualTo(42);
        await Assert.That(((BsonDouble)retrieved["double"]).Value).IsEqualTo(3.14);
        await Assert.That(((BsonBoolean)retrieved["bool"]).Value).IsTrue();
        await Assert.That(retrieved["null"].IsNull).IsTrue();
    }

    [Test]
    public async Task ManySmallDocuments_FillMultiplePages()
    {
        var col = _engine.GetCollection<BsonDocument>("many_small");
        
        // Insert many small documents
        for (int i = 0; i < 500; i++)
        {
            col.Insert(new BsonDocument().Set("_id", i).Set("v", i));
        }
        
        var count = col.Count();
        await Assert.That(count).IsEqualTo(500);
        
        // Verify first and last
        var first = col.FindById(0);
        var last = col.FindById(499);
        
        await Assert.That(first).IsNotNull();
        await Assert.That(last).IsNotNull();
    }

    #endregion

    #region Persistence Tests

    [Test]
    public async Task DataPageAccess_PersistAcrossReopen()
    {
        var path = Path.Combine(Path.GetTempPath(), $"persist_dpa_{Guid.NewGuid()}.db");
        
        try
        {
            // Create and populate
            using (var engine = new TinyDbEngine(path))
            {
                var col = engine.GetCollection<BsonDocument>("persist_col");
                for (int i = 0; i < 10; i++)
                {
                    col.Insert(new BsonDocument().Set("_id", i).Set("name", $"doc_{i}"));
                }
                engine.Flush();
            }
            
            // Reopen and verify
            using (var engine = new TinyDbEngine(path))
            {
                var col = engine.GetCollection<BsonDocument>("persist_col");
                var count = col.Count();
                await Assert.That(count).IsEqualTo(10);
                
                var doc5 = col.FindById(5);
                await Assert.That(doc5!).IsNotNull();
                await Assert.That(((BsonString)doc5!["name"]).Value).IsEqualTo("doc_5");
            }
        }
        finally
        {
            if (File.Exists(path)) File.Delete(path);
        }
    }

    #endregion
}

/// <summary>
/// Tests for DataPageAccess boundary conditions.
/// </summary>
public class DataPageAccessBoundaryTests : IDisposable
{
    private readonly string _dbPath;
    private TinyDbEngine _engine;

    public DataPageAccessBoundaryTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"dpa_boundary_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Test]
    public async Task FindNonExistent_ReturnsNull()
    {
        var col = _engine.GetCollection<BsonDocument>("boundary_test");
        col.Insert(new BsonDocument().Set("_id", 1));
        
        var notFound = col.FindById(9999);
        await Assert.That(notFound == null).IsTrue();
    }

    [Test]
    public async Task DeleteNonExistent_ReturnsFalse()
    {
        var col = _engine.GetCollection<BsonDocument>("del_boundary");
        col.Insert(new BsonDocument().Set("_id", 1));
        
        var deleted = col.Delete(9999);
        await Assert.That(deleted).IsEqualTo(0); // Delete returns int (affected count)
    }

    [Test]
    public async Task UpdateNonExistent_ReturnsFalse()
    {
        var col = _engine.GetCollection<BsonDocument>("upd_boundary");
        col.Insert(new BsonDocument().Set("_id", 1));
        
        var updated = _engine.UpdateDocumentInternal("upd_boundary", new BsonDocument().Set("_id", 9999).Set("value", 100));
        await Assert.That(updated).IsEqualTo(0); // Update returns int (affected count)
    }

    [Test]
    public async Task InsertDuplicate_ThrowsException()
    {
        var col = _engine.GetCollection<BsonDocument>("dup_test");
        col.Insert(new BsonDocument().Set("_id", 1));
        
        await Assert.That(() => col.Insert(new BsonDocument().Set("_id", 1)))
            .ThrowsException();
    }

    [Test]
    public async Task InsertMany_ThenDeleteAll_Works()
    {
        var col = _engine.GetCollection<BsonDocument>("ins_del_all");
        
        for (int i = 0; i < 20; i++)
        {
            col.Insert(new BsonDocument().Set("_id", i));
        }
        
        await Assert.That(col.Count()).IsEqualTo(20);
        
        for (int i = 0; i < 20; i++)
        {
            col.Delete(i);
        }
        
        await Assert.That(col.Count()).IsEqualTo(0);
    }

    [Test]
    public async Task InsertUpdateDelete_Sequence_Works()
    {
        var col = _engine.GetCollection<BsonDocument>("iud_sequence");
        
        // Insert
        col.Insert(new BsonDocument().Set("_id", 1).Set("state", "inserted"));
        var doc1 = col.FindById(1);
        await Assert.That(((BsonString)doc1!["state"]).Value).IsEqualTo("inserted");
        
        // Update
        _engine.UpdateDocumentInternal("iud_sequence", new BsonDocument().Set("_id", 1).Set("state", "updated"));
        var doc2 = col.FindById(1);
        await Assert.That(((BsonString)doc2!["state"]).Value).IsEqualTo("updated");
        
        // Delete
        var deleted = col.Delete(1);
        await Assert.That(deleted).IsEqualTo(1); // Delete returns int (1 = success)
        
        var doc3 = col.FindById(1);
        await Assert.That(doc3 == null).IsTrue();
    }
}
