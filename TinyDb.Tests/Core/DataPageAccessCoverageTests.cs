using System;
using System.Collections.Generic;
using System.Reflection;
using System.Linq;
using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Storage;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class DataPageAccessCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private TinyDbEngine _engine;

    public DataPageAccessCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"dpa_test_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Test]
    public async Task ScanDocumentsFromPage_Should_Return_All_Documents()
    {
        var col = _engine.GetCollectionWithName<BsonDocument>("test");
        var docs = new List<BsonDocument>();
        for (int i = 0; i < 5; i++)
        {
            var d = new BsonDocument().Set("_id", i).Set("val", "test" + i);
            col.Insert(d);
            docs.Add(d);
        }

        var dpa = GetField<object>(_engine, "_dataPageAccess");
        var pm = GetField<PageManager>(_engine, "_pageManager");
        
        Page dataPage = null!;
        for (uint i = 1; i < 10; i++)
        {
            try 
            {
                var p = pm.GetPage(i);
                if (p.Header.PageType == PageType.Data && p.Header.ItemCount > 0)
                {
                    dataPage = p;
                    break;
                }
            } catch { }
        }
        
        await Assert.That(dataPage).IsNotNull();

        // Invoke ScanDocumentsFromPage
        var method = dpa.GetType().GetMethod("ScanDocumentsFromPage", BindingFlags.Public | BindingFlags.Instance);
        var result = (IEnumerable<BsonDocument>)method!.Invoke(dpa, new object[] { dataPage })!;
        
        var list = result.ToList();
        await Assert.That(list.Count).IsGreaterThan(0);
        await Assert.That(((BsonInt32)list[0]["_id"]).Value).IsEqualTo(0);
    }

    [Test]
    public async Task ReadDocumentAt_WithFields_Should_Project()
    {
        var col = _engine.GetCollectionWithName<BsonDocument>("test_proj");
        col.Insert(new BsonDocument().Set("_id", 1).Set("a", 10).Set("b", 20).Set("c", 30));
        
        var dpa = GetField<object>(_engine, "_dataPageAccess");
        var pm = GetField<PageManager>(_engine, "_pageManager");
        
        Page dataPage = null!;
        for (uint i = 1; i < 10; i++)
        {
            try 
            {
                var p = pm.GetPage(i);
                if (p.Header.PageType == PageType.Data && p.Header.ItemCount > 0)
                {
                    dataPage = p;
                    break;
                }
            } catch { }
        }
        
        await Assert.That(dataPage).IsNotNull();
        
        // Invoke ReadDocumentAt(Page p, int index, HashSet<string>? fields)
        var method = dpa.GetType().GetMethod("ReadDocumentAt", new[] { typeof(Page), typeof(int), typeof(HashSet<string>) });
        
        var fields = new HashSet<string> { "a", "c" };
        var doc = (BsonDocument)method!.Invoke(dpa, new object[] { dataPage, 0, fields })!;
        
        await Assert.That(doc.Count).IsEqualTo(2);
        await Assert.That(doc.ContainsKey("a")).IsTrue();
        await Assert.That(doc.ContainsKey("c")).IsTrue();
        await Assert.That(doc.ContainsKey("b")).IsFalse();
    }

    [Test]
    public async Task ScanRawDocumentsFromPage_Should_Return_Memory()
    {
        var col = _engine.GetCollectionWithName<BsonDocument>("test_raw");
        col.Insert(new BsonDocument().Set("_id", 1));
        
        var dpa = GetField<object>(_engine, "_dataPageAccess");
        var pm = GetField<PageManager>(_engine, "_pageManager");
        
        Page dataPage = null!;
        for (uint i = 1; i < 10; i++)
        {
            try 
            {
                var p = pm.GetPage(i);
                if (p.Header.PageType == PageType.Data && p.Header.ItemCount > 0)
                {
                    dataPage = p;
                    break;
                }
            } catch { }
        }
        
        await Assert.That(dataPage).IsNotNull();
        
        var method = dpa.GetType().GetMethod("ScanRawDocumentsFromPage");
        var result = (IEnumerable<ReadOnlyMemory<byte>>)method!.Invoke(dpa, new object[] { dataPage })!;
        
        var list = result.ToList();
        await Assert.That(list.Count).IsGreaterThan(0);
        
        // Verify we can deserialize
        var doc = BsonSerializer.DeserializeDocument(list[0]);
        await Assert.That(((BsonInt32)doc["_id"]).Value).IsEqualTo(1);
    }

    private T GetField<T>(object instance, string name)
    {
        var field = instance.GetType().GetField(name, BindingFlags.NonPublic | BindingFlags.Instance);
        if (field == null)
        {
             // Try base type
             field = instance.GetType().BaseType?.GetField(name, BindingFlags.NonPublic | BindingFlags.Instance);
        }
        return (T)field!.GetValue(instance)!;
    }
}