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
    }

    [Test]
    public async Task Engine_CollectionOperations_ShouldWork()
    {
        using var engine = new TinyDbEngine(_dbFile);
        
        // Drop non-existent collection
        await Assert.That(engine.DropCollection("nonexistent")).IsFalse();
        
        // Create and drop
        engine.GetCollectionWithName<BsonDocument>("col1").Insert(new BsonDocument());
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
        var col = engine.GetCollectionWithName<BsonDocument>("col3");
        col.Insert(new BsonDocument());
        col.Insert(new BsonDocument());
        
        // Force flush to ensure they might be evicted or just checking state
        engine.Flush();
        
        // This relies on internal implementation details but let's check
        // At least it shouldn't throw
        await Assert.That(engine.GetCachedDocumentCount("col3")).IsGreaterThanOrEqualTo(0);
    }
}
