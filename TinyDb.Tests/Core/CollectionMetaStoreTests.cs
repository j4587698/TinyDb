using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Storage;
using TinyDb.Tests.Utils;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

/// <summary>
/// Tests for CollectionMetaStore internal class.
/// With InternalsVisibleTo, we can directly access internal members.
/// </summary>
public class CollectionMetaStoreTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public CollectionMetaStoreTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"meta_store_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    private CollectionMetaStore GetCollectionMetaStore() => _engine._collectionMetaStore;

    [Test]
    public async Task GetMetadata_UnknownCollection_ReturnsEmptyDocument()
    {
        var metaStore = GetCollectionMetaStore();
        var metadata = metaStore.GetMetadata("unknown_collection");
        
        await Assert.That(metadata).IsNotNull();
        await Assert.That(metadata.Count()).IsEqualTo(0);
    }

    [Test]
    public async Task GetMetadata_RegisteredCollection_ReturnsEmptyDocument()
    {
        // Register collection via engine
        _engine.GetCollection<TestItem>("test_collection");
        
        var metaStore = GetCollectionMetaStore();
        var metadata = metaStore.GetMetadata("test_collection");
        
        await Assert.That(metadata).IsNotNull();
        await Assert.That(metadata.Count()).IsEqualTo(0);
    }

    [Test]
    public async Task UpdateMetadata_NewCollection_ShouldRegisterAndSave()
    {
        var metaStore = GetCollectionMetaStore();
        
        var metadata = new BsonDocument()
            .Set("rootIndexPage", new BsonInt32(123))
            .Set("customProperty", new BsonString("test value"));
        
        metaStore.UpdateMetadata("new_collection_with_meta", metadata, true);
        
        // Verify it's registered
        await Assert.That(metaStore.IsKnown("new_collection_with_meta")).IsTrue();
        
        // Verify metadata was stored
        var retrieved = metaStore.GetMetadata("new_collection_with_meta");
        await Assert.That(((BsonInt32)retrieved["rootIndexPage"]).Value).IsEqualTo(123);
        await Assert.That(((BsonString)retrieved["customProperty"]).Value).IsEqualTo("test value");
    }

    [Test]
    public async Task UpdateMetadata_ExistingCollection_ShouldUpdateMetadata()
    {
        var metaStore = GetCollectionMetaStore();
        
        // First update - creates collection
        var metadata1 = new BsonDocument().Set("version", new BsonInt32(1));
        metaStore.UpdateMetadata("existing_collection", metadata1, false);
        
        // Second update - updates metadata
        var metadata2 = new BsonDocument()
            .Set("version", new BsonInt32(2))
            .Set("newField", new BsonString("new value"));
        metaStore.UpdateMetadata("existing_collection", metadata2, true);
        
        // Verify updated metadata
        var retrieved = metaStore.GetMetadata("existing_collection");
        await Assert.That(((BsonInt32)retrieved["version"]).Value).IsEqualTo(2);
        await Assert.That(((BsonString)retrieved["newField"]).Value).IsEqualTo("new value");
    }

    [Test]
    public async Task UpdateMetadata_Persistence_ShouldSurviveReopen()
    {
        var path = Path.Combine(Path.GetTempPath(), $"meta_persist_{Guid.NewGuid():N}.db");
        
        try
        {
            // Create and update metadata
            using (var engine = new TinyDbEngine(path))
            {
                var metaStore = engine._collectionMetaStore;
                
                var metadata = new BsonDocument()
                    .Set("persistedValue", new BsonInt32(42))
                    .Set("persistedString", new BsonString("hello world"));
                
                metaStore.UpdateMetadata("persistent_meta_col", metadata, true);
                
                engine.Flush();
            }
            
            // Reopen and verify
            using (var engine = new TinyDbEngine(path))
            {
                var metaStore = engine._collectionMetaStore;
                
                var retrieved = metaStore.GetMetadata("persistent_meta_col");
                
                await Assert.That(((BsonInt32)retrieved["persistedValue"]).Value).IsEqualTo(42);
                await Assert.That(((BsonString)retrieved["persistedString"]).Value).IsEqualTo("hello world");
            }
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task GetCollectionNames_ReturnsAllRegisteredCollections()
    {
        var metaStore = GetCollectionMetaStore();
        
        // Register multiple collections through the engine
        _engine.GetCollection<TestItem>("col_a");
        _engine.GetCollection<TestItem>("col_b");
        _engine.GetCollection<TestItem>("col_c");
        
        var names = metaStore.GetCollectionNames();
        
        await Assert.That(names).Contains("col_a");
        await Assert.That(names).Contains("col_b");
        await Assert.That(names).Contains("col_c");
    }

    [Test]
    public async Task IsKnown_UnknownCollection_ReturnsFalse()
    {
        var metaStore = GetCollectionMetaStore();
        await Assert.That(metaStore.IsKnown("does_not_exist")).IsFalse();
    }

    [Test]
    public async Task IsKnown_RegisteredCollection_ReturnsTrue()
    {
        _engine.GetCollection<TestItem>("is_known_test");
        var metaStore = GetCollectionMetaStore();
        await Assert.That(metaStore.IsKnown("is_known_test")).IsTrue();
    }

    [Test]
    public async Task RegisterCollection_DuplicateRegistration_ShouldNotDuplicate()
    {
        // Register same collection multiple times
        _engine.GetCollection<TestItem>("duplicate_test");
        _engine.GetCollection<TestItem>("duplicate_test");
        _engine.GetCollection<TestItem>("duplicate_test");
        
        var names = _engine.GetCollectionNames().ToList();
        var count = names.Count(n => n == "duplicate_test");
        
        await Assert.That(count).IsEqualTo(1);
    }

    [Test]
    public async Task RemoveCollection_ExistingCollection_ShouldRemove()
    {
        _engine.GetCollection<TestItem>("to_remove");
        var metaStore = GetCollectionMetaStore();
        
        await Assert.That(metaStore.IsKnown("to_remove")).IsTrue();
        
        _engine.DropCollection("to_remove");
        
        await Assert.That(metaStore.IsKnown("to_remove")).IsFalse();
    }

    [Test]
    public async Task RemoveCollection_NonExistentCollection_ShouldNotThrow()
    {
        var result = _engine.DropCollection("never_existed");
        await Assert.That(result).IsFalse();
    }

    [Test]
    public async Task MultipleMetadataUpdates_ShouldMaintainCorrectState()
    {
        var metaStore = GetCollectionMetaStore();
        
        // Create multiple collections with metadata
        for (int i = 0; i < 5; i++)
        {
            var metadata = new BsonDocument()
                .Set("index", new BsonInt32(i))
                .Set("name", new BsonString($"collection_{i}"));
            metaStore.UpdateMetadata($"multi_col_{i}", metadata, false);
        }
        
        // Force flush
        _engine.Flush();
        
        // Verify all collections exist with correct metadata
        for (int i = 0; i < 5; i++)
        {
            var retrieved = metaStore.GetMetadata($"multi_col_{i}");
            await Assert.That(((BsonInt32)retrieved["index"]).Value).IsEqualTo(i);
            await Assert.That(((BsonString)retrieved["name"]).Value).IsEqualTo($"collection_{i}");
        }
    }

    [Test]
    public async Task UpdateMetadata_WithComplexDocument_ShouldPersist()
    {
        var metaStore = GetCollectionMetaStore();
        
        var nestedDoc = new BsonDocument()
            .Set("nestedInt", new BsonInt32(100))
            .Set("nestedString", new BsonString("nested value"));
        
        var arrayValues = new BsonArray()
            .AddValue(new BsonInt32(1))
            .AddValue(new BsonInt32(2))
            .AddValue(new BsonInt32(3));
        
        var metadata = new BsonDocument()
            .Set("nested", nestedDoc)
            .Set("array", arrayValues)
            .Set("boolean", BsonBoolean.True)
            .Set("double", new BsonDouble(3.14));
        
        metaStore.UpdateMetadata("complex_meta_col", metadata, true);
        
        var retrieved = metaStore.GetMetadata("complex_meta_col");
        
        var nestedResult = (BsonDocument)retrieved["nested"];
        await Assert.That(((BsonInt32)nestedResult["nestedInt"]).Value).IsEqualTo(100);
        await Assert.That(((BsonString)nestedResult["nestedString"]).Value).IsEqualTo("nested value");
        await Assert.That(((BsonArray)retrieved["array"]).Count).IsEqualTo(3);
        await Assert.That(((BsonBoolean)retrieved["boolean"]).Value).IsTrue();
        await Assert.That(((BsonDouble)retrieved["double"]).Value).IsEqualTo(3.14);
    }

    [Entity]
    public class TestItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }
}

/// <summary>
/// Tests for CollectionMetaStore edge cases and error conditions.
/// </summary>
public class CollectionMetaStoreEdgeCasesTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public CollectionMetaStoreEdgeCasesTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"meta_edge_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    private CollectionMetaStore GetCollectionMetaStore() => _engine._collectionMetaStore;

    [Test]
    public async Task Collection_WithEmptyName_ShouldWork()
    {
        // Empty string as collection name (edge case)
        var metaStore = GetCollectionMetaStore();
        
        metaStore.RegisterCollection("", true);
        
        await Assert.That(metaStore.IsKnown("")).IsTrue();
    }

    [Test]
    public async Task Collection_WithSpecialCharacters_ShouldWork()
    {
        _engine.GetCollection<TestItem>("col-with-dashes");
        _engine.GetCollection<TestItem>("col_with_underscores");
        _engine.GetCollection<TestItem>("col.with.dots");
        
        var names = _engine.GetCollectionNames().ToList();
        
        await Assert.That(names).Contains("col-with-dashes");
        await Assert.That(names).Contains("col_with_underscores");
        await Assert.That(names).Contains("col.with.dots");
    }

    [Test]
    public async Task Collection_WithUnicodeCharacters_ShouldWork()
    {
        _engine.GetCollection<TestItem>("集合名称");
        _engine.GetCollection<TestItem>("コレクション");
        _engine.GetCollection<TestItem>("коллекция");
        
        var names = _engine.GetCollectionNames().ToList();
        
        await Assert.That(names).Contains("集合名称");
        await Assert.That(names).Contains("コレクション");
        await Assert.That(names).Contains("коллекция");
    }

    [Test]
    public async Task Collection_CaseSensitivity_ShouldBeDistinct()
    {
        _engine.GetCollection<TestItem>("CaseSensitive");
        _engine.GetCollection<TestItem>("casesensitive");
        _engine.GetCollection<TestItem>("CASESENSITIVE");
        
        var names = _engine.GetCollectionNames().ToList();
        
        await Assert.That(names).Contains("CaseSensitive");
        await Assert.That(names).Contains("casesensitive");
        await Assert.That(names).Contains("CASESENSITIVE");
        await Assert.That(names.Count).IsGreaterThanOrEqualTo(3);
    }

    [Test]
    public async Task LoadCollections_WithNoExistingData_ShouldNotThrow()
    {
        var path = Path.Combine(Path.GetTempPath(), $"empty_load_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(path);
            var names = engine.GetCollectionNames().ToList();
            await Assert.That(names).IsEmpty();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task SaveCollections_WithEmptyMetadata_ShouldSaveSuccessfully()
    {
        _engine.GetCollection<TestItem>("empty_meta_test");
        _engine.Flush();
        
        await Assert.That(_engine.CollectionExists("empty_meta_test")).IsTrue();
    }

    [Test]
    public async Task GetMetadata_AfterRemoveCollection_ShouldReturnEmptyDocument()
    {
        var metaStore = GetCollectionMetaStore();
        
        // Add collection with metadata
        var metadata = new BsonDocument().Set("test", new BsonInt32(123));
        metaStore.UpdateMetadata("temp_collection", metadata, false);
        
        // Remove collection
        _engine.DropCollection("temp_collection");
        
        // Get metadata - should return empty document
        var result = metaStore.GetMetadata("temp_collection");
        
        await Assert.That(result.Count()).IsEqualTo(0);
    }

    [Test]
    public async Task UpdateMetadata_MultipleTimes_ShouldOverwritePrevious()
    {
        var metaStore = GetCollectionMetaStore();
        
        // First update
        var meta1 = new BsonDocument().Set("version", new BsonInt32(1));
        metaStore.UpdateMetadata("overwrite_test", meta1, false);
        
        // Second update - completely different structure
        var meta2 = new BsonDocument().Set("newKey", new BsonString("new value"));
        metaStore.UpdateMetadata("overwrite_test", meta2, true);
        
        var result = metaStore.GetMetadata("overwrite_test");
        
        // Should have new key, not old key
        await Assert.That(result.ContainsKey("newKey")).IsTrue();
        await Assert.That(result.ContainsKey("version")).IsFalse();
    }

    [Entity]
    public class TestItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }
}

/// <summary>
/// Tests for CollectionMetaStore persistence across database sessions.
/// </summary>
public class CollectionMetaStorePersistenceTests
{
    [Test]
    public async Task Persistence_WithMultipleCollections_ShouldSurviveReopen()
    {
        var path = Path.Combine(Path.GetTempPath(), $"meta_persist_multi_{Guid.NewGuid():N}.db");
        
        try
        {
            // Create multiple collections with data
            using (var engine = new TinyDbEngine(path))
            {
                engine.GetCollection<TestItem>("persist_a").Insert(new TestItem { Id = 1 });
                engine.GetCollection<TestItem>("persist_b").Insert(new TestItem { Id = 2 });
                engine.GetCollection<TestItem>("persist_c").Insert(new TestItem { Id = 3 });
                engine.Flush();
            }
            
            // Reopen and verify
            using (var engine = new TinyDbEngine(path))
            {
                var names = engine.GetCollectionNames().ToList();
                await Assert.That(names).Contains("persist_a");
                await Assert.That(names).Contains("persist_b");
                await Assert.That(names).Contains("persist_c");
                await Assert.That(names.Count).IsEqualTo(3);
            }
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Persistence_DropCollectionThenReopen_ShouldNotHaveDroppedCollection()
    {
        var path = Path.Combine(Path.GetTempPath(), $"meta_drop_persist_{Guid.NewGuid():N}.db");
        
        try
        {
            // Create and then drop a collection
            using (var engine = new TinyDbEngine(path))
            {
                engine.GetCollection<TestItem>("to_keep");
                engine.GetCollection<TestItem>("to_drop");
                engine.Flush();
                
                engine.DropCollection("to_drop");
                engine.Flush();
            }
            
            // Reopen and verify
            using (var engine = new TinyDbEngine(path))
            {
                var names = engine.GetCollectionNames().ToList();
                await Assert.That(names).Contains("to_keep");
                await Assert.That(names).DoesNotContain("to_drop");
            }
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Persistence_MetadataUpdate_ShouldSurviveReopen()
    {
        var path = Path.Combine(Path.GetTempPath(), $"meta_update_persist_{Guid.NewGuid():N}.db");
        
        try
        {
            // Create collection with custom metadata
            using (var engine = new TinyDbEngine(path))
            {
                var metaStore = engine._collectionMetaStore;
                
                var metadata = new BsonDocument()
                    .Set("customInt", new BsonInt32(999))
                    .Set("customString", new BsonString("persisted data"));
                
                metaStore.UpdateMetadata("meta_persist_test", metadata, true);
                
                engine.Flush();
            }
            
            // Reopen and verify metadata persisted
            using (var engine = new TinyDbEngine(path))
            {
                var metaStore = engine._collectionMetaStore;
                
                var retrieved = metaStore.GetMetadata("meta_persist_test");
                
                await Assert.That(((BsonInt32)retrieved["customInt"]).Value).IsEqualTo(999);
                await Assert.That(((BsonString)retrieved["customString"]).Value).IsEqualTo("persisted data");
            }
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Persistence_EmptyDatabase_ShouldLoadWithoutErrors()
    {
        var path = Path.Combine(Path.GetTempPath(), $"empty_db_{Guid.NewGuid():N}.db");
        
        try
        {
            // Create empty database
            using (var engine = new TinyDbEngine(path))
            {
                engine.Flush();
            }
            
            // Reopen empty database
            using (var engine = new TinyDbEngine(path))
            {
                var names = engine.GetCollectionNames().ToList();
                await Assert.That(names).IsEmpty();
            }
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Persistence_ManyCollections_ShouldPersistAll()
    {
        var path = Path.Combine(Path.GetTempPath(), $"many_cols_{Guid.NewGuid():N}.db");
        const int collectionCount = 20;
        
        try
        {
            // Create many collections
            using (var engine = new TinyDbEngine(path))
            {
                for (int i = 0; i < collectionCount; i++)
                {
                    engine.GetCollection<TestItem>($"collection_{i:D3}");
                }
                engine.Flush();
            }
            
            // Reopen and verify all collections exist
            using (var engine = new TinyDbEngine(path))
            {
                var names = engine.GetCollectionNames().ToList();
                await Assert.That(names.Count).IsEqualTo(collectionCount);
                
                for (int i = 0; i < collectionCount; i++)
                {
                    await Assert.That(names).Contains($"collection_{i:D3}");
                }
            }
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Entity]
    public class TestItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }
}
