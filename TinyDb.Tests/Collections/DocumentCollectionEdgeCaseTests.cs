using System.IO;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Collections;

/// <summary>
/// Edge case tests for DocumentCollection to improve coverage
/// </summary>
public class DocumentCollectionEdgeCaseTests
{
    private string _databasePath = null!;
    private TinyDbEngine _engine = null!;
    private DocumentCollection<TestEntity> _collection = null!;

    [Before(Test)]
    public void Setup()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"doc_col_edge_{Guid.NewGuid():N}.db");
        var options = new TinyDbOptions
        {
            DatabaseName = "DocumentCollectionEdgeTestDb",
            PageSize = 4096,
            CacheSize = 100
        };

        _engine = new TinyDbEngine(_databasePath, options);
        _collection = (DocumentCollection<TestEntity>)_engine.GetCollection<TestEntity>();
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_databasePath))
        {
            File.Delete(_databasePath);
        }
    }

    #region Insert Edge Cases

    [Test]
    public async Task Insert_WithNullEntity_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.Insert((TestEntity)null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Insert_Batch_WithNullsInCollection_ShouldSkipNulls()
    {
        var entities = new List<TestEntity?>
        {
            new TestEntity { Name = "First" },
            null,
            new TestEntity { Name = "Third" },
            null,
            new TestEntity { Name = "Fifth" }
        };

        // The method accepts IEnumerable<T> where T : class, new()
        // so we need to cast to avoid null warning
        var count = _collection.Insert(entities!);

        // Only 3 non-null entities should be inserted
        await Assert.That(count).IsEqualTo(3);
        await Assert.That(_collection.Count()).IsEqualTo(3);
    }

    [Test]
    public async Task Insert_Batch_WithNullCollection_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.Insert((IEnumerable<TestEntity>)null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Insert_Batch_EmptyCollection_ShouldReturnZero()
    {
        var entities = new List<TestEntity>();

        var count = _collection.Insert(entities);

        await Assert.That(count).IsEqualTo(0);
        await Assert.That(_collection.Count()).IsEqualTo(0);
    }

    [Test]
    public async Task Insert_Batch_LargeCollection_ShouldBatchCorrectly()
    {
        var entities = new List<TestEntity>();
        for (int i = 0; i < 2500; i++) // More than BatchSize (1000)
        {
            entities.Add(new TestEntity { Name = $"Entity{i}" });
        }

        var count = _collection.Insert(entities);

        await Assert.That(count).IsEqualTo(2500);
        await Assert.That(_collection.Count()).IsEqualTo(2500);
    }

    #endregion

    #region Update Edge Cases

    [Test]
    public async Task Update_WithNullEntity_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.Update((TestEntity)null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Update_EntityWithoutId_ShouldThrowArgumentException()
    {
        var entity = new TestEntity { Name = "NoId", Id = ObjectId.Empty };
        // Id is explicitly set to empty ObjectId

        await Assert.That(() => _collection.Update(entity))
            .Throws<ArgumentException>();
    }

    [Test]
    public async Task Update_NonExistentDocument_InTransaction_ShouldConvertToInsert()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_update_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var collection = (DocumentCollection<TestEntity>)engine.GetCollection<TestEntity>();
            
            using var transaction = engine.BeginTransaction();

            var entity = new TestEntity 
            { 
                Id = ObjectId.NewObjectId(), 
                Name = "NonExistent" 
            };

            // Update a non-existent document in transaction should insert it
            var count = collection.Update(entity);

            await Assert.That(count).IsEqualTo(1);

            transaction.Commit();

            // Verify the document was inserted
            var found = collection.FindById(entity.Id);
            await Assert.That(found).IsNotNull();
            await Assert.That(found!.Name).IsEqualTo("NonExistent");
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    [Test]
    public async Task Update_Batch_WithNullCollection_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.Update((IEnumerable<TestEntity>)null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Update_Batch_WithNullsInCollection_ShouldSkipNulls()
    {
        // First insert some entities
        var entity1 = new TestEntity { Name = "First" };
        var entity2 = new TestEntity { Name = "Second" };
        _collection.Insert(entity1);
        _collection.Insert(entity2);

        // Update with some nulls
        entity1.Name = "Updated First";
        entity2.Name = "Updated Second";

        var entities = new List<TestEntity?> { entity1, null, entity2 };
        var count = _collection.Update(entities!);

        await Assert.That(count).IsEqualTo(2);
    }

    #endregion

    #region Delete Edge Cases

    [Test]
    public async Task Delete_WithNullId_ShouldReturnZero()
    {
        var count = _collection.Delete((BsonValue)null!);

        await Assert.That(count).IsEqualTo(0);
    }

    [Test]
    public async Task Delete_WithBsonNullValue_ShouldReturnZero()
    {
        var count = _collection.Delete(BsonNull.Value);

        await Assert.That(count).IsEqualTo(0);
    }

    [Test]
    public async Task Delete_NonExistentId_ShouldReturnZero()
    {
        var count = _collection.Delete(ObjectId.NewObjectId());

        await Assert.That(count).IsEqualTo(0);
    }

    [Test]
    public async Task Delete_Batch_WithNullCollection_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.Delete((IEnumerable<BsonValue>)null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Delete_Batch_WithNullsInCollection_ShouldSkipNulls()
    {
        var entity1 = new TestEntity { Name = "First" };
        var entity2 = new TestEntity { Name = "Second" };
        _collection.Insert(entity1);
        _collection.Insert(entity2);

        var ids = new List<BsonValue?> { entity1.Id, null, BsonNull.Value, entity2.Id };
        var count = _collection.Delete(ids!);

        await Assert.That(count).IsEqualTo(2);
        await Assert.That(_collection.Count()).IsEqualTo(0);
    }

    [Test]
    public async Task Delete_InTransaction_NonExistentDocument_ShouldReturnZero()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_del_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var collection = (DocumentCollection<TestEntity>)engine.GetCollection<TestEntity>();
            
            using var transaction = engine.BeginTransaction();

            var count = collection.Delete(ObjectId.NewObjectId());

            await Assert.That(count).IsEqualTo(0);

            transaction.Commit();
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    #endregion

    #region FindById Edge Cases

    [Test]
    public async Task FindById_WithNullId_ShouldReturnNull()
    {
        var result = _collection.FindById((BsonValue)null!);

        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task FindById_WithBsonNullValue_ShouldReturnNull()
    {
        var result = _collection.FindById(BsonNull.Value);

        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task FindById_NonExistentId_ShouldReturnNull()
    {
        var result = _collection.FindById(ObjectId.NewObjectId());

        await Assert.That(result).IsNull();
    }

    #endregion

    #region Find Edge Cases

    [Test]
    public async Task Find_WithNullPredicate_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.Find(null!).ToList())
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task FindOne_WithNullPredicate_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.FindOne(null!))
            .Throws<ArgumentNullException>();
    }

    #endregion

    #region Count Edge Cases

    [Test]
    public async Task Count_WithNullPredicate_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.Count(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Count_InTransaction_ShouldIncludePendingInserts()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_count_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var collection = (DocumentCollection<TestEntity>)engine.GetCollection<TestEntity>();
            
            // Insert some initial data
            collection.Insert(new TestEntity { Name = "Initial" });

            using var transaction = engine.BeginTransaction();

            // Insert in transaction
            collection.Insert(new TestEntity { Name = "InTransaction" });

            // Count should include the pending insert
            var count = collection.Count();
            await Assert.That(count).IsEqualTo(2);

            transaction.Commit();

            // After commit, count should still be 2
            await Assert.That(collection.Count()).IsEqualTo(2);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    #endregion

    #region Exists Edge Cases

    [Test]
    public async Task Exists_WithNullPredicate_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.Exists(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Exists_WithNoMatchingDocuments_ShouldReturnFalse()
    {
        _collection.Insert(new TestEntity { Name = "Test" });

        var exists = _collection.Exists(e => e.Name == "NonExistent");

        await Assert.That(exists).IsFalse();
    }

    [Test]
    public async Task Exists_WithMatchingDocument_ShouldReturnTrue()
    {
        _collection.Insert(new TestEntity { Name = "Test" });

        var exists = _collection.Exists(e => e.Name == "Test");

        await Assert.That(exists).IsTrue();
    }

    #endregion

    #region DeleteMany Edge Cases

    [Test]
    public async Task DeleteMany_WithNullPredicate_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.DeleteMany(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task DeleteMany_WithNoMatchingDocuments_ShouldReturnZero()
    {
        _collection.Insert(new TestEntity { Name = "Test" });

        var count = _collection.DeleteMany(e => e.Name == "NonExistent");

        await Assert.That(count).IsEqualTo(0);
        await Assert.That(_collection.Count()).IsEqualTo(1);
    }

    [Test]
    public async Task DeleteMany_WithMatchingDocuments_ShouldDeleteAll()
    {
        _collection.Insert(new TestEntity { Name = "Delete" });
        _collection.Insert(new TestEntity { Name = "Delete" });
        _collection.Insert(new TestEntity { Name = "Keep" });

        var count = _collection.DeleteMany(e => e.Name == "Delete");

        await Assert.That(count).IsEqualTo(2);
        await Assert.That(_collection.Count()).IsEqualTo(1);
    }

    #endregion

    #region DeleteAll Edge Cases

    [Test]
    public async Task DeleteAll_WithEmptyCollection_ShouldReturnZero()
    {
        var count = _collection.DeleteAll();

        await Assert.That(count).IsEqualTo(0);
    }

    [Test]
    public async Task DeleteAll_WithDocuments_ShouldDeleteAll()
    {
        _collection.Insert(new TestEntity { Name = "First" });
        _collection.Insert(new TestEntity { Name = "Second" });
        _collection.Insert(new TestEntity { Name = "Third" });

        var count = _collection.DeleteAll();

        await Assert.That(count).IsEqualTo(3);
        await Assert.That(_collection.Count()).IsEqualTo(0);
    }

    #endregion

    #region Upsert Edge Cases

    [Test]
    public async Task Upsert_WithNullEntity_ShouldThrowArgumentNullException()
    {
        await Assert.That(() => _collection.Upsert(null!))
            .Throws<ArgumentNullException>();
    }

    [Test]
    public async Task Upsert_NewEntity_ShouldInsert()
    {
        var entity = new TestEntity { Name = "New" };

        var (updateType, count) = _collection.Upsert(entity);

        await Assert.That(updateType).IsEqualTo(UpdateType.Insert);
        await Assert.That(count).IsEqualTo(1);
        await Assert.That(_collection.Count()).IsEqualTo(1);
    }

    [Test]
    public async Task Upsert_ExistingEntity_ShouldUpdate()
    {
        var entity = new TestEntity { Name = "Original" };
        _collection.Insert(entity);

        entity.Name = "Updated";
        var (updateType, count) = _collection.Upsert(entity);

        await Assert.That(updateType).IsEqualTo(UpdateType.Update);
        await Assert.That(count).IsEqualTo(1);

        var found = _collection.FindById(entity.Id);
        await Assert.That(found!.Name).IsEqualTo("Updated");
    }

    [Test]
    public async Task Upsert_EntityWithIdButNotInDb_ShouldInsert()
    {
        var entity = new TestEntity 
        { 
            Id = ObjectId.NewObjectId(), 
            Name = "NewWithId" 
        };

        var (updateType, count) = _collection.Upsert(entity);

        await Assert.That(updateType).IsEqualTo(UpdateType.Insert);
        await Assert.That(count).IsEqualTo(1);
    }

    #endregion

    #region Disposed Collection Tests

    [Test]
    public async Task Insert_OnDisposedCollection_ShouldThrowObjectDisposedException()
    {
        _collection.Dispose();

        await Assert.That(() => _collection.Insert(new TestEntity { Name = "Test" }))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Update_OnDisposedCollection_ShouldThrowObjectDisposedException()
    {
        var entity = new TestEntity { Id = ObjectId.NewObjectId(), Name = "Test" };
        _collection.Dispose();

        await Assert.That(() => _collection.Update(entity))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Delete_OnDisposedCollection_ShouldThrowObjectDisposedException()
    {
        _collection.Dispose();

        await Assert.That(() => _collection.Delete(ObjectId.NewObjectId()))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task FindById_OnDisposedCollection_ShouldThrowObjectDisposedException()
    {
        _collection.Dispose();

        await Assert.That(() => _collection.FindById(ObjectId.NewObjectId()))
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task FindAll_OnDisposedCollection_ShouldThrowObjectDisposedException()
    {
        _collection.Dispose();

        await Assert.That(() => _collection.FindAll().ToList())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Query_OnDisposedCollection_ShouldThrowObjectDisposedException()
    {
        _collection.Dispose();

        await Assert.That(() => _collection.Query())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task Count_OnDisposedCollection_ShouldThrowObjectDisposedException()
    {
        _collection.Dispose();

        await Assert.That(() => _collection.Count())
            .Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task GetIndexManager_OnDisposedCollection_ShouldThrowObjectDisposedException()
    {
        _collection.Dispose();

        await Assert.That(() => _collection.GetIndexManager())
            .Throws<ObjectDisposedException>();
    }

    #endregion

    #region ToString Tests

    [Test]
    public async Task ToString_ShouldReturnFormattedString()
    {
        var str = _collection.ToString();

        await Assert.That(str).Contains("DocumentCollection");
        await Assert.That(str).Contains("TestEntity");
    }

    #endregion

    #region IDocumentCollection Interface Tests

    [Test]
    public async Task IDocumentCollection_Name_ShouldReturnCollectionName()
    {
        var docCollection = (IDocumentCollection)_collection;

        await Assert.That(docCollection.Name).IsEqualTo("TestEntity");
    }

    [Test]
    public async Task IDocumentCollection_DocumentType_ShouldReturnEntityType()
    {
        var docCollection = (IDocumentCollection)_collection;

        await Assert.That(docCollection.DocumentType).IsEqualTo(typeof(TestEntity));
    }

    #endregion

    #region Transaction Integration Tests

    [Test]
    public async Task Insert_InTransaction_ShouldNotPersistUntilCommit()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_txn_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var collection = (DocumentCollection<TestEntity>)engine.GetCollection<TestEntity>();
            
            using var transaction = engine.BeginTransaction();

            collection.Insert(new TestEntity { Name = "InTransaction" });

            // Before commit, the document should be visible within transaction
            var countInTxn = collection.Count();
            await Assert.That(countInTxn).IsEqualTo(1);

            transaction.Rollback();

            // After rollback, the document should not exist
            await Assert.That(collection.Count()).IsEqualTo(0);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    [Test]
    public async Task Delete_InTransaction_ShouldRecordOperation()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_txn_del_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var collection = (DocumentCollection<TestEntity>)engine.GetCollection<TestEntity>();
            
            var entity = new TestEntity { Name = "ToDelete" };
            collection.Insert(entity);

            using var transaction = engine.BeginTransaction();

            var count = collection.Delete(entity.Id);
            await Assert.That(count).IsEqualTo(1);

            transaction.Commit();

            await Assert.That(collection.Count()).IsEqualTo(0);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    #endregion

    public class TestEntity
    {
        public ObjectId Id { get; set; } = ObjectId.NewObjectId();
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }
}
