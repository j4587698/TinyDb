using System.IO;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Metadata;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using PropertyMetadata = TinyDb.Metadata.PropertyMetadata;

namespace TinyDb.Tests.Core;

/// <summary>
/// Additional TransactionManager edge case tests for improved coverage
/// Focuses on: max transactions, commit/rollback exceptions, FK validation
/// </summary>
public class TransactionManagerEdgeCaseTests2
{
    private string _databasePath = null!;
    private TinyDbEngine _engine = null!;

    [Before(Test)]
    public void Setup()
    {
        _databasePath = Path.Combine(Path.GetTempPath(), $"txn_edge2_{Guid.NewGuid():N}.db");
        var options = new TinyDbOptions
        {
            DatabaseName = "TransactionEdge2TestDb",
            PageSize = 4096,
            CacheSize = 100,
            MaxTransactions = 3, // Low limit for testing
            TransactionTimeout = TimeSpan.FromSeconds(30)
        };

        _engine = new TinyDbEngine(_databasePath, options);
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

    #region Max Transactions Limit Tests

    [Test]
    public async Task BeginTransaction_WhenMaxReached_ShouldThrowInvalidOperationException()
    {
        var manager = new TransactionManager(_engine, 2, TimeSpan.FromMinutes(1));
        try
        {
            // Begin 2 transactions (max)
            var txn1 = manager.BeginTransaction();
            var txn2 = manager.BeginTransaction();

            await Assert.That(manager.ActiveTransactionCount).IsEqualTo(2);

            // Third should throw
            await Assert.That(() => manager.BeginTransaction())
                .Throws<InvalidOperationException>();
            
            txn1.Dispose();
            txn2.Dispose();
        }
        finally
        {
            manager.Dispose();
        }
    }

    [Test]
    public async Task BeginTransaction_AfterCommit_ShouldAllowNewTransaction()
    {
        var manager = new TransactionManager(_engine, 1, TimeSpan.FromMinutes(1));
        try
        {
            var txn1 = manager.BeginTransaction();
            txn1.Commit();

            await Assert.That(manager.ActiveTransactionCount).IsEqualTo(0);

            // Should allow new transaction after commit
            var txn2 = manager.BeginTransaction();
            await Assert.That(manager.ActiveTransactionCount).IsEqualTo(1);
            txn2.Dispose();
        }
        finally
        {
            manager.Dispose();
        }
    }

    [Test]
    public async Task BeginTransaction_AfterRollback_ShouldAllowNewTransaction()
    {
        var manager = new TransactionManager(_engine, 1, TimeSpan.FromMinutes(1));
        try
        {
            var txn1 = manager.BeginTransaction();
            txn1.Rollback();

            await Assert.That(manager.ActiveTransactionCount).IsEqualTo(0);

            // Should allow new transaction after rollback
            var txn2 = manager.BeginTransaction();
            await Assert.That(manager.ActiveTransactionCount).IsEqualTo(1);
            txn2.Dispose();
        }
        finally
        {
            manager.Dispose();
        }
    }

    #endregion

    #region Commit Transaction Not Active Tests

    [Test]
    public async Task Commit_AfterAlreadyCommitted_ShouldThrow()
    {
        using var txn = _engine.BeginTransaction();
        txn.Commit();
        
        await Assert.That(() => txn.Commit())
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Commit_AfterRollback_ShouldThrow()
    {
        using var txn = _engine.BeginTransaction();
        txn.Rollback();
        
        await Assert.That(() => txn.Commit())
            .Throws<InvalidOperationException>();
    }

    #endregion

    #region Rollback Transaction Not Active Tests

    [Test]
    public async Task Rollback_AfterAlreadyRolledBack_ShouldThrow()
    {
        using var txn = _engine.BeginTransaction();
        txn.Rollback();
        
        await Assert.That(() => txn.Rollback())
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Rollback_AfterCommit_ShouldThrow()
    {
        using var txn = _engine.BeginTransaction();
        txn.Commit();
        
        await Assert.That(() => txn.Rollback())
            .Throws<InvalidOperationException>();
    }

    #endregion

    #region Duplicate Document ID Tests

    [Test]
    public async Task Commit_WithDuplicateInserts_ShouldThrow()
    {
        var collection = _engine.GetCollection<TestEntity>();
        using var txn = _engine.BeginTransaction();

        var id = ObjectId.NewObjectId();
        var entity1 = new TestEntity { Id = id, Name = "Entity1" };
        var entity2 = new TestEntity { Id = id, Name = "Entity2" };

        collection.Insert(entity1);
        collection.Insert(entity2);

        await Assert.That(() => txn.Commit())
            .Throws<InvalidOperationException>()
            .WithMessage("Failed to commit transaction: Duplicate document IDs detected in transaction");
    }

    #endregion

    #region Foreign Key Validation Tests

    [Test]
    public async Task Commit_WithForeignKeyMetadata_ShouldSucceedWithValidParent()
    {
        // Create parent collection first
        var parentCollection = _engine.GetCollection<ParentEntity>();
        var parentId = ObjectId.NewObjectId();
        var parent = new ParentEntity { Id = parentId, Name = "Parent" };
        parentCollection.Insert(parent);

        // Create child collection with FK metadata
        var childCollection = _engine.GetCollection<ChildEntity>();
        
        // Register FK metadata manually (simulating attribute-based FK)
        var metadata = new EntityMetadata
        {
            CollectionName = "__collection_ChildEntity",
            TypeName = typeof(ChildEntity).FullName!,
            Properties = new List<PropertyMetadata>
            {
                new() { PropertyName = "ParentId", ForeignKeyCollection = "__collection_ParentEntity" }
            }
        };
        
        // Store metadata
        var metadataCollection = _engine.GetCollection<MetadataDocument>("__metadata_ChildEntity");
        metadataCollection.Insert(MetadataDocument.FromEntityMetadata(metadata));

        // Begin transaction and insert child with valid parent reference
        using var txn = _engine.BeginTransaction();
        
        var child = new ChildEntity 
        { 
            Id = ObjectId.NewObjectId(), 
            Name = "Child",
            ParentId = parentId // Valid parent
        };
        childCollection.Insert(child);

        // Should succeed with valid FK
        txn.Commit();
        await Assert.That(txn.State).IsEqualTo(TransactionState.Committed);
    }

    [Test]
    public async Task Commit_WithValidForeignKey_ShouldSucceed()
    {
        // Clean database for this test
        var dbPath = Path.Combine(Path.GetTempPath(), $"txn_fk_valid_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            
            // Create parent
            var parentCollection = engine.GetCollection<ParentEntity>();
            var parentId = ObjectId.NewObjectId();
            var parent = new ParentEntity { Id = parentId, Name = "Parent" };
            parentCollection.Insert(parent);

            // Create child collection with FK metadata
            var metadata = new EntityMetadata
            {
                CollectionName = "__collection_ChildEntity",
                TypeName = typeof(ChildEntity).FullName!,
                Properties = new List<PropertyMetadata>
                {
                    new() { PropertyName = "ParentId", ForeignKeyCollection = "__collection_ParentEntity" }
                }
            };
            
            var metadataCollection = engine.GetCollection<MetadataDocument>("__metadata_ChildEntity");
            metadataCollection.Insert(MetadataDocument.FromEntityMetadata(metadata));

            // Begin transaction and insert child with valid parent reference
            using var txn = engine.BeginTransaction();
            
            var childCollection = engine.GetCollection<ChildEntity>();
            var child = new ChildEntity 
            { 
                Id = ObjectId.NewObjectId(), 
                Name = "Child",
                ParentId = parentId // Valid parent
            };
            childCollection.Insert(child);

            // Should not throw
            txn.Commit();
            await Assert.That(txn.State).IsEqualTo(TransactionState.Committed);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    [Test]
    public async Task Commit_WithNullForeignKey_ShouldSucceed()
    {
        // Clean database for this test
        var dbPath = Path.Combine(Path.GetTempPath(), $"txn_fk_null_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);

            // Create child collection with FK metadata
            var metadata = new EntityMetadata
            {
                CollectionName = "__collection_NullableChildEntity",
                TypeName = typeof(NullableChildEntity).FullName!,
                Properties = new List<PropertyMetadata>
                {
                    new() { PropertyName = "ParentId", ForeignKeyCollection = "__collection_ParentEntity" }
                }
            };
            
            var metadataCollection = engine.GetCollection<MetadataDocument>("__metadata_NullableChildEntity");
            metadataCollection.Insert(MetadataDocument.FromEntityMetadata(metadata));

            using var txn = engine.BeginTransaction();
            
            var childCollection = engine.GetCollection<NullableChildEntity>();
            var child = new NullableChildEntity 
            { 
                Id = ObjectId.NewObjectId(), 
                Name = "Child",
                ParentId = null // Null FK should be allowed
            };
            childCollection.Insert(child);

            // Should not throw - null FK is allowed
            txn.Commit();
            await Assert.That(txn.State).IsEqualTo(TransactionState.Committed);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    #endregion

    #region ToCamelCase Edge Cases

    [Test]
    public async Task ToCamelCase_WithCamelCaseFieldInDocument_ShouldValidateCorrectly()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"txn_camel_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            
            // Create parent
            var parentCollection = engine.GetCollection<ParentEntity>();
            var parentId = ObjectId.NewObjectId();
            parentCollection.Insert(new ParentEntity { Id = parentId, Name = "Parent" });

            // Create FK metadata with PascalCase property name
            var metadata = new EntityMetadata
            {
                CollectionName = "__collection_CamelCaseChild",
                TypeName = "CamelCaseChild",
                Properties = new List<PropertyMetadata>
                {
                    new() { PropertyName = "ParentId", ForeignKeyCollection = "__collection_ParentEntity" }
                }
            };
            
            var metadataCollection = engine.GetCollection<MetadataDocument>("__metadata_CamelCaseChild");
            metadataCollection.Insert(MetadataDocument.FromEntityMetadata(metadata));

            // Insert document with camelCase field name directly
            using var txn = engine.BeginTransaction();
            
            var doc = new BsonDocument()
                .Set("_id", ObjectId.NewObjectId())
                .Set("parentId", parentId); // camelCase

            engine.InsertDocument("__collection_CamelCaseChild", doc);

            txn.Commit();
            await Assert.That(txn.State).IsEqualTo(TransactionState.Committed);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    #endregion

    #region Savepoint with Not Active Transaction Tests

    [Test]
    public async Task CreateSavepoint_AfterCommit_ShouldThrow()
    {
        var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(1));
        try
        {
            var txn = manager.BeginTransaction();
            txn.Commit();

            await Assert.That(() => txn.CreateSavepoint("test"))
                .Throws<InvalidOperationException>();
        }
        finally
        {
            manager.Dispose();
        }
    }

    [Test]
    public async Task RollbackToSavepoint_AfterCommit_ShouldThrow()
    {
        var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(1));
        try
        {
            var txn = manager.BeginTransaction();
            var spId = txn.CreateSavepoint("test");
            txn.Commit();

            await Assert.That(() => txn.RollbackToSavepoint(spId))
                .Throws<InvalidOperationException>();
        }
        finally
        {
            manager.Dispose();
        }
    }

    [Test]
    public async Task ReleaseSavepoint_AfterCommit_ShouldThrow()
    {
        var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(1));
        try
        {
            var txn = manager.BeginTransaction();
            var spId = txn.CreateSavepoint("test");
            txn.Commit();

            await Assert.That(() => txn.ReleaseSavepoint(spId))
                .Throws<InvalidOperationException>();
        }
        finally
        {
            manager.Dispose();
        }
    }

    #endregion

    #region RecordOperation with Not Active Transaction Tests

    [Test]
    public async Task Collection_AfterCommit_ShouldWorkOutsideTransaction()
    {
        var collection = _engine.GetCollection<TestEntity>();
        using var txn = _engine.BeginTransaction();
        txn.Commit();

        // After commit, collection operations should work (outside transaction context)
        var entity = new TestEntity { Id = ObjectId.NewObjectId(), Name = "Test" };
        collection.Insert(entity);
        
        var found = collection.FindById(entity.Id);
        await Assert.That(found).IsNotNull();
        await Assert.That(found!.Name).IsEqualTo("Test");
    }

    #endregion

    #region RollbackSingleOperation Default Case

    [Test]
    public async Task RollbackSingleOperation_WithAllSupportedTypes_ShouldSucceed()
    {
        // Test rollback with all supported operation types
        var collection = _engine.GetCollection<TestEntity>();
        using var txn = _engine.BeginTransaction();

        var entity = new TestEntity { Id = ObjectId.NewObjectId(), Name = "Test" };
        collection.Insert(entity);
        
        // Rollback should work for all supported operations
        txn.Rollback();
        await Assert.That(txn.State).IsEqualTo(TransactionState.RolledBack);
    }

    #endregion

    #region ApplySingleOperation Tests

    [Test]
    public async Task Commit_WithAllOperationTypes_ShouldSucceed()
    {
        var collection = _engine.GetCollection<TestEntity>();
        using var txn = _engine.BeginTransaction();

        // Insert
        var entity = new TestEntity { Id = ObjectId.NewObjectId(), Name = "Test" };
        collection.Insert(entity);

        // Update
        entity.Name = "Updated";
        collection.Update(entity);

        txn.Commit();
        await Assert.That(txn.State).IsEqualTo(TransactionState.Committed);

        // Verify
        var found = collection.FindById(entity.Id);
        await Assert.That(found).IsNotNull();
        await Assert.That(found!.Name).IsEqualTo("Updated");
    }

    [Test]
    public async Task Commit_WithDeleteOperation_ShouldSucceed()
    {
        var collection = _engine.GetCollection<TestEntity>();
        
        // Insert outside transaction
        var entity = new TestEntity { Id = ObjectId.NewObjectId(), Name = "Test" };
        collection.Insert(entity);

        using var txn = _engine.BeginTransaction();
        collection.Delete(entity.Id);
        txn.Commit();
        
        await Assert.That(txn.State).IsEqualTo(TransactionState.Committed);
        await Assert.That(collection.FindById(entity.Id)).IsNull();
    }

    #endregion

    #region Transaction State Management

    [Test]
    public async Task Transaction_States_ShouldTransitionCorrectly()
    {
        var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(1));
        try
        {
            var txn = manager.BeginTransaction();
            await Assert.That(txn.State).IsEqualTo(TransactionState.Active);

            txn.Commit();
            await Assert.That(txn.State).IsEqualTo(TransactionState.Committed);
        }
        finally
        {
            manager.Dispose();
        }
    }

    [Test]
    public async Task Transaction_StateAfterRollback_ShouldBeRolledBack()
    {
        var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(1));
        try
        {
            var txn = manager.BeginTransaction();
            await Assert.That(txn.State).IsEqualTo(TransactionState.Active);

            txn.Rollback();
            await Assert.That(txn.State).IsEqualTo(TransactionState.RolledBack);
        }
        finally
        {
            manager.Dispose();
        }
    }

    #endregion

    #region Dispose While Transaction Active

    [Test]
    public async Task ManagerDispose_WithActiveTransactions_ShouldMarkAsFailed()
    {
        var manager = new TransactionManager(_engine, 10, TimeSpan.FromMinutes(1));
        
        var txn = manager.BeginTransaction();
        await Assert.That(txn.State).IsEqualTo(TransactionState.Active);

        manager.Dispose();
        await Assert.That(txn.State).IsEqualTo(TransactionState.Failed);
    }

    #endregion

    #region ToCamelCase with Single Character Name

    [Test]
    public async Task ToCamelCase_WithSingleCharFieldName_ShouldWork()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"txn_single_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            
            // Create parent
            var parentCollection = engine.GetCollection<ParentEntity>();
            var parentId = ObjectId.NewObjectId();
            parentCollection.Insert(new ParentEntity { Id = parentId, Name = "Parent" });

            // Create FK metadata with single char property name
            var metadata = new EntityMetadata
            {
                CollectionName = "__collection_SingleCharChild",
                TypeName = "SingleCharChild",
                Properties = new List<PropertyMetadata>
                {
                    new() { PropertyName = "P", ForeignKeyCollection = "__collection_ParentEntity" }
                }
            };
            
            var metadataCollection = engine.GetCollection<MetadataDocument>("__metadata_SingleCharChild");
            metadataCollection.Insert(MetadataDocument.FromEntityMetadata(metadata));

            using var txn = engine.BeginTransaction();
            
            // Insert with lowercase single char - tests ToCamelCase with length == 1
            var doc = new BsonDocument()
                .Set("_id", ObjectId.NewObjectId())
                .Set("p", parentId); // lowercase single char

            engine.InsertDocument("__collection_SingleCharChild", doc);

            txn.Commit();
            await Assert.That(txn.State).IsEqualTo(TransactionState.Committed);
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    #endregion

    #region Test Entities

    [Entity]
    public class TestEntity
    {
        public ObjectId Id { get; set; } = ObjectId.NewObjectId();
        public string Name { get; set; } = string.Empty;
    }

    [Entity]
    public class ParentEntity
    {
        public ObjectId Id { get; set; } = ObjectId.NewObjectId();
        public string Name { get; set; } = string.Empty;
    }

    [Entity]
    public class ChildEntity
    {
        public ObjectId Id { get; set; } = ObjectId.NewObjectId();
        public string Name { get; set; } = string.Empty;
        public ObjectId ParentId { get; set; }
    }

    [Entity]
    public class NullableChildEntity
    {
        public ObjectId Id { get; set; } = ObjectId.NewObjectId();
        public string Name { get; set; } = string.Empty;
        public ObjectId? ParentId { get; set; }
    }

    #endregion
}
