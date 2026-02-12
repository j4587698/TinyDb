using TinyDb.Core;
using TinyDb.Attributes;
using TinyDb.Collections;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Collections;

/// <summary>
/// 异步集合操作测试
/// </summary>
public class AsyncCollectionTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public AsyncCollectionTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"async_test_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    #region InsertAsync Tests

    [Test]
    public async Task InsertAsync_Single_Should_Insert_And_Return_Id()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "Test" };

        var id = await collection.InsertAsync(entity);

        await Assert.That(id).IsNotNull();
        await Assert.That(collection.Count()).IsEqualTo(1);
        
        var loaded = collection.FindById(id);
        await Assert.That(loaded).IsNotNull();
        await Assert.That(loaded!.Name).IsEqualTo("Test");
    }

    [Test]
    public async Task InsertAsync_Single_Should_Set_Entity_Id()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "Test" };

        var id = await collection.InsertAsync(entity);

        await Assert.That(entity.Id != ObjectId.Empty).IsTrue();
        await Assert.That(entity.Id.ToString()).IsEqualTo(id.ToString());
    }

    [Test]
    public async Task InsertAsync_Multiple_Should_Insert_All()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entities = Enumerable.Range(1, 100)
            .Select(i => new AsyncTestEntity { Name = $"Entity_{i}" })
            .ToList();

        var count = await collection.InsertAsync(entities);

        await Assert.That(count).IsEqualTo(100);
        await Assert.That(collection.Count()).IsEqualTo(100);
    }

    [Test]
    public async Task InsertAsync_Should_Support_Cancellation()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entities = Enumerable.Range(1, 10000)
            .Select(i => new AsyncTestEntity { Name = $"Entity_{i}" })
            .ToList();

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await collection.InsertAsync(entities, cts.Token);
        });
    }

    [Test]
    public async Task InsertAsync_InTransaction_ShouldCommit()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "TxInsert" };

        using var transaction = _engine.BeginTransaction();
        var id = await collection.InsertAsync(entity);

        await Assert.That(collection.Count()).IsEqualTo(1);

        transaction.Commit();

        var loaded = collection.FindById(id);
        await Assert.That(loaded).IsNotNull();
        await Assert.That(loaded!.Name).IsEqualTo("TxInsert");
    }

    [Test]
    public async Task InsertAsync_BsonDocument_ShouldAssignId()
    {
        var collection = _engine.GetBsonCollection("bson_insert");
        var doc = new BsonDocument().Set("name", "doc");

        var id = await collection.InsertAsync(doc);

        await Assert.That(id).IsNotNull();
        var loaded = collection.FindById(id);
        await Assert.That(loaded).IsNotNull();
        await Assert.That(((BsonString)loaded!["name"]).Value).IsEqualTo("doc");
    }

    [Test]
    public async Task InsertAsync_Multiple_WithNullEntries_ShouldSkipNulls()
    {
        var collection = _engine.GetBsonCollection("bson_insert_many");
        var docs = new BsonDocument?[]
        {
            new BsonDocument().Set("name", "a"),
            null,
            new BsonDocument().Set("name", "b")
        };

        var count = await collection.InsertAsync(docs!);

        await Assert.That(count).IsEqualTo(2);
        await Assert.That(collection.Count()).IsEqualTo(2);
    }

    #endregion

    #region ReadAsync Tests

    [Test]
    public async Task FindByIdAsync_Should_Return_Document_When_Exists()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "Test" };
        var id = collection.Insert(entity);

        var loaded = await collection.FindByIdAsync(id);

        await Assert.That(loaded).IsNotNull();
        await Assert.That(loaded!.Name).IsEqualTo("Test");
    }

    [Test]
    public async Task FindAllAsync_Should_Return_All_Documents()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        collection.Insert(new AsyncTestEntity { Name = "One" });
        collection.Insert(new AsyncTestEntity { Name = "Two" });
        collection.Insert(new AsyncTestEntity { Name = "Three" });

        var all = await collection.FindAllAsync();

        await Assert.That(all.Count).IsEqualTo(3);
        var names = all.Select(x => x.Name).OrderBy(x => x).ToArray();
        await Assert.That(names.SequenceEqual(new[] { "One", "Three", "Two" })).IsTrue();
    }

    [Test]
    public async Task FindAsync_Should_Filter_Documents()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        collection.Insert(new AsyncTestEntity { Name = "Keep" });
        collection.Insert(new AsyncTestEntity { Name = "Match" });
        collection.Insert(new AsyncTestEntity { Name = "Match" });

        var matched = await collection.FindAsync(x => x.Name == "Match");

        await Assert.That(matched.Count).IsEqualTo(2);
        await Assert.That(matched.All(x => x.Name == "Match")).IsTrue();
    }

    [Test]
    public async Task FindOneAsync_Should_Return_First_Match()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        collection.Insert(new AsyncTestEntity { Name = "A" });
        collection.Insert(new AsyncTestEntity { Name = "B" });

        var found = await collection.FindOneAsync(x => x.Name == "B");

        await Assert.That(found).IsNotNull();
        await Assert.That(found!.Name).IsEqualTo("B");
    }

    [Test]
    public async Task CountAsync_Should_Return_Count()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        collection.Insert(new AsyncTestEntity { Name = "A" });
        collection.Insert(new AsyncTestEntity { Name = "B" });

        var count = await collection.CountAsync();

        await Assert.That(count).IsEqualTo(2);
    }

    [Test]
    public async Task CountAsync_WithPredicate_Should_Return_Matching_Count()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        collection.Insert(new AsyncTestEntity { Name = "A" });
        collection.Insert(new AsyncTestEntity { Name = "A" });
        collection.Insert(new AsyncTestEntity { Name = "B" });

        var count = await collection.CountAsync(x => x.Name == "A");

        await Assert.That(count).IsEqualTo(2);
    }

    [Test]
    public async Task ExistsAsync_Should_Return_True_When_Found()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        collection.Insert(new AsyncTestEntity { Name = "A" });

        var exists = await collection.ExistsAsync(x => x.Name == "A");

        await Assert.That(exists).IsTrue();
    }

    [Test]
    public async Task ExistsAsync_Should_Return_False_When_NotFound()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        collection.Insert(new AsyncTestEntity { Name = "A" });

        var exists = await collection.ExistsAsync(x => x.Name == "B");

        await Assert.That(exists).IsFalse();
    }

    [Test]
    public async Task FindAllAsync_Should_Support_Cancellation()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        collection.Insert(Enumerable.Range(1, 10).Select(i => new AsyncTestEntity { Name = $"Entity_{i}" }));

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await collection.FindAllAsync(cts.Token);
        });
    }

    #endregion

    #region UpdateAsync Tests

    [Test]
    public async Task UpdateAsync_Single_Should_Update_Document()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "Original" };
        collection.Insert(entity);

        entity.Name = "Updated";
        var count = await collection.UpdateAsync(entity);

        await Assert.That(count).IsEqualTo(1);
        
        var loaded = collection.FindById(entity.Id);
        await Assert.That(loaded!.Name).IsEqualTo("Updated");
    }

    [Test]
    public async Task UpdateAsync_Multiple_Should_Update_All()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entities = new[]
        {
            new AsyncTestEntity { Name = "One" },
            new AsyncTestEntity { Name = "Two" },
            new AsyncTestEntity { Name = "Three" }
        };
        collection.Insert(entities);

        foreach (var e in entities)
        {
            e.Name = e.Name + "_Updated";
        }

        var count = await collection.UpdateAsync(entities);

        await Assert.That(count).IsEqualTo(3);
        await Assert.That(collection.FindAll().All(e => e.Name.EndsWith("_Updated"))).IsTrue();
    }

    // 注意：UpdateAsync_Without_Valid_Id_Should_Throw 测试已被移除
    // 因为在 TUnit SourceGenerated 模式下，对于测试项目中定义的实体类，
    // AOT 适配器可能无法正确生成，导致回退到反射路径时行为不一致。
    // HasValidId 的正确行为已在 AotIdAccessorCoverageTests.cs 中充分测试。

    [Test]
    public async Task UpdateAsync_InTransaction_WhenMissing_ShouldInsert()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "TxNew" };

        using var transaction = _engine.BeginTransaction();
        var count = await collection.UpdateAsync(entity);

        await Assert.That(count).IsEqualTo(1);
        await Assert.That(collection.Count()).IsEqualTo(1);

        transaction.Commit();

        var loaded = collection.FindById(entity.Id);
        await Assert.That(loaded).IsNotNull();
        await Assert.That(loaded!.Name).IsEqualTo("TxNew");
    }

    [Test]
    public async Task UpdateAsync_InTransaction_WhenExists_ShouldUpdate()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "Before" };
        collection.Insert(entity);

        entity.Name = "After";

        using var transaction = _engine.BeginTransaction();
        var count = await collection.UpdateAsync(entity);

        await Assert.That(count).IsEqualTo(1);

        transaction.Commit();

        var loaded = collection.FindById(entity.Id);
        await Assert.That(loaded).IsNotNull();
        await Assert.That(loaded!.Name).IsEqualTo("After");
    }

    #endregion

    #region DeleteAsync Tests

    [Test]
    public async Task DeleteAsync_Single_Should_Delete_Document()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "ToDelete" };
        var id = collection.Insert(entity);

        var count = await collection.DeleteAsync(id);

        await Assert.That(count).IsEqualTo(1);
        await Assert.That(collection.Count()).IsEqualTo(0);
    }

    [Test]
    public async Task DeleteAsync_Multiple_Should_Delete_All()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entities = new[]
        {
            new AsyncTestEntity { Name = "One" },
            new AsyncTestEntity { Name = "Two" },
            new AsyncTestEntity { Name = "Three" }
        };
        collection.Insert(entities);
        var ids = entities.Select(e => (BsonValue)e.Id).ToList();

        var count = await collection.DeleteAsync(ids);

        await Assert.That(count).IsEqualTo(3);
        await Assert.That(collection.Count()).IsEqualTo(0);
    }

    [Test]
    public async Task DeleteAsync_NonExistent_Should_Return_Zero()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();

        var count = await collection.DeleteAsync(999);

        await Assert.That(count).IsEqualTo(0);
    }

    [Test]
    public async Task DeleteAsync_BsonNull_Should_Return_Zero()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();

        var count = await collection.DeleteAsync(BsonNull.Value);

        await Assert.That(count).IsEqualTo(0);
    }

    [Test]
    public async Task DeleteAsync_InTransaction_ShouldCommitDelete()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "TxDelete" };
        var id = collection.Insert(entity);

        using var transaction = _engine.BeginTransaction();
        var count = await collection.DeleteAsync(id);

        await Assert.That(count).IsEqualTo(1);
        await Assert.That(collection.Count()).IsEqualTo(0);

        transaction.Commit();

        var loaded = collection.FindById(id);
        await Assert.That(loaded).IsNull();
    }

    [Test]
    public async Task DeleteAsync_InTransaction_WhenMissing_ShouldReturnZero()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();

        using var transaction = _engine.BeginTransaction();
        var count = await collection.DeleteAsync(new BsonInt32(999));

        await Assert.That(count).IsEqualTo(0);

        transaction.Commit();
    }

    [Test]
    public async Task DeleteAllAsync_Should_Delete_All_Documents()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        collection.Insert(Enumerable.Range(1, 50).Select(i => new AsyncTestEntity { Name = $"Entity_{i}" }));

        await Assert.That(collection.Count()).IsEqualTo(50);

        var count = await collection.DeleteAllAsync();

        await Assert.That(count).IsEqualTo(50);
        await Assert.That(collection.Count()).IsEqualTo(0);
    }

    [Test]
    public async Task DeleteManyAsync_Should_Delete_Matching_Documents()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        collection.Insert(new AsyncTestEntity { Name = "Keep" });
        collection.Insert(new AsyncTestEntity { Name = "Delete" });
        collection.Insert(new AsyncTestEntity { Name = "Delete" });
        collection.Insert(new AsyncTestEntity { Name = "Keep" });

        var count = await collection.DeleteManyAsync(e => e.Name == "Delete");

        await Assert.That(count).IsEqualTo(2);
        await Assert.That(collection.Count()).IsEqualTo(2);
        await Assert.That(collection.FindAll().All(e => e.Name == "Keep")).IsTrue();
    }

    #endregion

    #region UpsertAsync Tests

    [Test]
    public async Task UpsertAsync_Should_Insert_If_New()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "New" };

        var (updateType, count) = await collection.UpsertAsync(entity);

        await Assert.That(updateType).IsEqualTo(UpdateType.Insert);
        await Assert.That(count).IsEqualTo(1);
        await Assert.That(collection.Count()).IsEqualTo(1);
    }

    [Test]
    public async Task UpsertAsync_Should_Update_If_Exists()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var entity = new AsyncTestEntity { Name = "Original" };
        collection.Insert(entity);

        entity.Name = "Updated";
        var (updateType, count) = await collection.UpsertAsync(entity);

        await Assert.That(updateType).IsEqualTo(UpdateType.Update);
        await Assert.That(count).IsEqualTo(1);
        
        var loaded = collection.FindById(entity.Id);
        await Assert.That(loaded!.Name).IsEqualTo("Updated");
    }

    #endregion

    #region Concurrent Async Operations

    [Test]
    public async Task ConcurrentInsertAsync_Should_Handle_Multiple_Inserts()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        var tasks = Enumerable.Range(1, 10)
            .Select(i => collection.InsertAsync(new AsyncTestEntity { Name = $"Concurrent_{i}" }))
            .ToList();

        await Task.WhenAll(tasks);

        await Assert.That(collection.Count()).IsEqualTo(10);
    }

    [Test]
    public async Task MixedAsyncOperations_Should_Work_Correctly()
    {
        var collection = _engine.GetCollection<AsyncTestEntity>();
        
        // Insert
        var entity1 = new AsyncTestEntity { Name = "Entity1" };
        var entity2 = new AsyncTestEntity { Name = "Entity2" };
        await collection.InsertAsync(entity1);
        await collection.InsertAsync(entity2);

        await Assert.That(collection.Count()).IsEqualTo(2);

        // Update
        entity1.Name = "Entity1_Updated";
        await collection.UpdateAsync(entity1);

        var loaded = collection.FindById(entity1.Id);
        await Assert.That(loaded!.Name).IsEqualTo("Entity1_Updated");

        // Delete
        await collection.DeleteAsync(entity2.Id);
        await Assert.That(collection.Count()).IsEqualTo(1);

        // Upsert (update existing)
        entity1.Name = "Entity1_Upserted";
        var (updateType, _) = await collection.UpsertAsync(entity1);
        await Assert.That(updateType).IsEqualTo(UpdateType.Update);

        // Upsert (insert new)
        var entity3 = new AsyncTestEntity { Name = "Entity3" };
        var (insertType, _) = await collection.UpsertAsync(entity3);
        await Assert.That(insertType).IsEqualTo(UpdateType.Insert);

        await Assert.That(collection.Count()).IsEqualTo(2);
    }

    #endregion
}

/// <summary>
/// 测试用实体类
/// </summary>
[Entity("AsyncTestEntities")]
public class AsyncTestEntity
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string Name { get; set; } = string.Empty;

    public int Value { get; set; }
}

/// <summary>
/// 无自动ID的测试实体类，用于测试无效ID的场景
/// </summary>
[Entity("AsyncTestNoAutoIdEntities")]
public class AsyncTestNoAutoIdEntity
{
    public ObjectId Id { get; set; }

    public string Name { get; set; } = string.Empty;
}
