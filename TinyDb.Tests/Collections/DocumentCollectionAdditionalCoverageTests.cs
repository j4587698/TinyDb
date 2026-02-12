using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Core;
using System.Reflection;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Collections;

public class DocumentCollectionAdditionalCoverageTests
{
    [Test]
    public async Task Constructor_NullArguments_ShouldThrow()
    {
        await Assert.That(() => new DocumentCollection<BsonDocument>(null!, "c"))
            .ThrowsExactly<ArgumentNullException>();

        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_ctor_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            await Assert.That(() => new DocumentCollection<BsonDocument>(engine, null!))
                .ThrowsExactly<ArgumentNullException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task Include_And_Count_ShouldWork()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_include_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var users = engine.GetCollection<User>("Users");
            var orders = (DocumentCollection<Order>)engine.GetCollection<Order>("Orders");

            users.Insert(new User { Id = 1, Name = "Alice" });
            orders.Insert(new Order { Id = 10, UserId = "1" });

            var includeByExpression = orders.Include(o => o.UserId).FindAll().ToList();
            await Assert.That(includeByExpression.Count).IsEqualTo(1);

            var includeByPath = orders.Include("UserId").FindAll().ToList();
            await Assert.That(includeByPath.Count).IsEqualTo(1);

            await Assert.That(orders.Count(o => o.UserId == "1")).IsEqualTo(1);
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task Upsert_And_AsyncGuards_ShouldCover_MissingBranches()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_upsert_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = (DocumentCollection<BsonDocument>)engine.GetBsonCollection("misc");

            await Assert.That(ReferenceEquals(col.Database, engine)).IsTrue();

            var upsert = col.Upsert(new BsonDocument().Set("x", 1));
            await Assert.That(upsert.UpdateType).IsEqualTo(UpdateType.Insert);
            await Assert.That(upsert.Count).IsEqualTo(1);

            await Assert.ThrowsAsync<ArgumentException>(() => col.UpdateAsync(new BsonDocument().Set("x", 1)));

            var upsertAsync = await col.UpsertAsync(new BsonDocument().Set("y", 2));
            await Assert.That(upsertAsync.UpdateType).IsEqualTo(UpdateType.Insert);
            await Assert.That(upsertAsync.Count).IsEqualTo(1);
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task Async_NullGuards_ShouldThrow()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_null_guards_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = (DocumentCollection<BatchItem>)engine.GetCollection<BatchItem>("batch");

            await Assert.ThrowsAsync<ArgumentNullException>(() => col.InsertAsync((BatchItem)null!));
            await Assert.ThrowsAsync<ArgumentNullException>(() => col.InsertAsync((IEnumerable<BatchItem>)null!));
            await Assert.ThrowsAsync<ArgumentNullException>(() => col.UpdateAsync((BatchItem)null!));
            await Assert.ThrowsAsync<ArgumentNullException>(() => col.UpdateAsync((IEnumerable<BatchItem>)null!));
            await Assert.ThrowsAsync<ArgumentNullException>(() => col.DeleteAsync((IEnumerable<BsonValue>)null!));
            await Assert.ThrowsAsync<ArgumentNullException>(() => col.DeleteManyAsync(null!));
            await Assert.ThrowsAsync<ArgumentNullException>(() => col.UpsertAsync((BatchItem)null!));
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task InsertAsync_LargeBatch_ShouldFlushInBatches()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_batch_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = (DocumentCollection<BatchItem>)engine.GetCollection<BatchItem>("batch");

            var items = Enumerable.Range(1, 1001).Select(i => new BatchItem { Id = i, Name = $"N{i}" }).ToList();
            var inserted = await col.InsertAsync(items);

            await Assert.That(inserted).IsEqualTo(1001);
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task EnsureIdIndex_WhenEngineDisposed_ShouldSwallowIndexErrorsAndStillThrowLater()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_disposed_{Guid.NewGuid():N}.db");

        try
        {
            var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = (DocumentCollection<BsonDocument>)engine.GetBsonCollection("misc");

            engine.Dispose();

            await Assert.That(() => col.Insert(new BsonDocument().Set("x", 1)))
                .Throws<ObjectDisposedException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task PrivateIdHelpers_WhenAotIdAccessorThrows_ShouldReturnBsonNull_And_SwallowSetId()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_id_helpers_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = (DocumentCollection<PlainEntity>)engine.GetCollection<PlainEntity>("plain");

            var getEntityId = typeof(DocumentCollection<PlainEntity>).GetMethod("GetEntityId", BindingFlags.Instance | BindingFlags.NonPublic);
            await Assert.That(getEntityId).IsNotNull();

            var updateEntityId = typeof(DocumentCollection<PlainEntity>).GetMethod("UpdateEntityId", BindingFlags.Instance | BindingFlags.NonPublic);
            await Assert.That(updateEntityId).IsNotNull();

            var entity = new PlainEntity { Name = "x" };
            var id = (BsonValue)getEntityId!.Invoke(col, new object[] { entity })!;
            await Assert.That(id).IsEqualTo(BsonNull.Value);

            await Assert.That(() => updateEntityId!.Invoke(col, new object[] { entity, new BsonObjectId(ObjectId.NewObjectId()) })).ThrowsNothing();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task InsertBatch_WithEmptyDocuments_ShouldReturnZero()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_empty_batch_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = (DocumentCollection<PlainEntity>)engine.GetCollection<PlainEntity>("plain");

            var insertBatch = typeof(DocumentCollection<PlainEntity>).GetMethod("InsertBatch", BindingFlags.Instance | BindingFlags.NonPublic);
            await Assert.That(insertBatch).IsNotNull();

            var inserted = (int)insertBatch!.Invoke(col, new object[] { new List<PlainEntity>(), new List<BsonDocument>() })!;
            await Assert.That(inserted).IsEqualTo(0);
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task EnsureIdIndex_Private_WhenEngineDisposed_ShouldSwallowEnsureIndexErrors()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_ensure_id_{Guid.NewGuid():N}.db");

        try
        {
            var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var col = (DocumentCollection<BatchItem>)engine.GetCollection<BatchItem>("batch");

            var ensureIdIndex = typeof(DocumentCollection<BatchItem>).GetMethod("EnsureIdIndex", BindingFlags.Instance | BindingFlags.NonPublic);
            await Assert.That(ensureIdIndex).IsNotNull();

            engine.Dispose();

            await Assert.That(() => ensureIdIndex!.Invoke(col, Array.Empty<object>())).ThrowsNothing();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    private static void CleanupDb(string dbPath)
    {
        if (File.Exists(dbPath)) File.Delete(dbPath);
        var walPath = dbPath + "-wal";
        if (File.Exists(walPath)) File.Delete(walPath);
        var shmPath = dbPath + "-shm";
        if (File.Exists(shmPath)) File.Delete(shmPath);
    }

    [Entity("Users")]
    public sealed class User
    {
        [Id]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    [Entity("Orders")]
    public sealed class Order
    {
        [Id]
        public int Id { get; set; }

        [ForeignKey("Users")]
        public string UserId { get; set; } = string.Empty;
    }

    [Entity("batch")]
    public sealed class BatchItem
    {
        [Id]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    public sealed class PlainEntity
    {
        public string Name { get; set; } = string.Empty;
    }
}
