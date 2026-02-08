using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.References;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.References;

public class DbRefSerializerTests
{
    [Test]
    public async Task GetRefProperties_ShouldReturnForeignKeyProperties()
    {
        var refs = DbRefSerializer.GetRefProperties(typeof(RefOrder));

        await Assert.That(refs.Count).IsEqualTo(1);
        await Assert.That(refs[0].Property.Name).IsEqualTo(nameof(RefOrder.UserId));
        await Assert.That(refs[0].Attribute.CollectionName).IsEqualTo("Users");
    }

    [Test]
    public async Task GetEntityId_ShouldReturnBsonId()
    {
        var doc = new BsonDocument().Set("_id", 42);
        var id = DbRefSerializer.GetEntityId(doc);

        await Assert.That(id).IsNotNull();
        await Assert.That(((BsonInt32)id).Value).IsEqualTo(42);
    }

    [Test]
    public async Task GetEntityId_Null_ShouldReturnBsonNull()
    {
        var id = DbRefSerializer.GetEntityId(null!);

        await Assert.That(id).IsEqualTo(BsonNull.Value);
    }

    [Test]
    public async Task SerializeToDbRef_Null_ShouldReturnBsonNull()
    {
#pragma warning disable CS0618
        var dbRef = DbRefSerializer.SerializeToDbRef(null, "Users");
#pragma warning restore CS0618

        await Assert.That(dbRef).IsEqualTo(BsonNull.Value);
    }

    [Test]
    public async Task SerializeToDbRef_MissingId_ShouldReturnBsonNull()
    {
        var doc = new BsonDocument();
#pragma warning disable CS0618
        var dbRef = DbRefSerializer.SerializeToDbRef(doc, "Users");
#pragma warning restore CS0618

        await Assert.That(dbRef).IsEqualTo(BsonNull.Value);
    }

    [Test]
    public async Task SerializeToDbRef_WithId_ShouldReturnDocument()
    {
        var doc = new BsonDocument().Set("_id", 123);
#pragma warning disable CS0618
        var dbRef = DbRefSerializer.SerializeToDbRef(doc, "Users");
#pragma warning restore CS0618

        await Assert.That(dbRef).IsNotNull();
        var dbRefDoc = (BsonDocument)dbRef;
        await Assert.That(((BsonString)dbRefDoc["$ref"]).Value).IsEqualTo("Users");
        await Assert.That(((BsonInt32)dbRefDoc["$id"]).Value).IsEqualTo(123);
    }

    [Test]
    public async Task IncludeQueryBuilder_ShouldReturnDocuments()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"include_ref_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var users = engine.GetCollection<RefUser>("Users");
            var orders = engine.GetCollection<RefOrder>("Orders");

            users.Insert(new RefUser { Id = 1, Name = "Alice" });
            orders.Insert(new RefOrder { Id = 10, UserId = "1" });

            var builder = new IncludeQueryBuilder<RefOrder>(engine, "Orders")
                .Include(o => o.UserId)
                .Include("UserId");

            var all = builder.FindAll().ToList();
            await Assert.That(all.Count).IsEqualTo(1);

            var byId = builder.FindById(new BsonInt32(10));
            await Assert.That(byId).IsNotNull();

            var filtered = builder.Find(o => o.UserId == "1").ToList();
            await Assert.That(filtered.Count).IsEqualTo(1);
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task IncludeQueryBuilder_Ctor_NullArguments_ShouldThrow()
    {
        await Assert.That(() => new IncludeQueryBuilder<RefOrder>(null!, "Orders"))
            .ThrowsExactly<ArgumentNullException>();

        var dbPath = Path.Combine(Path.GetTempPath(), $"include_ref_ctor_{Guid.NewGuid():N}.db");
        try
        {
            using var engine = new TinyDbEngine(dbPath);
            await Assert.That(() => new IncludeQueryBuilder<RefOrder>(engine, null!))
                .ThrowsExactly<ArgumentNullException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task IncludeQueryBuilder_FindAll_WithoutIncludes_ShouldReturnDocuments()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"include_ref_noincludes_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var orders = engine.GetCollection<RefOrder>("Orders");

            orders.Insert(new RefOrder { Id = 10, UserId = "1" });

            var builder = new IncludeQueryBuilder<RefOrder>(engine, "Orders");

            var all = builder.FindAll().ToList();
            await Assert.That(all.Count).IsEqualTo(1);
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
    public class RefUser
    {
        [Id]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Entity("Orders")]
    public class RefOrder
    {
        [Id]
        public int Id { get; set; }

        [ForeignKey("Users")]
        public string UserId { get; set; } = string.Empty;
    }
}
