using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.References;
using TinyDb.Serialization;
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

    [Test]
    public async Task IncludeQueryBuilder_ShouldLoadNavigationPropertyByForeignKey()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"include_ref_navigation_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var users = engine.GetCollection<RefUser>("Users");
            var orders = engine.GetCollection<RefOrderWithUser>("OrdersWithUsers");

            users.Insert(new RefUser { Id = 1, Name = "Alice" });
            orders.Insert(new RefOrderWithUser { Id = 10, UserId = 1 });

            var order = new IncludeQueryBuilder<RefOrderWithUser>(engine, "OrdersWithUsers")
                .Include(o => o.UserId)
                .FindById(new BsonInt32(10));

            await Assert.That(order).IsNotNull();
            await Assert.That(order!.User).IsNotNull();
            await Assert.That(order.User!.Name).IsEqualTo("Alice");

            var orderByNavigation = new IncludeQueryBuilder<RefOrderWithUser>(engine, "OrdersWithUsers")
                .Include(o => o.User)
                .FindById(new BsonInt32(10));

            await Assert.That(orderByNavigation).IsNotNull();
            await Assert.That(orderByNavigation!.User).IsNotNull();
            await Assert.That(orderByNavigation.User!.Name).IsEqualTo("Alice");
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task IncludeQueryBuilder_StringFind_ShouldLoadNavigationProperty()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"include_ref_string_find_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var users = engine.GetCollection<RefUser>("Users");
            var orders = engine.GetCollection<RefOrderWithUser>("OrdersWithUsers");

            users.Insert(new RefUser { Id = 1, Name = "Alice" });
            users.Insert(new RefUser { Id = 2, Name = "Bob" });
            orders.Insert(new RefOrderWithUser { Id = 10, UserId = 1 });
            orders.Insert(new RefOrderWithUser { Id = 11, UserId = 2 });

            var order = new IncludeQueryBuilder<RefOrderWithUser>(engine, "OrdersWithUsers")
                .Include("User")
                .Find("UserId = @userId", QueryParams.Create(("userId", 1)))
                .Single();

            await Assert.That(order.User).IsNotNull();
            await Assert.That(order.User!.Name).IsEqualTo("Alice");
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task IncludeQueryBuilder_SqlFind_ShouldLoadNavigationProperty()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"include_ref_sql_find_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath);
            var users = engine.GetCollection<RefUser>("Users");
            var orders = engine.GetCollection<RefOrderWithUser>("OrdersWithUsers");

            users.Insert(new RefUser { Id = 1, Name = "Alice" });
            users.Insert(new RefUser { Id = 2, Name = "Bob" });
            orders.Insert(new RefOrderWithUser { Id = 10, UserId = 1 });
            orders.Insert(new RefOrderWithUser { Id = 11, UserId = 2 });

            var order = new IncludeQueryBuilder<RefOrderWithUser>(engine, "OrdersWithUsers")
                .Include("User")
                .FindSql("select * from OrdersWithUsers where UserId = @userId", QueryParams.Create(("userId", 2)))
                .Single();

            await Assert.That(order.User).IsNotNull();
            await Assert.That(order.User!.Name).IsEqualTo("Bob");
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task AotAdapter_ShouldExposeForeignKeyMetadata_ForInclude()
    {
        var found = AotHelperRegistry.TryGetAdapter<RefOrderWithUser>(out var adapter);

        await Assert.That(found).IsTrue();
        await Assert.That(adapter).IsNotNull();

        var reference = adapter!.ForeignKeyReferences
            .SingleOrDefault(r => r.ForeignKeyPropertyName == nameof(RefOrderWithUser.UserId));

        await Assert.That(reference).IsNotNull();
        await Assert.That(reference!.CollectionName).IsEqualTo("Users");
        await Assert.That(reference.TargetPropertyName).IsEqualTo(nameof(RefOrderWithUser.User));
        await Assert.That(reference.TargetPropertyType).IsEqualTo(typeof(RefUser));

        var order = new RefOrderWithUser();
        var user = new RefUser { Id = 99, Name = "Bob" };

        await Assert.That(adapter.TrySetPropertyValueUntyped(order, nameof(RefOrderWithUser.User), user)).IsTrue();
        await Assert.That(ReferenceEquals(order.User, user)).IsTrue();
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

    [Entity("OrdersWithUsers")]
    public class RefOrderWithUser
    {
        [Id]
        public int Id { get; set; }

        [ForeignKey("Users")]
        public int UserId { get; set; }

        public RefUser? User { get; set; }
    }
}
