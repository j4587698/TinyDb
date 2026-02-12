using System;
using System.IO;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Metadata;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Metadata;

[NotInParallel]
public class SchemaValidationAndAutoCreateTests
{
    [Test]
    public async Task GetCollection_BsonDocument_Should_Throw_When_Schema_Is_Missing()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"schema_autocreate_bson_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            await Assert.That(() => engine.GetCollection<BsonDocument>("users")).Throws<InvalidOperationException>();
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
            var wal = Path.ChangeExtension(dbPath, "-wal.db");
            try { if (File.Exists(wal)) File.Delete(wal); } catch { }
        }
    }

    [Test]
    public async Task GetCollection_Should_AutoCreate_Schema_From_Entity_Type()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"schema_autocreate_entity_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            engine.GetCollection<Person>();

            var schema = engine.MetadataManager.GetMetadata("people");
            await Assert.That(schema).IsNotNull();
            await Assert.That(schema!.TypeName).IsEqualTo(typeof(Person).FullName!);

            var nameColumn = schema.Columns
                .OfType<BsonDocument>()
                .FirstOrDefault(d => d.TryGetValue("n", out var n) && n.ToString() == "name");

            await Assert.That(nameColumn).IsNotNull();
            await Assert.That(nameColumn!.TryGetValue("t", out var t)).IsTrue();
            await Assert.That(t!.ToString()).IsEqualTo("System.String");
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
            var wal = Path.ChangeExtension(dbPath, "-wal.db");
            try { if (File.Exists(wal)) File.Delete(wal); } catch { }
        }
    }

    [Test]
    public async Task SchemaValidation_Required_Should_Enforce_Required_Fields_But_Allow_Unknown()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"schema_validation_required_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions
            {
                EnableJournaling = false,
                SchemaValidationMode = SchemaValidationMode.Required
            });

            engine.MetadataManager.SaveMetadata(new MetadataDocument
            {
                TableName = "users",
                DisplayName = "users",
                Columns = new BsonArray()
                    .AddValue(new BsonDocument().Set("n", "name").Set("t", "System.String").Set("r", true))
            });

            var users = engine.GetCollection<BsonDocument>("users");

            await Assert.That(() => users.Insert(new BsonDocument().Set("age", 123))).Throws<InvalidOperationException>();
            await Assert.That(() => users.Insert(new BsonDocument().Set("name", "Alice").Set("age", 123))).ThrowsNothing();
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
            var wal = Path.ChangeExtension(dbPath, "-wal.db");
            try { if (File.Exists(wal)) File.Delete(wal); } catch { }
        }
    }

    [Test]
    public async Task SchemaValidation_Strict_Should_Reject_Unknown_And_Wrong_Type()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"schema_validation_strict_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions
            {
                EnableJournaling = false,
                SchemaValidationMode = SchemaValidationMode.Strict
            });

            engine.MetadataManager.SaveMetadata(new MetadataDocument
            {
                TableName = "users",
                DisplayName = "users",
                Columns = new BsonArray()
                    .AddValue(new BsonDocument().Set("n", "name").Set("t", "System.String").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "age").Set("t", "System.Int32").Set("r", false))
            });

            var users = engine.GetCollection<BsonDocument>("users");

            await Assert.That(() => users.Insert(new BsonDocument().Set("name", "Alice").Set("extra", 1))).Throws<InvalidOperationException>();
            await Assert.That(() => users.Insert(new BsonDocument().Set("name", "Alice").Set("age", "not-a-number"))).Throws<InvalidOperationException>();
            await Assert.That(() => users.Insert(new BsonDocument().Set("name", "Alice").Set("age", 42))).ThrowsNothing();
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
            var wal = Path.ChangeExtension(dbPath, "-wal.db");
            try { if (File.Exists(wal)) File.Delete(wal); } catch { }
        }
    }

    [Entity("people")]
    public partial class Person
    {
        [Id]
        public int Id { get; set; }

        [PropertyMetadata("Name", Required = true)]
        public string Name { get; set; } = string.Empty;
    }
}
