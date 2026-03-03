using System;
using System.IO;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Metadata;

[NotInParallel]
public sealed class MetadataApiCoverageTests
{
    [Test]
    public async Task SchemaDdlApi_ExportDdl_ShouldValidateArguments()
    {
        await Assert.That(() => SchemaDdlApi.ExportDdl(null!, "users")).Throws<ArgumentNullException>();

        var dbPath = CreateDbPath("metadata_api_exportddl_args");
        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var manager = engine.MetadataManager;

            await Assert.That(() => manager.ExportDdl(" ")).Throws<ArgumentException>();
            await Assert.That(() => manager.ExportDdl("users")).Throws<InvalidOperationException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task SchemaDdlApi_ExportDdl_ShouldExportSavedSchema()
    {
        var dbPath = CreateDbPath("metadata_api_exportddl_success");
        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var manager = engine.MetadataManager;
            manager.SaveMetadata(CreateSchema("users", "UserEntity"));

            var ddl = manager.ExportDdl("users");

            await Assert.That(ddl).Contains("create table \"users\"");
            await Assert.That(ddl).Contains("\"_id\" \"System.Int32\" pk required pn \"Id\"");
            await Assert.That(ddl).Contains("\"name\" \"System.String\" required pn \"Name\"");
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task SchemaDdlApi_ExportAllDdl_ShouldExportAllTables()
    {
        await Assert.That(() => SchemaDdlApi.ExportAllDdl(null!)).Throws<ArgumentNullException>();

        var dbPath = CreateDbPath("metadata_api_exportall");
        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var manager = engine.MetadataManager;

            manager.SaveMetadata(CreateSchema("users", "UserEntity"));
            manager.SaveMetadata(CreateSchema("orders", "OrderEntity"));

            var ddl = manager.ExportAllDdl();

            await Assert.That(ddl).Contains("create table \"users\"");
            await Assert.That(ddl).Contains("create table \"orders\"");
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task CSharpEntityGeneratorApi_GenerateCSharpEntity_ShouldValidateArguments()
    {
        await Assert.That(() => CSharpEntityGeneratorApi.GenerateCSharpEntity(null!, "users", null))
            .Throws<ArgumentNullException>();

        var dbPath = CreateDbPath("metadata_api_generate_args");
        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var manager = engine.MetadataManager;

            await Assert.That(() => manager.GenerateCSharpEntity(" ", null)).Throws<ArgumentException>();
            await Assert.That(() => manager.GenerateCSharpEntity("users", null)).Throws<InvalidOperationException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task CSharpEntityGeneratorApi_GenerateCSharpEntity_ShouldGenerateCode()
    {
        var dbPath = CreateDbPath("metadata_api_generate_success");
        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var manager = engine.MetadataManager;
            manager.SaveMetadata(CreateSchema("users", "UserEntity"));

            var code = manager.GenerateCSharpEntity("users", new CSharpEntityGenerationOptions
            {
                Namespace = "TinyDb.Generated",
                FileScopedNamespace = true
            });

            await Assert.That(code).Contains("namespace TinyDb.Generated;");
            await Assert.That(code).Contains("[Entity(\"users\")]");
            await Assert.That(code).Contains("public partial class UserEntity");
            await Assert.That(code).Contains("public string Name { get; set; } = string.Empty;");
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task CSharpEntityGeneratorApi_GenerateAllCSharpEntities_ShouldReturnAllTables()
    {
        await Assert.That(() => CSharpEntityGeneratorApi.GenerateAllCSharpEntities(null!, null))
            .Throws<ArgumentNullException>();

        var dbPath = CreateDbPath("metadata_api_generate_all");
        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            var manager = engine.MetadataManager;

            manager.SaveMetadata(CreateSchema("users", "UserEntity"));
            manager.SaveMetadata(CreateSchema("orders", "OrderEntity"));

            var all = manager.GenerateAllCSharpEntities();

            await Assert.That(all.Count).IsEqualTo(2);
            await Assert.That(all.ContainsKey("users")).IsTrue();
            await Assert.That(all.ContainsKey("orders")).IsTrue();
            await Assert.That(all["users"]).Contains("public partial class UserEntity");
            await Assert.That(all["orders"]).Contains("[Entity(\"orders\")]");
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    private static MetadataDocument CreateSchema(string tableName, string typeName)
    {
        return new MetadataDocument
        {
            TableName = tableName,
            TypeName = $"TinyDb.Tests.Metadata.{typeName}",
            DisplayName = tableName,
            Columns = new BsonArray()
                .AddValue(new BsonDocument()
                    .Set("n", "_id")
                    .Set("pn", "Id")
                    .Set("t", "System.Int32")
                    .Set("pk", true)
                    .Set("r", true)
                    .Set("o", 1))
                .AddValue(new BsonDocument()
                    .Set("n", "name")
                    .Set("pn", "Name")
                    .Set("t", "System.String")
                    .Set("r", true)
                    .Set("o", 2))
        };
    }

    private static string CreateDbPath(string name)
    {
        return Path.Combine(Path.GetTempPath(), $"{name}_{Guid.NewGuid():N}.db");
    }

    private static void CleanupDb(string dbPath)
    {
        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
        var wal = Path.ChangeExtension(dbPath, "-wal.db");
        try { if (File.Exists(wal)) File.Delete(wal); } catch { }
    }
}
