using System;
using System.IO;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Metadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Metadata;

[NotInParallel]
public sealed class MetadataManagerExtendedCoverageTests
{
    [Test]
    public async Task EnsureSchema_WhenReadOnlyAndSchemaMissing_ShouldThrow()
    {
        var path = CreateDbPath("metadata_readonly_missing");
        try
        {
            using (var init = new TinyDbEngine(path, new TinyDbOptions { EnableJournaling = false }))
            {
                // Ensure database file exists.
                _ = init.GetCollection<ReadOnlyEntity>();
            }

            using var engine = new TinyDbEngine(path, new TinyDbOptions { ReadOnly = true, EnableJournaling = false });
            await Assert.That(() => engine.GetCollection<AnotherReadOnlyEntity>("missing_schema_table")).Throws<InvalidOperationException>();
        }
        finally
        {
            CleanupDb(path);
        }
    }

    [Test]
    public async Task MetadataManager_BuildValidationProfileStrict_ShouldCoverKindAndSystemFieldBranches()
    {
        var path = CreateDbPath("metadata_kind_profile");
        try
        {
            using var engine = new TinyDbEngine(path, new TinyDbOptions { EnableJournaling = false });
            var manager = engine.MetadataManager;

            manager.SaveMetadata(new MetadataDocument
            {
                TableName = "users",
                DisplayName = "users",
                Columns = new BsonArray()
                    .AddValue(new BsonDocument().Set("n", "UserName").Set("t", "System.String").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "Attrs").Set("t", "System.Collections.Generic.Dictionary<System.String,System.Int32>").Set("r", false))
                    .AddValue(new BsonDocument().Set("n", "Bytes").Set("t", "System.Byte[]").Set("r", false))
                    .AddValue(new BsonDocument().Set("n", "Meta").Set("t", "TinyDb.Bson.BsonDocument").Set("r", false))
                    .AddValue(new BsonDocument().Set("n", "Tags").Set("t", "TinyDb.Bson.BsonArray").Set("r", false))
                    .AddValue(new BsonDocument().Set("n", "When").Set("t", "System.DateTime").Set("r", false))
                    .AddValue(new BsonDocument().Set("n", "Oid").Set("t", "TinyDb.Bson.ObjectId").Set("r", false))
                    .AddValue(new BsonDocument().Set("n", "Flag").Set("t", "System.Boolean").Set("r", false))
            });

            // Use a fresh manager instance to force validation profile build path.
            var freshManager = new MetadataManager(engine);

            var doc = new BsonDocument()
                .Set("userName", "Alice") // camelCase compatibility for required field "UserName"
                .Set("Attrs", new BsonDocument().Set("k", 1))
                .Set("Bytes", new BsonBinary(new byte[] { 1, 2, 3 }))
                .Set("Meta", new BsonDocument().Set("x", 1))
                .Set("Tags", new BsonArray().AddValue("a"))
                .Set("When", new BsonDateTime(DateTime.UtcNow))
                .Set("Oid", new BsonObjectId(ObjectId.NewObjectId()))
                .Set("Flag", new BsonBoolean(true))
                .Set("_isLargeDocument", new BsonBoolean(true))
                .Set("_largeDocumentIndex", new BsonInt32(0))
                .Set("_largeDocumentSize", new BsonInt32(10));

            await Assert.That(() => freshManager.ValidateDocumentForWrite("users", doc, SchemaValidationMode.Strict)).ThrowsNothing();
        }
        finally
        {
            CleanupDb(path);
        }
    }

    [Test]
    public async Task MetadataManager_DeleteAndMissingSchemaBranches_ShouldWork()
    {
        var path = CreateDbPath("metadata_delete_missing");
        try
        {
            using var engine = new TinyDbEngine(path, new TinyDbOptions { EnableJournaling = false });
            var manager = engine.MetadataManager;

            manager.SaveMetadata(new MetadataDocument
            {
                TableName = "to_delete",
                DisplayName = "to_delete",
                Columns = new BsonArray()
            });

            manager.DeleteMetadata("to_delete");
            await Assert.That(manager.GetMetadata("to_delete")).IsNull();

            await Assert.That(() => manager.ValidateDocumentForWrite("not_exists", new BsonDocument(), SchemaValidationMode.Required))
                .Throws<InvalidOperationException>();
        }
        finally
        {
            CleanupDb(path);
        }
    }

    [Test]
    public async Task MetadataManager_GetMetadata_WhenEngineDisposed_ShouldWrapException()
    {
        var path = CreateDbPath("metadata_disposed_wrap");
        MetadataManager manager;
        TinyDbEngine engine = new TinyDbEngine(path, new TinyDbOptions { EnableJournaling = false });
        try
        {
            manager = new MetadataManager(engine);
        }
        finally
        {
            engine.Dispose();
        }

        await Assert.That(() => manager.GetMetadata("users")).Throws<InvalidOperationException>();
        CleanupDb(path);
    }

    [Test]
    public async Task ApplySchemaDefaults_ShouldCoverDefaultValueBranches()
    {
        var path = CreateDbPath("metadata_defaults");
        try
        {
            using var engine = new TinyDbEngine(path, new TinyDbOptions { EnableJournaling = false });
            var manager = engine.MetadataManager;

            manager.SaveMetadata(new MetadataDocument
            {
                TableName = "defaults",
                DisplayName = "defaults",
                Columns = new BsonArray()
                    .AddValue(new BsonDocument().Set("n", "_id").Set("t", "System.Int32").Set("pk", true).Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "explicit").Set("t", "System.String").Set("r", true).Set("dv", "x"))
                    .AddValue(new BsonDocument().Set("n", "i64").Set("t", "System.Int64").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "dbl").Set("t", "System.Double").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "flt").Set("t", "System.Single").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "dec").Set("t", "System.Decimal").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "flag").Set("t", "System.Boolean").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "when").Set("t", "System.DateTime").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "gid").Set("t", "System.Guid").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "bytes").Set("t", "System.Byte[]").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "oid").Set("t", "TinyDb.Bson.ObjectId").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "arr").Set("t", "System.Collections.Generic.List<System.Int32>").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "map").Set("t", "System.Collections.Generic.Dictionary<System.String,System.Int32>").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "unknown").Set("t", "TinyDb.Custom.Unknown").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "optional").Set("t", "System.String").Set("r", false))
            });

            var patched = manager.ApplySchemaDefaults("defaults", new BsonDocument());

            await Assert.That(patched["explicit"].ToString()).IsEqualTo("x");
            await Assert.That(patched["i64"].ToInt64()).IsEqualTo(0L);
            await Assert.That(patched["dbl"].ToDouble()).IsEqualTo(0d);
            await Assert.That(patched["flt"].ToDouble()).IsEqualTo(0d);
            await Assert.That(patched["dec"].BsonType).IsEqualTo(BsonType.Decimal128);
            await Assert.That(patched["flag"].ToBoolean()).IsFalse();
            await Assert.That(patched["when"].BsonType).IsEqualTo(BsonType.DateTime);
            await Assert.That(patched["gid"].BsonType).IsEqualTo(BsonType.String);
            await Assert.That(patched["bytes"].BsonType).IsEqualTo(BsonType.Binary);
            await Assert.That(patched["oid"].BsonType).IsEqualTo(BsonType.ObjectId);
            await Assert.That(patched["arr"].BsonType).IsEqualTo(BsonType.Array);
            await Assert.That(patched["map"].BsonType).IsEqualTo(BsonType.Document);
            await Assert.That(patched["unknown"].IsNull).IsTrue();
            await Assert.That(patched["optional"].IsNull).IsTrue();
        }
        finally
        {
            CleanupDb(path);
        }
    }

    [Test]
    public async Task MetadataManager_PrivateKindHelpersAndValidationProfileCacheMiss_ShouldBeCovered()
    {
        var path = CreateDbPath("metadata_private_kind_helpers");
        try
        {
            using var engine = new TinyDbEngine(path, new TinyDbOptions { EnableJournaling = false });
            var manager = engine.MetadataManager;

            manager.SaveMetadata(new MetadataDocument
            {
                TableName = "kind_cov",
                DisplayName = "kind_cov",
                Columns = new BsonArray()
                    .AddValue(new BsonDocument().Set("n", "name").Set("t", "System.String").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "blob").Set("t", "System.Byte[]").Set("r", false))
            });

            var profilesField = typeof(MetadataManager).GetField("_validationProfiles", BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new MissingFieldException(typeof(MetadataManager).FullName, "_validationProfiles");
            var profiles = profilesField.GetValue(manager) ?? throw new InvalidOperationException("Validation profile cache instance is missing.");
            // In NativeAOT, private member metadata can be reduced; run cache clear only when method metadata is available.
            var clearMethod = profilesField.FieldType.GetMethod("Clear", BindingFlags.Instance | BindingFlags.Public);
            clearMethod?.Invoke(profiles, Array.Empty<object>());

            var validDoc = new BsonDocument().Set("name", "ok");
            await Assert.That(() => manager.ValidateDocumentForWrite("kind_cov", validDoc, SchemaValidationMode.Required)).ThrowsNothing();

            var tryGetExpectedKind = typeof(MetadataManager).GetMethod("TryGetExpectedKind", BindingFlags.Static | BindingFlags.NonPublic)
                ?? throw new MissingMethodException(typeof(MetadataManager).FullName, "TryGetExpectedKind");
            var expectedKindType = tryGetExpectedKind.GetParameters()[1].ParameterType.GetElementType()
                ?? throw new InvalidOperationException("Expected kind out parameter type is missing.");

            object? expectedKind = Enum.ToObject(expectedKindType, 0);
            var expectedKindArgs = new object?[] { "System.Byte[]", expectedKind };
            var resolved = (bool)tryGetExpectedKind.Invoke(null, expectedKindArgs)!;
            await Assert.That(resolved).IsTrue();
            await Assert.That(expectedKindArgs[1]!.ToString()).IsEqualTo("Binary");

            var isCompatible = typeof(MetadataManager).GetMethod("IsCompatible", BindingFlags.Static | BindingFlags.NonPublic)
                ?? throw new MissingMethodException(typeof(MetadataManager).FullName, "IsCompatible");
            var binaryKind = Enum.Parse(expectedKindType, "Binary");
            var binaryOk = (bool)isCompatible.Invoke(null, new object?[] { new BsonBinary(new byte[] { 9 }), binaryKind })!;
            await Assert.That(binaryOk).IsTrue();

            var unknownKind = Enum.ToObject(expectedKindType, 999);
            var unknownOk = (bool)isCompatible.Invoke(null, new object?[] { new BsonString("x"), unknownKind })!;
            await Assert.That(unknownOk).IsTrue();

            var getTypeDefaultValue = typeof(MetadataManager).GetMethod("GetTypeDefaultValue", BindingFlags.Static | BindingFlags.NonPublic)
                ?? throw new MissingMethodException(typeof(MetadataManager).FullName, "GetTypeDefaultValue");
            var defaultString = (BsonValue)getTypeDefaultValue.Invoke(null, new object[] { "System.String" })!;

            await Assert.That(defaultString.BsonType).IsEqualTo(BsonType.String);
            await Assert.That(defaultString.ToString()).IsEqualTo(string.Empty);
        }
        finally
        {
            CleanupDb(path);
        }
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

    private sealed class ReadOnlyEntity
    {
        public int Id { get; set; }
    }

    private sealed class AnotherReadOnlyEntity
    {
        public int Id { get; set; }
    }
}
