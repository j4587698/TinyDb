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
public class OnlinePatchingTests
{
    [Test]
    public async Task FindById_ShouldApplySchemaDefaults_ForBsonDocumentResults()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"online_patching_{Guid.NewGuid():N}.db");

        try
        {
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });

            var schema = new MetadataDocument
            {
                TableName = "users",
                DisplayName = "users",
                Columns = new BsonArray()
                    .AddValue(new BsonDocument().Set("n", "age").Set("t", "System.Int32").Set("r", true))
                    .AddValue(new BsonDocument().Set("n", "nickname").Set("t", "System.String").Set("r", false))
            };

            engine.MetadataManager.SaveMetadata(schema);

            var users = engine.GetCollection<BsonDocument>("users");
            users.Insert(new BsonDocument().Set("_id", "u1").Set("name", "Alice"));

            var loaded = users.FindById("u1");
            await Assert.That(loaded).IsNotNull();

            await Assert.That(loaded!.ContainsKey("age")).IsTrue();
            await Assert.That(loaded["age"].ToInt32()).IsEqualTo(0);

            await Assert.That(loaded.ContainsKey("nickname")).IsTrue();
            await Assert.That(loaded["nickname"].IsNull).IsTrue();
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
            var wal = Path.ChangeExtension(dbPath, "-wal.db");
            try { if (File.Exists(wal)) File.Delete(wal); } catch { }
        }
    }
}

