using System;
using System.IO;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
[SkipInAot]
public sealed class TransactionManagerCoverageEdgeCasesTests
{
    [Test]
    public async Task GetForeignKeyDefinitions_WhenMetadataReadThrows_ShouldWrap()
    {
        var dbPath = CreateDbPath("tx_fk_metadata_wrap");
        var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        try
        {
            var manager = engine.TransactionManager;

            engine.Dispose();

            await Assert.That(() => manager.GetForeignKeyDefinitions("orders"))
                .Throws<InvalidOperationException>();
        }
        finally
        {
            CleanupDb(dbPath);
        }
    }

    [Test]
    public async Task RollbackSingleOperation_ShouldCoverUpdateDeleteAndDropIndexCompensation()
    {
        var dbPath = CreateDbPath("tx_rollback_compensation");
        var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        try
        {
            var collectionName = "rb_comp_col";
            var col = engine.GetBsonCollection(collectionName);
            col.Insert(new BsonDocument().Set("_id", 1).Set("v", 1));

            var manager = engine.TransactionManager;

            var updateCompensation = new TransactionOperation(
                TransactionOperationType.Update,
                collectionName,
                documentId: new BsonInt32(1),
                originalDocument: new BsonDocument()
                    .Set("_id", 1)
                    .Set("_collection", collectionName)
                    .Set("v", 99));
            manager.RollbackSingleOperation(updateCompensation);

            var updated = engine.FindById(collectionName, new BsonInt32(1));
            await Assert.That(updated).IsNotNull();
            await Assert.That(updated!["v"].ToInt32()).IsEqualTo(99);

            var deleteCompensation = new TransactionOperation(
                TransactionOperationType.Delete,
                collectionName,
                documentId: new BsonInt32(2),
                originalDocument: new BsonDocument()
                    .Set("_id", 2)
                    .Set("_collection", collectionName)
                    .Set("v", 2));
            manager.RollbackSingleOperation(deleteCompensation);

            var reinserted = engine.FindById(collectionName, new BsonInt32(2));
            await Assert.That(reinserted).IsNotNull();

            var dropIndexCompensation = new TransactionOperation(
                TransactionOperationType.DropIndex,
                collectionName,
                indexName: "idx_v",
                indexFields: new[] { "v" },
                indexUnique: false);
            manager.RollbackSingleOperation(dropIndexCompensation);

            var index = engine.GetIndexManager(collectionName).GetIndex("idx_v");
            await Assert.That(index).IsNotNull();
        }
        finally
        {
            try { engine.Dispose(); } catch { }
            CleanupDb(dbPath);
        }
    }

    private static string CreateDbPath(string tag)
    {
        return Path.Combine(Path.GetTempPath(), $"{tag}_{Guid.NewGuid():N}.db");
    }

    private static void CleanupDb(string dbPath)
    {
        try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }

        var directory = Path.GetDirectoryName(dbPath) ?? string.Empty;
        var name = Path.GetFileNameWithoutExtension(dbPath);
        var ext = Path.GetExtension(dbPath).TrimStart('.');
        var walPath = Path.Combine(directory, $"{name}-wal.{ext}");
        try { if (File.Exists(walPath)) File.Delete(walPath); } catch { }
    }
}
