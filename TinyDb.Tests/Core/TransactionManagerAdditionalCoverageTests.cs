using System.Reflection;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Metadata;
using EntityPropertyMetadata = TinyDb.Metadata.PropertyMetadata;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

[NotInParallel]
public class TransactionManagerAdditionalCoverageTests
{
    [Test]
    public async Task Rollback_WithUnsupportedOperation_ShouldWrapException()
    {
        var testDirectory = CreateTempDirectory();
        try
        {
            var dbPath = Path.Combine(testDirectory, "test.db");
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            using var manager = new TransactionManager(engine, maxTransactions: 10, transactionTimeout: TimeSpan.FromMinutes(1));

            var tx = (Transaction)manager.BeginTransaction();
            manager.RecordOperation(tx, new TransactionOperation((TransactionOperationType)999, "c"));

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
            {
                tx.Rollback();
                return Task.CompletedTask;
            });
        }
        finally
        {
            TryDeleteDirectory(testDirectory);
        }
    }

    [Test]
    public async Task Commit_WithUnsupportedOperationType_ShouldWrapException()
    {
        var testDirectory = CreateTempDirectory();
        try
        {
            var dbPath = Path.Combine(testDirectory, "test.db");
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            using var manager = new TransactionManager(engine, maxTransactions: 10, transactionTimeout: TimeSpan.FromMinutes(1));

            var tx = (Transaction)manager.BeginTransaction();
            manager.RecordOperation(tx, new TransactionOperation((TransactionOperationType)999, "c"));

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
            {
                tx.Commit();
                return Task.CompletedTask;
            });
        }
        finally
        {
            TryDeleteDirectory(testDirectory);
        }
    }

    [Test]
    public async Task Commit_DropIndex_WithNullCollectionName_ShouldWrapApplyException()
    {
        var testDirectory = CreateTempDirectory();
        try
        {
            var dbPath = Path.Combine(testDirectory, "test.db");
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            using var manager = new TransactionManager(engine, maxTransactions: 10, transactionTimeout: TimeSpan.FromMinutes(1));

            var tx = (Transaction)manager.BeginTransaction();
            manager.RecordOperation(tx, new TransactionOperation(TransactionOperationType.DropIndex, null!, indexName: "idx"));

            await Assert.ThrowsAsync<InvalidOperationException>(() =>
            {
                tx.Commit();
                return Task.CompletedTask;
            });
        }
        finally
        {
            TryDeleteDirectory(testDirectory);
        }
    }

    [Test]
    public async Task Commit_ForeignKeyScan_MetadataReadError_ShouldBeIgnored()
    {
        var testDirectory = CreateTempDirectory();
        try
        {
            var dbPath = Path.Combine(testDirectory, "test.db");
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            using var manager = new TransactionManager(engine, maxTransactions: 10, transactionTimeout: TimeSpan.FromMinutes(1));

            var badMetadata = new BsonDocument().Set("_id", new BsonDocument()).Set("CollectionName", "X");
            engine.GetCollection<BsonDocument>("__metadata_Broken").Insert(badMetadata);

            var tx = (Transaction)manager.BeginTransaction();
            tx.RecordInsert("fk_scan_test", new BsonDocument().Set("a", 1));

            tx.Commit();
            await Assert.That(engine.FindAll("fk_scan_test").Any()).IsTrue();
        }
        finally
        {
            TryDeleteDirectory(testDirectory);
        }
    }

    [Test]
    public async Task CleanupExpiredTransactions_ShouldRemoveExpiredEntries()
    {
        var testDirectory = CreateTempDirectory();
        try
        {
            var dbPath = Path.Combine(testDirectory, "test.db");
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            using var manager = new TransactionManager(engine, maxTransactions: 10, transactionTimeout: TimeSpan.FromTicks(-1));

            _ = manager.BeginTransaction();
            await Assert.That(manager.ActiveTransactionCount).IsEqualTo(1);

            var method = typeof(TransactionManager).GetMethod("CheckAndCleanupExpiredTransactions", BindingFlags.Instance | BindingFlags.NonPublic);
            await Assert.That(method).IsNotNull();
            method!.Invoke(manager, null);

            await Assert.That(manager.ActiveTransactionCount).IsEqualTo(0);
        }
        finally
        {
            TryDeleteDirectory(testDirectory);
        }
    }

    [Test]
    public async Task Commit_ForeignKeyValidation_ShouldCover_FieldLookupBranches()
    {
        var testDirectory = CreateTempDirectory();
        try
        {
            var dbPath = Path.Combine(testDirectory, "test.db");
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            using var manager = new TransactionManager(engine, maxTransactions: 10, transactionTimeout: TimeSpan.FromMinutes(1));

            var refId = ObjectId.NewObjectId();
            engine.GetCollection<BsonDocument>("Ref").Insert(new BsonDocument().Set("_id", refId));

            var meta = new EntityMetadata
            {
                TypeName = "FkEntity",
                CollectionName = "fk_branch_test",
                DisplayName = "FkEntity",
                Properties = new List<EntityPropertyMetadata>
                {
                    new EntityPropertyMetadata { PropertyName = "", PropertyType = typeof(ObjectId).FullName!, ForeignKeyCollection = "Ref" },
                    new EntityPropertyMetadata { PropertyName = "A", PropertyType = typeof(ObjectId).FullName!, ForeignKeyCollection = "Ref" },
                    new EntityPropertyMetadata { PropertyName = "B", PropertyType = typeof(ObjectId).FullName!, ForeignKeyCollection = "Ref" },
                    new EntityPropertyMetadata { PropertyName = "C", PropertyType = typeof(ObjectId).FullName!, ForeignKeyCollection = "Ref" },
                    new EntityPropertyMetadata { PropertyName = "D", PropertyType = typeof(ObjectId).FullName!, ForeignKeyCollection = "Ref" }
                }
            };

            engine.GetCollection<MetadataDocument>("__metadata_FkBranchTest").Insert(MetadataDocument.FromEntityMetadata(meta));

            var tx = (Transaction)manager.BeginTransaction();
            tx.RecordInsert(
                "fk_branch_test",
                new BsonDocument()
                    .Set("a", new BsonObjectId(refId))
                    .Set("B", new BsonObjectId(refId))
                    .Set("d", BsonNull.Value));

            tx.Commit();
            await Assert.That(engine.FindAll("fk_branch_test").Any()).IsTrue();
        }
        finally
        {
            TryDeleteDirectory(testDirectory);
        }
    }

    private static string CreateTempDirectory()
    {
        var path = Path.Combine(Path.GetTempPath(), "TinyDbTransactionManagerAdditionalCoverageTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void TryDeleteDirectory(string path)
    {
        if (!Directory.Exists(path)) return;
        try { Directory.Delete(path, true); } catch { }
    }
}
