using System;
using System.IO;
using TinyDb.Bson;
using TinyDb.Index;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class IndexManagerAdditionalCoverageTests
{
    [Test]
    public async Task CollectionName_ReturnsProvidedName()
    {
        using var manager = new IndexManager("MyCol");
        await Assert.That(manager.CollectionName).IsEqualTo("MyCol");
    }

    [Test]
    public async Task Constructor_NullArguments_ShouldThrow()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"idx_mgr_ctor_{Guid.NewGuid():N}.db");
        DiskStream? ds = null;
        PageManager? pm = null;
        try
        {
            ds = new DiskStream(dbPath);
            pm = new PageManager(ds, 4096);

            await Assert.That(() => new IndexManager(null!, pm)).Throws<ArgumentNullException>();
            await Assert.That(() => new IndexManager("col", null!)).Throws<ArgumentNullException>();
            await Assert.That(() => new IndexManager(null!)).Throws<ArgumentNullException>();
        }
        finally
        {
            pm?.Dispose();
            ds?.Dispose();
            TryDelete(dbPath);
        }
    }

    [Test]
    public async Task CreateIndex_WhenUniquenessDiffers_ShouldThrow()
    {
        using var manager = new IndexManager("col");
        manager.CreateIndex("idx_name", new[] { "Name" }, unique: false);

        await Assert.That(() => manager.CreateIndex("idx_name", new[] { "Name" }, unique: true))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task CreateIndex_WhenFieldsDiffer_ShouldThrow()
    {
        using var manager = new IndexManager("col");
        manager.CreateIndex("idx_name", new[] { "Name" }, unique: false);

        await Assert.That(() => manager.CreateIndex("idx_name", new[] { "OtherField" }, unique: false))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task CreateIndex_WithEmptyFieldName_ShouldStillCreateIndex()
    {
        using var manager = new IndexManager("col");

        var created = manager.CreateIndex("idx_empty", new[] { "" });
        await Assert.That(created).IsTrue();
        await Assert.That(manager.IndexExists("idx_empty")).IsTrue();
    }

    [Test]
    public async Task ValidateAllIndexes_WhenPageManagerDisposed_ShouldReportInvalid()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"idx_mgr_validate_{Guid.NewGuid():N}.db");
        DiskStream? ds = null;
        PageManager? pm = null;
        try
        {
            ds = new DiskStream(dbPath);
            pm = new PageManager(ds, 4096);

            using var manager = new IndexManager("col", pm);
            manager.CreateIndex("idx_age", new[] { "Age" });

            pm.Dispose();
            pm = null;

            var result = manager.ValidateAllIndexes();
            await Assert.That(result.TotalIndexes).IsEqualTo(1);
            await Assert.That(result.InvalidIndexes).IsEqualTo(1);
            await Assert.That(result.Errors.Count).IsEqualTo(1);
        }
        finally
        {
            pm?.Dispose();
            ds?.Dispose();
            TryDelete(dbPath);
        }
    }

    [Test]
    public async Task InsertDocument_WhenIndexInsertThrows_ShouldWrapInnerException()
    {
        using var manager = new IndexManager("col");
        manager.CreateIndex("idx_age", new[] { "Age" });

        var doc = new BsonDocument().Set("_id", 1).Set("age", 25);
        try
        {
            manager.InsertDocument(doc, null!);
            throw new Exception("Expected InvalidOperationException to be thrown.");
        }
        catch (InvalidOperationException ex)
        {
            await Assert.That(ex.InnerException is ArgumentNullException).IsTrue();
        }
    }

    [Test]
    public async Task UpdateDocument_WhenDuplicateKeyInUniqueIndex_ShouldRollbackOldKey()
    {
        using var manager = new IndexManager("col");
        manager.CreateIndex("idx_age", new[] { "Age" }, unique: true);

        var idx = manager.GetIndex("idx_age")!;

        var doc1 = new BsonDocument().Set("_id", 1).Set("age", 25);
        var doc2 = new BsonDocument().Set("_id", 2).Set("age", 30);

        manager.InsertDocument(doc1, 1);
        manager.InsertDocument(doc2, 2);

        var updatedDoc2 = new BsonDocument().Set("_id", 2).Set("age", 25);
        await Assert.That(() => manager.UpdateDocument(doc2, updatedDoc2, 2)).Throws<InvalidOperationException>();

        await Assert.That(idx.Contains(new IndexKey(30), 2)).IsTrue();
        await Assert.That(idx.Contains(new IndexKey(25), 2)).IsFalse();
    }

    [Test]
    public async Task InsertDocument_CompositeIndex_MissingField_UsesBsonNull()
    {
        using var manager = new IndexManager("col");
        manager.CreateIndex("idx_age_name", new[] { "Age", "Name" });

        var doc = new BsonDocument().Set("_id", 1).Set("age", 25);
        manager.InsertDocument(doc, 1);

        var idx = manager.GetIndex("idx_age_name")!;
        await Assert.That(idx.Contains(new IndexKey(25, BsonNull.Value), 1)).IsTrue();
    }

    [Test]
    public async Task GetBestIndex_WithUniqueBonus_ShouldPreferUniqueIndex()
    {
        using var manager = new IndexManager("col");
        manager.CreateIndex("idx_unique", new[] { "Age" }, unique: true);
        manager.CreateIndex("idx_nonunique", new[] { "Age" }, unique: false);

        var best = manager.GetBestIndex(new[] { "age" });
        await Assert.That(best).IsNotNull();
        await Assert.That(best!.Name).IsEqualTo("idx_unique");
    }

    private static void TryDelete(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); } catch { }
    }
}
