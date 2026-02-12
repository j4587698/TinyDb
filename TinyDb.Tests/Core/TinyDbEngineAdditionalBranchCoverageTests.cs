using System;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TinyDbEngineAdditionalBranchCoverageTests
{
    private static CollectionState GetCollectionState(TinyDbEngine engine, string collectionName)
    {
        var field = typeof(TinyDbEngine).GetField("_collectionStates", BindingFlags.NonPublic | BindingFlags.Instance);
        if (field == null) throw new InvalidOperationException("Unable to access _collectionStates");

        var states = (ConcurrentDictionary<string, CollectionState>?)field.GetValue(engine);
        if (states == null) throw new InvalidOperationException("Unable to read _collectionStates");

        return states[collectionName];
    }

    [Test]
    public async Task Ctor_WhenDirectoryMissing_ShouldCreateDirectory()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_missingdir_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, true);

            using var engine = new TinyDbEngine(dbPath);
            await Assert.That(Directory.Exists(dir)).IsTrue();
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }

    [Test]
    public async Task GetCollection_WithNullOrEmptyName_ShouldUseTypeName()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_colname_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);

            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });

            engine.EnsureBsonSchema(nameof(BsonDocument));
            engine.EnsureBsonSchema("explicit");

            engine.GetCollection<BsonDocument>((string?)null).Insert(new BsonDocument().Set("x", 1));
            await Assert.That(engine.CollectionExists(nameof(BsonDocument))).IsTrue();

            engine.GetCollection<BsonDocument>("").Insert(new BsonDocument().Set("y", 2));
            await Assert.That(engine.CollectionExists(nameof(BsonDocument))).IsTrue();

            engine.GetCollection<BsonDocument>("explicit").Insert(new BsonDocument().Set("z", 3));
            await Assert.That(engine.CollectionExists("explicit")).IsTrue();
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }

    [Test]
    public async Task CompactDatabase_WhenTempFileExists_ShouldDeleteAndProceed()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_compact_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);

            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
            engine.GetBsonCollection("c").Insert(new BsonDocument().Set("x", 1));

            File.WriteAllText(dbPath + ".compact", "stale");
            await Assert.That(File.Exists(dbPath + ".compact")).IsTrue();

            engine.CompactDatabase();
            await Assert.That(File.Exists(dbPath + ".compact")).IsFalse();
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }

    [Test]
    public async Task Ctor_WhenPasswordTooShort_ShouldThrow()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_pwd_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);
            await Assert.That(() => new TinyDbEngine(dbPath, new TinyDbOptions { Password = "abc", EnableJournaling = false }))
                .Throws<ArgumentException>();
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }

    [Test]
    public async Task InsertDocumentsAsync_WhenDocsNull_ShouldThrow()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_insert_async_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });

            await Assert.ThrowsAsync<ArgumentNullException>(() => engine.InsertDocumentsAsync("c", null!));
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }

    [Test]
    public async Task UpdateDocumentInternal_WhenDocumentMissingId_ShouldReturnZero()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_update_noid_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });

            var count = engine.UpdateDocumentInternal("c", new BsonDocument().Set("x", 1));
            await Assert.That(count).IsEqualTo(0);
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }

    [Test]
    public async Task UpdateAndDelete_WhenIndexEntryOutOfRange_ShouldReturnZero()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_index_mismatch_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });

            var collection = engine.GetBsonCollection("c");
            var id = collection.Insert(new BsonDocument().Set("x", 1));

            var state = GetCollectionState(engine, "c");
            await Assert.That(state.Index.TryGet(id, out var location)).IsTrue();

            state.Index.Set(id, new DocumentLocation(location.PageId, (ushort)(location.EntryIndex + 100)));

            var updateDoc = new BsonDocument().Set("_id", id).Set("x", 2);
            await Assert.That(engine.UpdateDocumentInternal("c", updateDoc)).IsEqualTo(0);
            await Assert.That(await engine.UpdateDocumentAsync("c", updateDoc)).IsEqualTo(0);

            await Assert.That(engine.DeleteDocument("c", id)).IsEqualTo(0);
            await Assert.That(await engine.DeleteDocumentAsync("c", id)).IsEqualTo(0);
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }

    [Test]
    public async Task UpdateAndDeleteAsync_WhenExistingIsLargeDocument_ShouldSucceed()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_large_update_delete_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false, PageSize = 4096 });

            var big = new string('x', 6000);

            var colSync = engine.GetBsonCollection("large_sync");
            var idSync = colSync.Insert(new BsonDocument().Set("payload", big));
            var updatedSync = engine.UpdateDocumentInternal("large_sync", new BsonDocument().Set("_id", idSync).Set("payload", "small"));
            await Assert.That(updatedSync).IsEqualTo(1);

            var colAsync = engine.GetBsonCollection("large_async");
            var idAsync = colAsync.Insert(new BsonDocument().Set("payload", big));
            var updatedAsync = await engine.UpdateDocumentAsync("large_async", new BsonDocument().Set("_id", idAsync).Set("payload", "small"));
            await Assert.That(updatedAsync).IsEqualTo(1);

            var deletedAsync = await engine.DeleteDocumentAsync("large_async", idAsync);
            await Assert.That(deletedAsync).IsEqualTo(1);
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }

    [Test]
    public async Task DeleteDocumentAsync_WhenEntryIsLargeDocument_Should_Delete()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_large_delete_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false, PageSize = 4096 });

            var big = new string('x', 6000);
            var col = engine.GetBsonCollection("large_delete_async");
            var id = col.Insert(new BsonDocument().Set("payload", big));

            var deleted = await engine.DeleteDocumentAsync("large_delete_async", id);
            await Assert.That(deleted).IsEqualTo(1);
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }
}
