using System;
using System.IO;
using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Collections;

public class DocumentCollectionInsertBatchEmptyCoverageTests
{
    [Test]
    public async Task InsertAsync_WithEmptyEnumerable_ShouldReturnZero()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_insertbatch_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });

            var collection = engine.GetBsonCollection("c");
            var inserted = await collection.InsertAsync(Array.Empty<BsonDocument>()).ConfigureAwait(false);
            await Assert.That(inserted).IsEqualTo(0);
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }
}
