using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using TinyDb.Bson;
using TinyDb.Collections;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Collections;

public class DocumentCollectionInsertBatchEmptyCoverageTests
{
    [Test]
    public async Task InsertBatchAsync_WhenDocumentsEmpty_ShouldReturnZero()
    {
        var dir = Path.Combine(Path.GetTempPath(), $"tinydb_insertbatch_{Guid.NewGuid():N}");
        var dbPath = Path.Combine(dir, "db.db");

        try
        {
            Directory.CreateDirectory(dir);
            using var engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });

            var collection = (DocumentCollection<BsonDocument>)engine.GetCollection<BsonDocument>("c");
            var method = typeof(DocumentCollection<BsonDocument>).GetMethod("InsertBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance);
            await Assert.That(method).IsNotNull();

            var task = (Task<int>)method!.Invoke(
                collection,
                new object[] { new List<BsonDocument>(), new List<BsonDocument>(), CancellationToken.None })!;

            var inserted = await task.ConfigureAwait(false);
            await Assert.That(inserted).IsEqualTo(0);
        }
        finally
        {
            try { if (Directory.Exists(dir)) Directory.Delete(dir, true); } catch { }
        }
    }
}

