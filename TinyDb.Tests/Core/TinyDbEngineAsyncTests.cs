using System.Threading;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class TinyDbEngineAsyncTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;

    public TinyDbEngineAsyncTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"engine_async_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine?.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
        var walPath = _dbPath + "-wal";
        if (File.Exists(walPath)) File.Delete(walPath);
        var shmPath = _dbPath + "-shm";
        if (File.Exists(shmPath)) File.Delete(shmPath);
    }

    [Test]
    public async Task InsertDocumentsAsync_Empty_ReturnsZero()
    {
        var count = await _engine.InsertDocumentsAsync("async_insert", Array.Empty<BsonDocument>());

        await Assert.That(count).IsEqualTo(0);
    }

    [Test]
    public async Task InsertDocumentsAsync_WithNullEntries_ShouldInsertValid()
    {
        var docs = new BsonDocument[]
        {
            new BsonDocument().Set("_id", 1).Set("name", "a"),
            null!,
            new BsonDocument().Set("_id", 2).Set("name", "b")
        };

        var count = await _engine.InsertDocumentsAsync("async_insert", docs);

        await Assert.That(count).IsEqualTo(2);

        var all = _engine.FindAll("async_insert").ToList();
        await Assert.That(all.Count).IsEqualTo(2);
    }

    [Test]
    public async Task InsertDocumentsAsync_Canceled_ShouldThrow()
    {
        var docs = new[]
        {
            new BsonDocument().Set("_id", 1).Set("name", "a")
        };

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await _engine.InsertDocumentsAsync("async_insert", docs, cts.Token);
        });
    }

    [Test]
    public async Task UpdateDocumentAsync_MissingId_ReturnsZero()
    {
        var doc = new BsonDocument().Set("name", "no_id");

        var count = await _engine.UpdateDocumentAsync("async_update", doc);

        await Assert.That(count).IsEqualTo(0);
    }

    [Test]
    public async Task UpdateDocumentAsync_NotFound_ReturnsZero()
    {
        var doc = new BsonDocument().Set("_id", 99).Set("name", "missing");

        var count = await _engine.UpdateDocumentAsync("async_update", doc);

        await Assert.That(count).IsEqualTo(0);
    }

    [Test]
    public async Task UpdateDocumentAsync_ShouldUpdateAndNormalizeCollection()
    {
        var original = new BsonDocument().Set("_id", 1).Set("name", "before");
        _engine.InsertDocument("async_update", original);

        var updated = new BsonDocument()
            .Set("_id", 1)
            .Set("_collection", "wrong")
            .Set("name", "after");

        var count = await _engine.UpdateDocumentAsync("async_update", updated);

        await Assert.That(count).IsEqualTo(1);

        var loaded = _engine.FindById("async_update", new BsonInt32(1));
        await Assert.That(loaded).IsNotNull();
        await Assert.That(loaded!["name"].ToString()).IsEqualTo("after");
        await Assert.That(loaded["_collection"].ToString()).IsEqualTo("async_update");
    }

    [Test]
    public async Task InsertDocumentsAsync_LargeDocument_ShouldPersistAndResolve()
    {
        var payload = CreateLargePayload(20000);
        var docs = new[]
        {
            new BsonDocument().Set("_id", 1).Set("payload", payload)
        };

        var count = await _engine.InsertDocumentsAsync("async_large", docs);

        await Assert.That(count).IsEqualTo(1);

        var loaded = _engine.FindById("async_large", new BsonInt32(1));
        await Assert.That(loaded).IsNotNull();
        await Assert.That(((BsonString)loaded!["payload"]).Value).IsEqualTo(payload);

        var raw = GetRawDocument("async_large", new BsonInt32(1));
        await Assert.That(raw).IsNotNull();
        await Assert.That(raw!.TryGetValue("_isLargeDocument", out var isLarge)).IsTrue();
        await Assert.That(isLarge!.ToBoolean(null)).IsTrue();
    }

    [Test]
    public async Task UpdateDocumentAsync_LargeDocument_ShouldReplaceIndexDocument()
    {
        var payload1 = CreateLargePayload(20000);
        var payload2 = CreateLargePayload(24000);
        var docs = new[]
        {
            new BsonDocument().Set("_id", 1).Set("payload", payload1)
        };

        await _engine.InsertDocumentsAsync("async_large_update", docs);

        var beforeRaw = GetRawDocument("async_large_update", new BsonInt32(1));
        await Assert.That(beforeRaw).IsNotNull();
        var beforeSize = beforeRaw!["_largeDocumentSize"].ToInt64(null);

        var updated = new BsonDocument().Set("_id", 1).Set("payload", payload2);
        var count = await _engine.UpdateDocumentAsync("async_large_update", updated);

        await Assert.That(count).IsEqualTo(1);

        var loaded = _engine.FindById("async_large_update", new BsonInt32(1));
        await Assert.That(loaded).IsNotNull();
        await Assert.That(((BsonString)loaded!["payload"]).Value).IsEqualTo(payload2);

        var afterRaw = GetRawDocument("async_large_update", new BsonInt32(1));
        await Assert.That(afterRaw).IsNotNull();
        await Assert.That(afterRaw!.TryGetValue("_isLargeDocument", out var isLarge)).IsTrue();
        await Assert.That(isLarge!.ToBoolean(null)).IsTrue();
        var afterSize = afterRaw["_largeDocumentSize"].ToInt64(null);
        await Assert.That(afterSize).IsNotEqualTo(beforeSize);
    }

    [Test]
    public async Task UpdateDocumentAsync_PageOverflow_ShouldReinsert()
    {
        var path = Path.Combine(Path.GetTempPath(), $"engine_async_overflow_{Guid.NewGuid():N}.db");
        var options = new TinyDbOptions { PageSize = 4096 };
        var engine = new TinyDbEngine(path, options);

        try
        {
            const string collection = "async_update_overflow";
            var dataCapacity = (int)options.PageSize - Page.DataStartOffset;
            var maxDocSize = (int)options.PageSize - 300;

            int payload1 = FindPayloadLengthForEntrySize(1, dataCapacity / 2 - 200);
            int entry1 = GetEntrySize(1, payload1);

            int payloadUpdated = FindPayloadLengthForEntrySize(1, dataCapacity - entry1 + 50);
            int entryUpdated = GetEntrySize(1, payloadUpdated);

            await Assert.That(entryUpdated - 4).IsLessThanOrEqualTo(maxDocSize);
            await Assert.That(entry1 + entryUpdated).IsGreaterThan(dataCapacity);

            engine.InsertDocument(collection, CreateSizedDoc(1, payload1));
            engine.InsertDocument(collection, CreateSizedDoc(2, payload1));

            var updated = CreateSizedDoc(1, payloadUpdated).Set("_collection", collection);
            var count = await engine.UpdateDocumentAsync(collection, updated);

            await Assert.That(count).IsEqualTo(1);
            var loaded = engine.FindById(collection, new BsonInt32(1));
            await Assert.That(loaded).IsNotNull();
            await Assert.That(loaded!["payload"].ToString()!.Length).IsEqualTo(payloadUpdated);
        }
        finally
        {
            engine.Dispose();
            if (File.Exists(path)) File.Delete(path);
            var walPath = path + "-wal";
            if (File.Exists(walPath)) File.Delete(walPath);
            var shmPath = path + "-shm";
            if (File.Exists(shmPath)) File.Delete(shmPath);
        }

        static BsonDocument CreateSizedDoc(int id, int payloadLength)
        {
            var payload = new string('x', payloadLength);
            return new BsonDocument().Set("_id", id).Set("payload", payload);
        }

        static int GetEntrySize(int id, int payloadLength)
        {
            var bytes = BsonSerializer.SerializeDocument(CreateSizedDoc(id, payloadLength));
            return bytes.Length + 4;
        }

        static int FindPayloadLengthForEntrySize(int id, int targetEntrySize)
        {
            var payloadLength = Math.Max(0, targetEntrySize - 128);
            while (true)
            {
                var entrySize = GetEntrySize(id, payloadLength);
                if (entrySize >= targetEntrySize)
                {
                    return payloadLength;
                }
                payloadLength += Math.Max(1, targetEntrySize - entrySize);
            }
        }
    }

    private static string CreateLargePayload(int size)
    {
        return new string('x', size);
    }

    private BsonDocument? GetRawDocument(string collection, BsonValue id)
    {
        foreach (var slice in _engine.FindAllRaw(collection))
        {
            BsonDocument doc;
            try
            {
                doc = BsonSerializer.DeserializeDocument(slice);
            }
            catch
            {
                continue;
            }

            if (doc.TryGetValue("_id", out var docId) && docId.Equals(id))
            {
                return doc;
            }
        }

        return null;
    }
}
