using TinyDb.Core;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineUpdateTests : IDisposable
{
    private readonly string _testDbPath;

    public EngineUpdateTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"engine_update_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch {}
    }

    [Test]
    public async Task Update_Large_Document_To_Small_Should_Work()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        // Direct engine usage to bypass mapper issues with byte[]
        
        var largeData = new byte[10000]; 
        var doc = new TinyDb.Bson.BsonDocument()
            .Set("_id", 1)
            .Set("Data", new TinyDb.Bson.BsonBinary(largeData));
            
        engine.InsertDocument("large_docs", doc);

        // Update to small
        doc = doc.Set("Data", new TinyDb.Bson.BsonBinary(new byte[10]));
        engine.UpdateDocument("large_docs", doc);

        var loaded = engine.FindById("large_docs", new TinyDb.Bson.BsonInt32(1));
        await Assert.That(loaded).IsNotNull();
        var loadedData = (TinyDb.Bson.BsonBinary)loaded!["Data"];
        await Assert.That(loadedData.Bytes.Length).IsEqualTo(10);
    }

    [Test]
    public async Task Update_Small_Document_To_Large_Should_Work()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        
        var doc = new TinyDb.Bson.BsonDocument()
            .Set("_id", 1)
            .Set("Data", new TinyDb.Bson.BsonBinary(new byte[10]));
        engine.InsertDocument("large_docs", doc);

        // Update to large
        doc = doc.Set("Data", new TinyDb.Bson.BsonBinary(new byte[10000]));
        engine.UpdateDocument("large_docs", doc);

        var loaded = engine.FindById("large_docs", new TinyDb.Bson.BsonInt32(1));
        await Assert.That(loaded).IsNotNull();
        var loadedData = (TinyDb.Bson.BsonBinary)loaded!["Data"];
        await Assert.That(loadedData.Bytes.Length).IsEqualTo(10000);
    }

    [Test]
    public async Task Update_Large_To_Large_Should_Work()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        
        var doc = new TinyDb.Bson.BsonDocument()
            .Set("_id", 1)
            .Set("Data", new TinyDb.Bson.BsonBinary(new byte[10000]));
        engine.InsertDocument("large_docs", doc);

        // Update to different large
        doc = doc.Set("Data", new TinyDb.Bson.BsonBinary(new byte[12000]));
        engine.UpdateDocument("large_docs", doc);

        var loaded = engine.FindById("large_docs", new TinyDb.Bson.BsonInt32(1));
        await Assert.That(loaded).IsNotNull();
        var loadedData = (TinyDb.Bson.BsonBinary)loaded!["Data"];
        await Assert.That(loadedData.Bytes.Length).IsEqualTo(12000);
    }

    [Test]
    public async Task Update_PageOverflow_ShouldFallbackToDeleteAndInsert()
    {
        var options = new TinyDbOptions { PageSize = 4096 };
        using var engine = new TinyDbEngine(_testDbPath, options);

        const string collection = "sync_update_overflow";
        var dataCapacity = (int)options.PageSize - TinyDb.Storage.Page.DataStartOffset;
        var maxDocSize = (int)options.PageSize - 300;

        int payload1 = FindPayloadLengthForEntrySize(1, dataCapacity / 2 - 200, collection);
        int entry1 = GetEntrySize(1, payload1, collection);

        int payloadUpdated = FindPayloadLengthForEntrySize(1, dataCapacity - entry1 + 50, collection);
        int entryUpdated = GetEntrySize(1, payloadUpdated, collection);

        await Assert.That(entryUpdated - 4).IsLessThanOrEqualTo(maxDocSize);
        await Assert.That(entry1 + entryUpdated).IsGreaterThan(dataCapacity);

        engine.InsertDocument(collection, CreateSizedDoc(1, payload1, collection));
        engine.InsertDocument(collection, CreateSizedDoc(2, payload1, collection));

        var updated = CreateSizedDoc(1, payloadUpdated, collection);
        var count = engine.UpdateDocumentInternal(collection, updated);

        await Assert.That(count).IsEqualTo(1);
        var loaded = engine.FindById(collection, new TinyDb.Bson.BsonInt32(1));
        await Assert.That(loaded).IsNotNull();
        await Assert.That(loaded!["payload"].ToString()!.Length).IsEqualTo(payloadUpdated);

        static TinyDb.Bson.BsonDocument CreateSizedDoc(int id, int payloadLength, string col)
        {
            var payload = new string('x', payloadLength);
            return new TinyDb.Bson.BsonDocument().Set("_id", id).Set("_collection", col).Set("payload", payload);
        }

        static int GetEntrySize(int id, int payloadLength, string col)
        {
            var bytes = TinyDb.Serialization.BsonSerializer.SerializeDocument(CreateSizedDoc(id, payloadLength, col));
            return bytes.Length + 4;
        }

        static int FindPayloadLengthForEntrySize(int id, int targetEntrySize, string col)
        {
            var payloadLength = Math.Max(0, targetEntrySize - 128);
            while (true)
            {
                var entrySize = GetEntrySize(id, payloadLength, col);
                if (entrySize >= targetEntrySize)
                {
                    return payloadLength;
                }
                payloadLength += Math.Max(1, targetEntrySize - entrySize);
            }
        }
    }
}
