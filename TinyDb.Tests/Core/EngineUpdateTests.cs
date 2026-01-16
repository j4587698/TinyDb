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
}
