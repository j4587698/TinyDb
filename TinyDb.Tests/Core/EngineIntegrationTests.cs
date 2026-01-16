using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineIntegrationTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public EngineIntegrationTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"eng_int_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Insert_Large_Document_Via_Engine_Should_Work()
    {
        var largeData = new byte[10000]; // > 4096
        new Random().NextBytes(largeData);
        var doc = new BsonDocument().Set("data", new BsonBinary(largeData));
        
        var id = _engine.InsertDocument("large_col", doc);
        await Assert.That(id).IsNotNull();
        
        var loaded = _engine.FindById("large_col", id);
        await Assert.That(loaded).IsNotNull();
        var loadedData = (BsonBinary)loaded!["data"];
        await Assert.That(loadedData.Bytes.Length).IsEqualTo(10000);
        await Assert.That(loadedData.Bytes.SequenceEqual(largeData)).IsTrue();
        
        // Update it
        var newData = new byte[12000];
        doc = doc.Set("data", new BsonBinary(newData)).Set("_id", id); // Add ID!
        _engine.UpdateDocument("large_col", doc);
        
        var updated = _engine.FindById("large_col", id);
        var updatedData = (BsonBinary)updated!["data"];
        await Assert.That(updatedData.Bytes.Length).IsEqualTo(12000);
        
        // Delete it
        var deleted = _engine.DeleteDocument("large_col", id);
        await Assert.That(deleted).IsEqualTo(1);
        await Assert.That(_engine.FindById("large_col", id)).IsNull();
    }

    [Test]
    public async Task UpdateDocument_Should_Handle_Regular_To_Large()
    {
        var doc = new BsonDocument().Set("data", new BsonBinary(new byte[10]));
        var id = _engine.InsertDocument("reg_to_large", doc);
        
        // Update to large
        doc = doc.Set("data", new BsonBinary(new byte[10000])).Set("_id", id);
        _engine.UpdateDocument("reg_to_large", doc);
        
        var loaded = _engine.FindById("reg_to_large", id);
        var loadedData = (BsonBinary)loaded!["data"];
        await Assert.That(loadedData.Bytes.Length).IsEqualTo(10000);
    }

    [Test]
    public async Task UpdateDocument_Should_Handle_Large_To_Regular()
    {
        var doc = new BsonDocument().Set("data", new BsonBinary(new byte[10000]));
        var id = _engine.InsertDocument("large_to_reg", doc);
        
        // Update to regular
        doc = doc.Set("data", new BsonBinary(new byte[10])).Set("_id", id);
        _engine.UpdateDocument("large_to_reg", doc);
        
        var loaded = _engine.FindById("large_to_reg", id);
        var loadedData = (BsonBinary)loaded!["data"];
        await Assert.That(loadedData.Bytes.Length).IsEqualTo(10);
    }
}
