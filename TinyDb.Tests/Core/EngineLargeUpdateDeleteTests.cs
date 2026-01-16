using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineLargeUpdateDeleteTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public EngineLargeUpdateDeleteTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"eng_large_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Update_Large_Document_With_New_Large_Data_Should_Work()
    {
        var id = new BsonInt32(1);
        var initialData = new byte[10000];
        new Random().NextBytes(initialData);
        var doc = new BsonDocument().Set("_id", id).Set("Data", new BsonBinary(initialData));
        
        _engine.InsertDocument("col", doc);
        
        var newData = new byte[15000];
        new Random().NextBytes(newData);
        doc = doc.Set("Data", new BsonBinary(newData));
        
        var result = _engine.UpdateDocument("col", doc);
        await Assert.That(result).IsEqualTo(1);
        
        var loaded = _engine.FindById("col", id);
        await Assert.That(((BsonBinary)loaded!["Data"]).Bytes.Length).IsEqualTo(15000);
    }

    [Test]
    public async Task Delete_Large_Document_Should_Work()
    {
        var id = new BsonInt32(1);
        var data = new byte[10000];
        var doc = new BsonDocument().Set("_id", id).Set("Data", new BsonBinary(data));
        
        _engine.InsertDocument("col", doc);
        
        var result = _engine.DeleteDocument("col", id);
        await Assert.That(result).IsEqualTo(1);
        
        var loaded = _engine.FindById("col", id);
        await Assert.That(loaded).IsNull();
    }
}
