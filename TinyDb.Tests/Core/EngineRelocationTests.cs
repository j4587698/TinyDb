using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineRelocationTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public EngineRelocationTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"eng_reloc_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Update_Document_Grow_Should_Work()
    {
        var id = new BsonInt32(1);
        _engine.InsertDocument("col", new BsonDocument().Set("_id", id).Set("Data", "small"));
        
        // Update to something larger but still fits in one page (default 8192)
        var mediumData = new string('x', 3000);
        _engine.UpdateDocument("col", new BsonDocument().Set("_id", id).Set("Data", mediumData));
        
        var loaded = _engine.FindById("col", id);
        await Assert.That(((BsonString)loaded!["Data"]).Value.Length).IsEqualTo(3000);
        
        // Update to something that might trigger large document storage
        var largeData = new string('y', 10000);
        _engine.UpdateDocument("col", new BsonDocument().Set("_id", id).Set("Data", largeData));
        
        var loaded2 = _engine.FindById("col", id);
        await Assert.That(((BsonString)loaded2!["Data"]).Value.Length).IsEqualTo(10000);
    }
}
