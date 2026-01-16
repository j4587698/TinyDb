using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineDeletePageTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public EngineDeletePageTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"eng_del_page_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Delete_Last_Document_On_Page_Should_Free_Page()
    {
        var id1 = new BsonInt32(1);
        var id2 = new BsonInt32(2);
        
        _engine.InsertDocument("col", new BsonDocument().Set("_id", id1).Set("Val", "A"));
        _engine.InsertDocument("col", new BsonDocument().Set("_id", id2).Set("Val", "B"));
        
        var statsBefore = _engine.GetStatistics();
        
        _engine.DeleteDocument("col", id1);
        _engine.DeleteDocument("col", id2);
        
        _engine.Flush();
        var statsAfter = _engine.GetStatistics();
        
        // Since they were likely on the same page, deleting both should free the page
        await Assert.That(statsAfter.UsedPages).IsLessThan(statsBefore.UsedPages);
    }
}
