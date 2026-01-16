using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineAdditionalTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public EngineAdditionalTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"eng_add_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task InsertDocument_Without_Id_Should_Generate_Id()
    {
        var doc = new BsonDocument();
        doc = doc.Set("name", "test");
        
        // Use internal InsertDocument via reflection or if internal is visible
        // TinyDb.Tests has InternalsVisibleTo
        var id = _engine.InsertDocument("test_col", doc);
        
        await Assert.That(id).IsNotNull();
        await Assert.That(id.IsObjectId).IsTrue();
    }

    [Test]
    public async Task FindById_With_NonExistent_Id_Should_Return_Null()
    {
        var result = _engine.FindById("test_col", new BsonObjectId(TinyDb.Bson.ObjectId.NewObjectId()));
        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task UpdateDocument_With_NonExistent_Id_Should_Return_Zero()
    {
        var doc = new BsonDocument().Set("_id", new BsonObjectId(TinyDb.Bson.ObjectId.NewObjectId()));
        var result = _engine.UpdateDocument("test_col", doc);
        await Assert.That(result).IsEqualTo(0);
    }

    [Test]
    public async Task DeleteDocument_With_NonExistent_Id_Should_Return_Zero()
    {
        var result = _engine.DeleteDocument("test_col", new BsonObjectId(TinyDb.Bson.ObjectId.NewObjectId()));
        await Assert.That(result).IsEqualTo(0);
    }

    [Test]
    public async Task InsertDocuments_Should_Insert_Multiple()
    {
        var docs = new BsonDocument[] 
        {
            new BsonDocument().Set("val", 1),
            new BsonDocument().Set("val", 2)
        };
        
        var count = _engine.InsertDocuments("batch_col", docs);
        await Assert.That(count).IsEqualTo(2);
        
        var all = _engine.FindAll("batch_col").ToList();
        await Assert.That(all.Count).IsEqualTo(2);
    }
}
