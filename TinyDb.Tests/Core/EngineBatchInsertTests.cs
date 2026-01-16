using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineBatchInsertTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public EngineBatchInsertTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"eng_batch_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task InsertDocuments_EmptyArray_Should_Return_Zero()
    {
        var result = _engine.InsertDocuments("col", Array.Empty<BsonDocument>());
        await Assert.That(result).IsEqualTo(0);
    }

    [Test]
    public async Task InsertDocuments_With_Null_Docs_Should_Skip_Them()
    {
        var docs = new BsonDocument[] { null!, new BsonDocument().Set("a", 1) };
        var result = _engine.InsertDocuments("col", docs);
        await Assert.That(result).IsEqualTo(1);
    }
}
