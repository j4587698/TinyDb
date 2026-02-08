using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class BTreeDeleteStressTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public BTreeDeleteStressTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"btree_del_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Delete_Many_Items_Should_Trigger_Merge_And_Redistribute()
    {
        // Use BsonDocument collection to avoid mapping issues with anonymous types
        // Anonymous types as generics are tricky because T is object
        var colName = "col";
        
        int count = 500;
        for (int i = 0; i < count; i++)
        {
            var doc = new BsonDocument()
                .Set("_id", i)
                .Set("Data", new string('x', 100));
            _engine.InsertDocument(colName, doc);
        }

        await Assert.That(_engine.FindAll(colName).Count()).IsEqualTo(count);

        // Delete half (evens)
        for (int i = 0; i < count; i += 2)
        {
            var deleted = _engine.DeleteDocument(colName, new BsonInt32(i));
            // Ensure deletion actually happened
            await Assert.That(deleted).IsEqualTo(1);
        }

        // Verify deletion by checking existence of deleted items
        for (int i = 0; i < count; i += 2)
        {
            var doc = _engine.FindById(colName, new BsonInt32(i));
            await Assert.That(doc == null).IsTrue();
        }

        // Verify index consistency
        for (int i = 1; i < count; i += 2)
        {
            var doc = _engine.FindById(colName, new BsonInt32(i));
            await Assert.That(doc != null).IsTrue();
        }
    }
}
