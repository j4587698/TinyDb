using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TinyDb.Attributes;

namespace TinyDb.Tests.Core;

public class TransactionManagerRollbackTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public TransactionManagerRollbackTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"trans_rb_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Entity("col")]
    public class Item
    {
        public int Id { get; set; }
        public string Val { get; set; } = "";
    }

    [Test]
    public async Task Rollback_Should_Handle_Insert_Update_Delete()
    {
        var col = _engine.GetCollection<Item>();
        col.Insert(new Item { Id = 1, Val = "A" });

        using (var trans = _engine.BeginTransaction())
        {
            // Update 1
            col.Update(new Item { Id = 1, Val = "B" });
            
            // Insert 2
            col.Insert(new Item { Id = 2, Val = "C" });
            
            // Insert 3 then Delete 3
            col.Insert(new Item { Id = 3, Val = "D" });
            col.Delete(new BsonInt32(3));
            
            trans.Rollback();
        }

        var all = col.FindAll().OrderBy(x => x.Id).ToList();
        await Assert.That(all.Count).IsEqualTo(1);
        await Assert.That(all[0].Val).IsEqualTo("A");
    }
}
