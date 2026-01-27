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

    [Test]
    public async Task Rollback_Should_Handle_Insert_Update_Delete()
    {
        var col = _engine.GetCollection<TransactionRollbackItem>();
        col.Insert(new TransactionRollbackItem { Id = 1, Val = "A" });

        using (var trans = _engine.BeginTransaction())
        {
            // Update 1
            col.Update(new TransactionRollbackItem { Id = 1, Val = "B" });

            // Insert 2
            col.Insert(new TransactionRollbackItem { Id = 2, Val = "C" });

            // Insert 3 then Delete 3
            col.Insert(new TransactionRollbackItem { Id = 3, Val = "D" });
            col.Delete(new BsonInt32(3));

            trans.Rollback();
        }

        var all = col.FindAll().OrderBy(x => x.Id).ToList();
        await Assert.That(all.Count).IsEqualTo(1);
        await Assert.That(all[0].Val).IsEqualTo("A");
    }
}

[Entity("col")]
public class TransactionRollbackItem
{
    public int Id { get; set; }
    public string Val { get; set; } = "";
}
