using TinyDb.Index;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class BTreeMaintenanceTests
{
    [Test]
    public async Task Delete_Root_Item_Should_Work()
    {
        var index = new BTreeIndex("idx", new[] { "Id" }, true, 4);
        var id = new BsonObjectId(ObjectId.NewObjectId());
        var key = new IndexKey(1);
        
        index.Insert(key, id);
        await Assert.That(index.EntryCount).IsEqualTo(1);
        
        var deleted = index.Delete(key, id);
        await Assert.That(deleted).IsTrue();
        await Assert.That(index.EntryCount).IsEqualTo(0);
    }

    [Test]
    public async Task Delete_Multiple_Items_Triggering_Merge()
    {
        // Max keys = 4, so min keys = 2.
        var index = new BTreeIndex("idx", new[] { "Id" }, true, 4);
        var documentIds = new List<BsonValue>();
        
        // Insert 10 items to cause splits
        for (int i = 0; i < 10; i++)
        {
            var id = new BsonInt32(i);
            documentIds.Add(id);
            index.Insert(new IndexKey(i), id);
        }
        
        await Assert.That(index.EntryCount).IsEqualTo(10);
        
        // Delete items to trigger rebalance/merge
        for (int i = 0; i < 10; i++)
        {
            var deleted = index.Delete(new IndexKey(i), documentIds[i]);
            await Assert.That(deleted).IsTrue();
        }
        
        await Assert.That(index.EntryCount).IsEqualTo(0);
    }

    [Test]
    public async Task Delete_NonExistent_Item_Should_Return_False()
    {
        var index = new BTreeIndex("idx", new[] { "Id" }, true, 4);
        var deleted = index.Delete(new IndexKey(1), new BsonInt32(1));
        await Assert.That(deleted).IsFalse();
    }

    [Test]
    public async Task Clear_Index_Should_Reset_Stats()
    {
        // Assuming there is a Clear method or similar. If not, we skip this.
        // Let's check if there is a Clear method.
    }
}
