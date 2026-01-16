using TinyDb.Index;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class BTreeStructureFullTests
{
    [Test]
    public async Task BTree_Large_Scale_Delete_Should_Maintain_Integrity()
    {
        var index = new BTreeIndex("idx", new[] { "Id" }, true, 10);
        var documentIds = new List<BsonValue>();
        
        // Insert 200 items to cause many splits
        for (int i = 0; i < 200; i++)
        {
            var id = new BsonInt32(i);
            documentIds.Add(id);
            index.Insert(new IndexKey(i), id);
        }
        
        await Assert.That(index.EntryCount).IsEqualTo(200);
        
        // Delete in reverse order to trigger merges and redistributions
        for (int i = 199; i >= 0; i--)
        {
            var deleted = index.Delete(new IndexKey(i), documentIds[i]);
            await Assert.That(deleted).IsTrue();
        }
        
        await Assert.That(index.EntryCount).IsEqualTo(0);
    }

    [Test]
    public async Task BTree_Duplicate_Keys_NonUnique_Index_Should_Work()
    {
        var index = new BTreeIndex("idx", new[] { "Val" }, false, 10);
        var key = new IndexKey(1);
        
        for (int i = 0; i < 20; i++)
        {
            index.Insert(key, new BsonInt32(i));
        }
        
        await Assert.That(index.EntryCount).IsEqualTo(20);
        
        var results = index.Find(key).ToList();
        await Assert.That(results.Count).IsEqualTo(20);
        
        // Delete one
        index.Delete(key, new BsonInt32(5));
        await Assert.That(index.EntryCount).IsEqualTo(19);
    }
}
