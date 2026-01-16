using TinyDb.Index;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class BTreeStructureAdvancedTests
{
    [Test]
    public async Task BTree_Deep_Tree_Operations()
    {
        // Use very small max keys to force deep tree and many splits/merges
        var index = new BTreeIndex("idx", new[] { "Id" }, true, 4);
        for (int i = 0; i < 500; i++)
        {
            index.Insert(new IndexKey(i), new BsonInt32(i));
        }
        
        await Assert.That(index.EntryCount).IsEqualTo(500);
        
        // Delete in non-sequential order to trigger various merge/rebalance scenarios
        var random = new Random(42);
        var keysToDelete = Enumerable.Range(0, 500).OrderBy(x => random.Next()).ToList();
        
        foreach (var key in keysToDelete)
        {
            var deleted = index.Delete(new IndexKey(key), new BsonInt32(key));
            await Assert.That(deleted).IsTrue();
        }
        
        await Assert.That(index.EntryCount).IsEqualTo(0);
    }

    [Test]
    public async Task IndexKey_Composite_Comparison_Should_Work()
    {
        var k1 = new IndexKey(1, 1);
        var k2 = new IndexKey(1, 2);
        var k3 = new IndexKey(2, 1);
        
        await Assert.That(k1.CompareTo(k2)).IsLessThan(0);
        await Assert.That(k2.CompareTo(k3)).IsLessThan(0);
        await Assert.That(k1.Equals(new IndexKey(1, 1))).IsTrue();
    }

    [Test]
    public async Task BTree_Composite_Key_Operations()
    {
        var index = new BTreeIndex("idx", new[] { "A", "B" }, true, 10);
        
        index.Insert(new IndexKey(1, 1), new BsonInt32(11));
        index.Insert(new IndexKey(1, 2), new BsonInt32(12));
        index.Insert(new IndexKey(2, 1), new BsonInt32(21));
        
        await Assert.That(index.EntryCount).IsEqualTo(3);
        
        var found = index.Find(new IndexKey(1, 2)).ToList();
        await Assert.That(found.Count).IsEqualTo(1);
        await Assert.That(found[0]).IsEqualTo(new BsonInt32(12));
    }
}
