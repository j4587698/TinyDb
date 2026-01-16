using TinyDb.Index;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class BTreeStressTests
{
    [Test]
    public async Task Stress_Test_Insert_Delete_Random()
    {
        var index = new BTreeIndex("stress", new[] { "id" }, false, 10);
        var rnd = new Random(12345);
        var keys = new List<int>();
        
        // Insert 2000 items
        for (int i = 0; i < 2000; i++)
        {
            var k = rnd.Next(10000);
            if (!keys.Contains(k))
            {
                keys.Add(k);
                index.Insert(new IndexKey(new BsonInt32(k)), new BsonString($"doc_{k}"));
            }
        }
        
        await Assert.That(index.Validate()).IsTrue();
        
        // Delete 1000 items randomly
        for (int i = 0; i < 1000; i++)
        {
            if (keys.Count == 0) break;
            var idx = rnd.Next(keys.Count);
            var k = keys[idx];
            keys.RemoveAt(idx);
            
            var deleted = index.Delete(new IndexKey(new BsonInt32(k)), new BsonString($"doc_{k}"));
            await Assert.That(deleted).IsTrue();
        }
        
        await Assert.That(index.Validate()).IsTrue();
        await Assert.That(index.EntryCount).IsEqualTo(keys.Count);
    }
}
