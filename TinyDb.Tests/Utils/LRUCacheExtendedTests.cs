using TinyDb.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Utils;

public class LRUCacheExtendedTests
{
    [Test]
    public async Task LRUCache_Advanced_Operations()
    {
        var cache = new LRUCache<int, string>(5);
        for (int i = 0; i < 5; i++) cache.Put(i, i.ToString());
        
        await Assert.That(cache.Capacity).IsEqualTo(5);
        await Assert.That(cache.ContainsKey(1)).IsTrue();
        await Assert.That(cache.ContainsKey(10)).IsFalse();
        
        var keys = cache.GetKeys().ToList();
        await Assert.That(keys.Count).IsEqualTo(5);
        
        var values = cache.GetValues().ToList();
        await Assert.That(values.Count).IsEqualTo(5);
        
        // Trim
        cache.Trim(2);
        await Assert.That(cache.Count).IsEqualTo(2);
        
        await Assert.That(cache.ToString()).Contains("LRUCache");
    }
}
