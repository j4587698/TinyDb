using TinyDb.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Utils;

public class LRUCacheTests
{
    [Test]
    public async Task LRUCache_Stats_Should_Be_Correct()
    {
        var cache = new LRUCache<int, string>(2);
        
        cache.Put(1, "A");
        cache.Put(2, "B");
        
        cache.TryGetValue(1, out _);
        cache.TryGetValue(3, out _);
        
        await Assert.That(cache.Hits).IsEqualTo(1);
        await Assert.That(cache.Misses).IsEqualTo(1);
        await Assert.That(cache.HitRatio).IsEqualTo(0.5);
    }

    [Test]
    public async Task LRUCache_GetOrAdd_Should_Work()
    {
        var cache = new LRUCache<int, string>(10);
        var val = cache.GetOrAdd(1, k => "V" + k);
        await Assert.That(val).IsEqualTo("V1");
        await Assert.That(cache.Count).IsEqualTo(1);
    }

    [Test]
    public async Task LRUCache_TryRemove_Should_Work()
    {
        var cache = new LRUCache<int, string>(10);
        cache.Put(1, "A");
        var removed = cache.TryRemove(1);
        await Assert.That(removed).IsTrue();
        await Assert.That(cache.Count).IsEqualTo(0);
    }

    [Test]
    public async Task LRUCache_Clear_Should_Work()
    {
        var cache = new LRUCache<int, string>(10);
        cache.Put(1, "A");
        cache.Put(2, "B");
        cache.Clear();
        await Assert.That(cache.Count).IsEqualTo(0);
    }
}