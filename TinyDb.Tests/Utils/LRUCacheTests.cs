using System;
using TinyDb.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Utils;

public class LRUCacheTests
{
    [Test]
    public async Task LRUCache_Constructor_WithNonPositiveCapacity_ShouldThrow()
    {
        await Assert.That(() => new LRUCache<int, string>(0)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new LRUCache<int, string>(-1)).Throws<ArgumentOutOfRangeException>();
    }

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
    public async Task LRUCache_GetOrAdd_WhenKeyExists_ShouldNotInvokeFactory()
    {
        var cache = new LRUCache<int, string>(10);
        var called = 0;

        var v1 = cache.GetOrAdd(1, k => { called++; return "V" + k; });
        var v2 = cache.GetOrAdd(1, k => { called++; return "X" + k; });

        await Assert.That(v1).IsEqualTo("V1");
        await Assert.That(v2).IsEqualTo("V1");
        await Assert.That(called).IsEqualTo(1);
    }

    [Test]
    public async Task LRUCache_Put_WhenOverCapacity_ShouldEvictLeastRecentlyUsed()
    {
        var cache = new LRUCache<int, string>(1);
        cache.Put(1, "A");
        cache.Put(2, "B");

        await Assert.That(cache.Count).IsEqualTo(1);
        await Assert.That(cache.ContainsKey(1)).IsFalse();
        await Assert.That(cache.ContainsKey(2)).IsTrue();
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
    public async Task LRUCache_TryRemove_WhenMissing_ShouldReturnFalse()
    {
        var cache = new LRUCache<int, string>(10);
        var removed = cache.TryRemove(1);
        await Assert.That(removed).IsFalse();
    }

    [Test]
    public async Task LRUCache_TryGetLeastRecentlyUsed_WhenEmpty_ShouldReturnFalse()
    {
        var cache = new LRUCache<int, string>(10);
        var ok = cache.TryGetLeastRecentlyUsed(out var key, out var value);

        await Assert.That(ok).IsFalse();
        await Assert.That(key).IsEqualTo(0);
        await Assert.That(value).IsNull();
    }

    [Test]
    public async Task LRUCache_TryGetLeastRecentlyUsed_WhenNotEmpty_ShouldReturnLast()
    {
        var cache = new LRUCache<int, string>(10);
        cache.Put(1, "A");
        cache.Put(2, "B");

        // Touch key 2 so key 1 becomes LRU
        cache.Touch(2);

        var ok = cache.TryGetLeastRecentlyUsed(out var key, out var value);
        await Assert.That(ok).IsTrue();
        await Assert.That(key).IsEqualTo(1);
        await Assert.That(value).IsEqualTo("A");
    }

    [Test]
    public async Task LRUCache_Trim_WithNonPositiveCapacity_ShouldThrow()
    {
        var cache = new LRUCache<int, string>(10);
        await Assert.That(() => cache.Trim(0)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => cache.Trim(-1)).Throws<ArgumentOutOfRangeException>();
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
