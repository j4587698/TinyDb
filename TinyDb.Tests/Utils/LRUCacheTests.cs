using System.Diagnostics.CodeAnalysis;
using TinyDb.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Utils;

public class LRUCacheTests
{
    [Test]
    public async Task Constructor_Should_Initialize_With_Capacity()
    {
        // Act
        var cache = new LRUCache<int, string>(5);

        // Assert
        await Assert.That(cache.Capacity).IsEqualTo(5);
        await Assert.That(cache.Count).IsEqualTo(0);
        await Assert.That(cache.Hits).IsEqualTo(0);
        await Assert.That(cache.Misses).IsEqualTo(0);
        await Assert.That(cache.HitRatio).IsEqualTo(0.0);
    }

    [Test]
    public async Task Put_Should_Add_Item_To_Cache()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);

        // Act
        cache.Put(1, "one");

        // Assert
        await Assert.That(cache.Count).IsEqualTo(1);
        var found = cache.TryGetValue(1, out var value);
        await Assert.That(found).IsTrue();
        await Assert.That(value).IsEqualTo("one");
    }

    [Test]
    public async Task TryGetValue_Should_Return_Value_For_Existing_Key()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);
        cache.Put(1, "one");
        cache.Put(2, "two");

        // Act
        var found = cache.TryGetValue(1, out var result);

        // Assert
        await Assert.That(found).IsTrue();
        await Assert.That(result).IsEqualTo("one");
        await Assert.That(cache.Hits).IsEqualTo(1);
        await Assert.That(cache.Misses).IsEqualTo(0);
    }

    [Test]
    public async Task TryGetValue_Should_Return_False_For_Non_Existing_Key()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);
        cache.Put(1, "one");

        // Act
        var found = cache.TryGetValue(99, out var result);

        // Assert
        await Assert.That(found).IsFalse();
        await Assert.That(result).IsEqualTo(default(string));
        await Assert.That(cache.Hits).IsEqualTo(0);
        await Assert.That(cache.Misses).IsEqualTo(1);
    }

    [Test]
    public async Task Put_Should_Evict_Least_Recently_Used_When_Capacity_Exceeded()
    {
        // Arrange
        var cache = new LRUCache<int, string>(2);
        cache.Put(1, "one");
        cache.Put(2, "two");

        // Act - Add third item, should evict the first
        cache.Put(3, "three");

        // Assert
        await Assert.That(cache.Count).IsEqualTo(2);
        var found1 = cache.TryGetValue(1, out var val1);
        await Assert.That(found1).IsFalse(); // Should be evicted
        var found2 = cache.TryGetValue(2, out var val2);
        await Assert.That(found2).IsTrue();
        await Assert.That(val2).IsEqualTo("two"); // Should still exist
        var found3 = cache.TryGetValue(3, out var val3);
        await Assert.That(found3).IsTrue();
        await Assert.That(val3).IsEqualTo("three"); // Should exist
    }

    [Test]
    public async Task TryGetValue_Should_Update_Recency_Order()
    {
        // Arrange
        var cache = new LRUCache<int, string>(2);
        cache.Put(1, "one");
        cache.Put(2, "two");

        // Act - Access first item to make it most recently used
        cache.TryGetValue(1, out _);
        cache.Put(3, "three"); // Should evict item 2, not item 1

        // Assert
        var found1 = cache.TryGetValue(1, out var val1);
        await Assert.That(found1).IsTrue();
        await Assert.That(val1).IsEqualTo("one"); // Should still exist
        var found2 = cache.TryGetValue(2, out var val2);
        await Assert.That(found2).IsFalse(); // Should be evicted
        var found3 = cache.TryGetValue(3, out var val3);
        await Assert.That(found3).IsTrue();
        await Assert.That(val3).IsEqualTo("three"); // Should exist
    }

    [Test]
    public async Task Put_Should_Update_Value_For_Existing_Key()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);
        cache.Put(1, "one");

        // Act
        cache.Put(1, "updated one");

        // Assert
        await Assert.That(cache.Count).IsEqualTo(1);
        var found = cache.TryGetValue(1, out var value);
        await Assert.That(found).IsTrue();
        await Assert.That(value).IsEqualTo("updated one");
    }

    [Test]
    public async Task ContainsKey_Should_Return_True_For_Existing_Key()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);
        cache.Put(1, "one");

        // Act
        var contains = cache.ContainsKey(1);

        // Assert
        await Assert.That(contains).IsTrue();
    }

    [Test]
    public async Task ContainsKey_Should_Return_False_For_Non_Existing_Key()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);
        cache.Put(1, "one");

        // Act
        var contains = cache.ContainsKey(99);

        // Assert
        await Assert.That(contains).IsFalse();
    }

    [Test]
    public async Task TryRemove_Should_Remove_Item_And_Return_True()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);
        cache.Put(1, "one");
        cache.Put(2, "two");

        // Act
        var removed = cache.TryRemove(1);

        // Assert
        await Assert.That(removed).IsTrue();
        await Assert.That(cache.Count).IsEqualTo(1);
        await Assert.That(cache.ContainsKey(1)).IsFalse();
        var found = cache.TryGetValue(1, out var val);
        await Assert.That(found).IsFalse();
    }

    [Test]
    public async Task TryRemove_Should_Return_False_For_Non_Existing_Key()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);
        cache.Put(1, "one");

        // Act
        var removed = cache.TryRemove(99);

        // Assert
        await Assert.That(removed).IsFalse();
        await Assert.That(cache.Count).IsEqualTo(1);
    }

    [Test]
    public async Task HitRatio_Should_Be_Calculated_Correctly()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);
        cache.Put(1, "one");
        cache.Put(2, "two");

        // Act - Perform some operations
        cache.TryGetValue(1, out _); // Hit
        cache.TryGetValue(2, out _); // Hit
        cache.TryGetValue(99, out _); // Miss
        cache.TryGetValue(1, out _); // Hit
        cache.TryGetValue(98, out _); // Miss

        // Assert
        await Assert.That(cache.Hits).IsEqualTo(3);
        await Assert.That(cache.Misses).IsEqualTo(2);
        await Assert.That(cache.HitRatio).IsEqualTo(0.6); // 3/5 = 0.6
    }

    [Test]
    public async Task HitRatio_Should_Be_Zero_When_No_Accesses()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);

        // Act & Assert
        await Assert.That(cache.HitRatio).IsEqualTo(0.0);
    }

    [Test]
    public async Task GetOrAdd_Should_Add_Item_When_Not_Exists()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);

        // Act
        var value = cache.GetOrAdd(1, key => $"value-{key}");

        // Assert
        await Assert.That(value).IsEqualTo("value-1");
        await Assert.That(cache.Count).IsEqualTo(1);
    }

    [Test]
    public async Task GetOrAdd_Should_Return_Existing_Value_When_Exists()
    {
        // Arrange
        var cache = new LRUCache<int, string>(3);
        cache.Put(1, "existing");

        // Act
        var value = cache.GetOrAdd(1, key => $"value-{key}");

        // Assert
        await Assert.That(value).IsEqualTo("existing");
        await Assert.That(cache.Count).IsEqualTo(1);
    }

    [Test]
    public async Task Concurrent_Operations_Should_Work_Correctly()
    {
        // Arrange
        var cache = new LRUCache<int, string>(100);
        var tasks = new List<Task>();

        // Act - Simulate concurrent operations
        for (int i = 0; i < 5; i++)
        {
            var threadId = i;
            tasks.Add(Task.Run(() =>
            {
                for (int j = 0; j < 100; j++)
                {
                    var key = threadId * 100 + j;
                    cache.Put(key, $"value-{key}");
                    cache.TryGetValue(key, out _);
                    cache.TryRemove(key);
                }
            }));
        }

        await Task.WhenAll(tasks);

        // Assert
        await Assert.That(cache.Count).IsLessThanOrEqualTo(100);
        await Assert.That(cache.Hits + cache.Misses).IsGreaterThan(0);
    }
}