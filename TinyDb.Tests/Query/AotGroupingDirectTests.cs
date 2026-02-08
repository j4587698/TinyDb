using TinyDb.Query;

namespace TinyDb.Tests.Query;

/// <summary>
/// Direct tests for AotGrouping class to improve coverage (12.7% -> higher)
/// </summary>
public class AotGroupingDirectTests
{
    [Test]
    public async Task Constructor_ShouldInitializeKeyAndElements()
    {
        var elements = new object[] { 1, 2, 3 };
        var grouping = new QueryPipeline.AotGrouping("testKey", elements);

        await Assert.That(grouping.Key).IsEqualTo("testKey");
        await Assert.That(grouping.Count).IsEqualTo(3);
    }

    [Test]
    public async Task GetEnumerator_ShouldEnumerateElements()
    {
        var elements = new object[] { "a", "b", "c" };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var list = new List<object>();
        foreach (var item in grouping)
        {
            list.Add(item);
        }

        await Assert.That(list.Count).IsEqualTo(3);
        await Assert.That(list[0]).IsEqualTo("a");
        await Assert.That(list[1]).IsEqualTo("b");
        await Assert.That(list[2]).IsEqualTo("c");
    }

    [Test]
    public async Task NonGenericGetEnumerator_ShouldWork()
    {
        var elements = new object[] { 1, 2 };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var enumerable = (System.Collections.IEnumerable)grouping;
        var list = new List<object>();
        foreach (var item in enumerable)
        {
            list.Add(item);
        }

        await Assert.That(list.Count).IsEqualTo(2);
    }

    #region Sum Tests

    [Test]
    public async Task Sum_WithIntValues_ShouldCalculateCorrectly()
    {
        var elements = new object[] { 10, 20, 30 };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var sum = grouping.Sum(x => x);

        await Assert.That(sum).IsEqualTo(60m);
    }

    [Test]
    public async Task Sum_WithDecimalValues_ShouldCalculateCorrectly()
    {
        var elements = new object[] { 1.5m, 2.5m, 3.0m };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var sum = grouping.Sum(x => x);

        await Assert.That(sum).IsEqualTo(7.0m);
    }

    [Test]
    public async Task Sum_WithNullValues_ShouldSkipNulls()
    {
        var elements = new object[] { 10, null!, 30 };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var sum = grouping.Sum(x => x);

        await Assert.That(sum).IsEqualTo(40m);
    }

    [Test]
    public async Task Sum_WithSelector_ShouldApplySelector()
    {
        var elements = new object[] { new TestItem(10), new TestItem(20) };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var sum = grouping.Sum(x => ((TestItem)x).Value);

        await Assert.That(sum).IsEqualTo(30m);
    }

    [Test]
    public async Task Sum_WithSelectorReturningNull_ShouldSkipNulls()
    {
        var elements = new object[] { new TestItem(10), new TestItemNullable(null), new TestItem(20) };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var sum = grouping.Sum(x => x is TestItem t ? (object)t.Value : null!);

        await Assert.That(sum).IsEqualTo(30m);
    }

    [Test]
    public async Task Sum_EmptyCollection_ShouldReturnZero()
    {
        var grouping = new QueryPipeline.AotGrouping("key", Array.Empty<object>());

        var sum = grouping.Sum(x => x);

        await Assert.That(sum).IsEqualTo(0m);
    }

    #endregion

    #region Average Tests

    [Test]
    public async Task Average_WithIntValues_ShouldCalculateCorrectly()
    {
        var elements = new object[] { 10, 20, 30 };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var avg = grouping.Average(x => x);

        await Assert.That(avg).IsEqualTo(20m);
    }

    [Test]
    public async Task Average_EmptyCollection_ShouldReturnZero()
    {
        var grouping = new QueryPipeline.AotGrouping("key", Array.Empty<object>());

        var avg = grouping.Average(x => x);

        await Assert.That(avg).IsEqualTo(0m);
    }

    [Test]
    public async Task Average_WithSelector_ShouldApplySelector()
    {
        var elements = new object[] { new TestItem(10), new TestItem(30) };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var avg = grouping.Average(x => ((TestItem)x).Value);

        await Assert.That(avg).IsEqualTo(20m);
    }

    #endregion

    #region Min Tests

    [Test]
    public async Task Min_WithIntValues_ShouldFindMinimum()
    {
        var elements = new object[] { 30, 10, 20 };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var min = grouping.Min(x => x);

        await Assert.That(min).IsEqualTo(10);
    }

    [Test]
    public async Task Min_WithStringValues_ShouldFindMinimum()
    {
        var elements = new object[] { "banana", "apple", "cherry" };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var min = grouping.Min(x => x);

        await Assert.That(min).IsEqualTo("apple");
    }

    [Test]
    public async Task Min_EmptyCollection_ShouldReturnNull()
    {
        var grouping = new QueryPipeline.AotGrouping("key", Array.Empty<object>());

        var min = grouping.Min(x => x);

        await Assert.That(min).IsNull();
    }

    [Test]
    public async Task Min_WithNullValues_ShouldSkipNulls()
    {
        var elements = new object[] { 30, null!, 10 };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var min = grouping.Min(x => x);

        await Assert.That(min).IsEqualTo(10);
    }

    [Test]
    public async Task Min_WithSelector_ShouldApplySelector()
    {
        var elements = new object[] { new TestItem(30), new TestItem(10), new TestItem(20) };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var min = grouping.Min(x => ((TestItem)x).Value);

        await Assert.That(min).IsEqualTo(10);
    }

    [Test]
    public async Task Min_AllNullValues_ShouldReturnNull()
    {
        var elements = new object[] { new TestItemNullable(null), new TestItemNullable(null) };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var min = grouping.Min(x => ((TestItemNullable)x).Value!);

        await Assert.That(min).IsNull();
    }

    #endregion

    #region Max Tests

    [Test]
    public async Task Max_WithIntValues_ShouldFindMaximum()
    {
        var elements = new object[] { 10, 30, 20 };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var max = grouping.Max(x => x);

        await Assert.That(max).IsEqualTo(30);
    }

    [Test]
    public async Task Max_WithStringValues_ShouldFindMaximum()
    {
        var elements = new object[] { "banana", "apple", "cherry" };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var max = grouping.Max(x => x);

        await Assert.That(max).IsEqualTo("cherry");
    }

    [Test]
    public async Task Max_EmptyCollection_ShouldReturnNull()
    {
        var grouping = new QueryPipeline.AotGrouping("key", Array.Empty<object>());

        var max = grouping.Max(x => x);

        await Assert.That(max).IsNull();
    }

    [Test]
    public async Task Max_WithNullValues_ShouldSkipNulls()
    {
        var elements = new object[] { 10, null!, 30 };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var max = grouping.Max(x => x);

        await Assert.That(max).IsEqualTo(30);
    }

    [Test]
    public async Task Max_WithSelector_ShouldApplySelector()
    {
        var elements = new object[] { new TestItem(10), new TestItem(30), new TestItem(20) };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var max = grouping.Max(x => ((TestItem)x).Value);

        await Assert.That(max).IsEqualTo(30);
    }

    [Test]
    public async Task Max_AllNullValues_ShouldReturnNull()
    {
        var elements = new object[] { new TestItemNullable(null), new TestItemNullable(null) };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var max = grouping.Max(x => ((TestItemNullable)x).Value!);

        await Assert.That(max).IsNull();
    }

    #endregion

    #region IGrouping Interface Tests

    [Test]
    public async Task AsIGrouping_Key_ShouldBeAccessible()
    {
        var elements = new object[] { 1, 2, 3 };
        IGrouping<object, object> grouping = new QueryPipeline.AotGrouping("myKey", elements);

        await Assert.That(grouping.Key).IsEqualTo("myKey");
    }

    [Test]
    public async Task AsIGrouping_Enumeration_ShouldWork()
    {
        var elements = new object[] { "x", "y", "z" };
        IGrouping<object, object> grouping = new QueryPipeline.AotGrouping("key", elements);

        var count = 0;
        foreach (var _ in grouping) count++;

        await Assert.That(count).IsEqualTo(3);
    }

    #endregion

    #region Edge Cases

    [Test]
    public async Task NullKey_ShouldBeAllowed()
    {
        var elements = new object[] { 1 };
        var grouping = new QueryPipeline.AotGrouping(null!, elements);

        await Assert.That(grouping.Key).IsNull();
    }

    [Test]
    public async Task SingleElement_AllOperationsShouldWork()
    {
        var elements = new object[] { 42 };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        await Assert.That(grouping.Count).IsEqualTo(1);
        await Assert.That(grouping.Sum(x => x)).IsEqualTo(42m);
        await Assert.That(grouping.Average(x => x)).IsEqualTo(42m);
        await Assert.That(grouping.Min(x => x)).IsEqualTo(42);
        await Assert.That(grouping.Max(x => x)).IsEqualTo(42);
    }

    [Test]
    public async Task MixedTypes_SumShouldConvertToDecimal()
    {
        var elements = new object[] { 10, 20L, 30.5, 40m };
        var grouping = new QueryPipeline.AotGrouping("key", elements);

        var sum = grouping.Sum(x => x);

        await Assert.That(sum).IsEqualTo(100.5m);
    }

    #endregion

    #region Helper Classes

    private record TestItem(int Value);
    private record TestItemNullable(int? Value);

    #endregion
}
