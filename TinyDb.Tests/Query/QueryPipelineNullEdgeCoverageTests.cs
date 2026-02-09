using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryPipelineNullEdgeCoverageTests
{
    public sealed class Item
    {
        public int Id { get; set; }
        public string? Category { get; set; }
        public DateTime When { get; set; }
    }

    public sealed class Projection
    {
        public string? Category { get; set; }
    }

    [Test]
    public async Task ExecuteAot_Where_ShouldSkipNullItems_AndFilterFalse()
    {
        var items = new List<Item>
        {
            null!,
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" }
        };

        var query = TestQueryables.InMemory(items).Where(x => x.Id > 1);
        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null)!;

        var ids = result.Cast<Item>().Select(x => x.Id).ToList();
        await Assert.That(ids.SequenceEqual(new[] { 2 })).IsTrue();
    }

    [Test]
    public async Task ExecuteAot_GroupBy_Generic_NullKey_ShouldUseEmptyString()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = null },
            new() { Id = 2, Category = "A" }
        };

        var query = TestQueryables.InMemory(items)
            .GroupBy(x => x.Category)
            .Select(g => g.Key);

        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null)!;
        var keys = result.Cast<string>().ToList();

        await Assert.That(keys).Contains("");
        await Assert.That(keys).Contains("A");
    }

    [Test]
    public async Task ExecuteAot_GroupBy_NonGeneric_NullKey_ShouldUseEmptyString()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = null },
            new() { Id = 2, Category = "A" }
        };

        var query = TestQueryables.InMemory(items)
            .Select(x => new Projection { Category = x.Category })
            .GroupBy(x => x.Category)
            .Select(g => g.Key);

        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null)!;
        var keys = result.Cast<string>().ToList();

        await Assert.That(keys).Contains("");
        await Assert.That(keys).Contains("A");
    }

    [Test]
    public async Task ObjectComparer_Compare_ShouldHandleNull_String_IComparable_AndFallback()
    {
        var comparerType = typeof(QueryPipeline).GetNestedType("ObjectComparer", BindingFlags.NonPublic);
        await Assert.That(comparerType).IsNotNull();

        var comparer = (IComparer<object>)Activator.CreateInstance(comparerType!)!;

        await Assert.That(comparer.Compare(null!, new object())).IsEqualTo(-1);
        await Assert.That(comparer.Compare(new object(), null!)).IsEqualTo(1);

        await Assert.That(comparer.Compare(1, 2)).IsLessThan(0);
        await Assert.That(comparer.Compare("a", "b")).IsLessThan(0);

        var earlier = new DateTime(2000, 1, 1);
        var later = new DateTime(2000, 1, 2);
        await Assert.That(comparer.Compare(earlier, later)).IsLessThan(0);

        // Different comparable types: should fall back to string comparison without throwing.
        var mixed = comparer.Compare(earlier, "x");
        await Assert.That(mixed).IsNotEqualTo(0);
    }
}
