using System;
using System.IO;
using System.Linq;
using TinyDb.Attributes;
using TinyDb.Collections;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryableTerminalMethodsCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly ITinyCollection<Item> _items;

    public QueryableTerminalMethodsCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"query_terms_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath);
        _items = _engine.GetCollection<Item>("items");

        _items.Insert(new Item { Id = 1, Category = "A", Value = 10, InStock = true });
        _items.Insert(new Item { Id = 2, Category = "A", Value = 20, InStock = true });
        _items.Insert(new Item { Id = 3, Category = "A", Value = 30, InStock = false });
        _items.Insert(new Item { Id = 4, Category = "B", Value = 40, InStock = true });
        _items.Insert(new Item { Id = 5, Category = "B", Value = 50, InStock = true });
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task Terminal_Count_LongCount_ShouldWork()
    {
        var q = _items.Query().Where(x => x.Value > 0);
        await Assert.That(q.Count()).IsEqualTo(5);
        await Assert.That(q.LongCount()).IsEqualTo(5L);
    }

    [Test]
    public async Task Terminal_Count_LongCount_WithPredicate_ShouldWork()
    {
        await Assert.That(_items.Query().Count(x => x.InStock)).IsEqualTo(4);
        await Assert.That(_items.Query().LongCount(x => x.InStock)).IsEqualTo(4L);
    }

    [Test]
    public async Task Terminal_First_Last_ElementAt_ShouldWork()
    {
        var q = _items.Query().OrderBy(x => x.Id);

        await Assert.That(q.First().Id).IsEqualTo(1);
        await Assert.That(q.Last().Id).IsEqualTo(5);
        await Assert.That(q.ElementAt(2).Id).IsEqualTo(3);
    }

    [Test]
    public async Task Terminal_FirstOrDefault_LastOrDefault_ElementAtOrDefault_ShouldReturnNullWhenMissing()
    {
        var q = _items.Query().OrderBy(x => x.Id);

        await Assert.That(q.FirstOrDefault(x => x.Id == 999)).IsNull();
        await Assert.That(q.LastOrDefault(x => x.Id == 999)).IsNull();
        await Assert.That(q.ElementAtOrDefault(99)).IsNull();
    }

    [Test]
    public async Task Terminal_Single_SingleOrDefault_ShouldWork()
    {
        await Assert.That(_items.Query().Single(x => x.Id == 3).Id).IsEqualTo(3);
        await Assert.That(_items.Query().SingleOrDefault(x => x.Id == 999)).IsNull();
    }

    [Test]
    public async Task Terminal_Any_All_ShouldWork()
    {
        await Assert.That(_items.Query().Any()).IsTrue();
        await Assert.That(_items.Query().Any(x => x.Category == "Z")).IsFalse();

        await Assert.That(_items.Query().All(x => x.Value >= 0)).IsTrue();
        await Assert.That(_items.Query().All(x => x.InStock)).IsFalse();
    }

    [Test]
    public async Task Pipeline_SkipTake_WithConstants_ShouldWork()
    {
        var ids = _items.Query()
            .OrderBy(x => x.Id)
            .Skip(1)
            .Take(2)
            .Select(x => x.Id)
            .ToList();

        await Assert.That(ids.SequenceEqual(new[] { 2, 3 })).IsTrue();
    }

    [Test]
    public async Task Pipeline_Distinct_And_ThenBy_ShouldWork()
    {
        var categories = _items.Query()
            .Select(x => x.Category)
            .Distinct()
            .OrderBy(x => x)
            .ToList();

        await Assert.That(categories.SequenceEqual(new[] { "A", "B" })).IsTrue();

        var ids = _items.Query()
            .OrderByDescending(x => x.Category)
            .ThenBy(x => x.Value)
            .Select(x => x.Id)
            .ToList();

        await Assert.That(ids.SequenceEqual(new[] { 4, 5, 1, 2, 3 })).IsTrue();
    }

    [Test]
    public async Task Pipeline_UnsupportedOperator_ShouldThrow_InAotOnlyMode()
    {
        await Assert.That(() => _items.Query()
                .OrderBy(x => x.Id)
                .Select(x => x.Id)
                .Reverse()
                .ToList())
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task GroupBy_DirectEnumeration_ShouldThrow_InAotOnlyMode()
    {
        await Assert.That(() => _items.Query().GroupBy(x => x.Category).ToList())
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task GroupBy_Aggregations_ShouldWork()
    {
        var sum = _items.Query().GroupBy(x => x.Category).Sum(g => (decimal)g.Count());
        var avg = _items.Query().GroupBy(x => x.Category).Average(g => (decimal)g.Count());
        var min = _items.Query().GroupBy(x => x.Category).Min(g => (decimal)g.Count());
        var max = _items.Query().GroupBy(x => x.Category).Max(g => (decimal)g.Count());

        await Assert.That(sum).IsEqualTo(5m);
        await Assert.That(avg).IsEqualTo(2.5m);
        await Assert.That(min).IsEqualTo(2m);
        await Assert.That(max).IsEqualTo(3m);
    }

    [Entity]
    public sealed class Item
    {
        public int Id { get; set; }
        public string Category { get; set; } = "";
        public int Value { get; set; }
        public bool InStock { get; set; }
    }
}
