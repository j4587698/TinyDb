using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryPipelineExecuteAotAdditionalCoverageTests
{
    public sealed class Item
    {
        public int Id { get; set; }
        public string Category { get; set; } = "";
    }

    private sealed class OrderByKey
    {
        public OrderByKey(int id) => Id = id;
        public int Id { get; }
        public override string ToString() => Id.ToString();
    }

    [Test]
    public async Task ExecuteAot_SelectThenWhere_ShouldUseNonGenericWherePath()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" }
        };

        var query = TestQueryables.InMemory(items)
            .Select(x => x.Category)
            .Where(x => x == "A");

        var result = QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null);
        await Assert.That(result).IsNotNull();

        var count = ((IEnumerable)result!).Cast<object>().Count();
        await Assert.That(count).IsEqualTo(1);
    }

    [Test]
    public async Task ExecuteAot_WhenPredicateExtracted_ShouldSkipInMemoryWhere()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" }
        };

        var query = TestQueryables.InMemory(items).Where(x => x.Id > 1);

        var result = QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: Expression.Constant(true));
        var list = (IEnumerable)result!;

        await Assert.That(list.Cast<object>().Count()).IsEqualTo(2);
    }

    [Test]
    public async Task ExecuteAot_GroupByWithoutSelect_ShouldThrowNotSupported()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" }
        };

        var query = TestQueryables.InMemory(items).GroupBy(x => x.Category);

        await Assert.That(() => QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ExecuteAot_UnsupportedOperation_ShouldThrowNotSupported()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" }
        };

        var query = TestQueryables.InMemory(items).Reverse();

        await Assert.That(() => QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ExecuteAot_OrderBy_WithAnonymousKey_ShouldUseComparerFallback()
    {
        var items = new List<Item>
        {
            new() { Id = 10, Category = "A" },
            new() { Id = 2, Category = "B" },
            new() { Id = 1, Category = "C" }
        };

        var query = TestQueryables.InMemory(items).OrderBy(x => new OrderByKey(x.Id));

        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null)!;
        await Assert.That(result.Cast<object>().Count()).IsEqualTo(3);
    }
}
