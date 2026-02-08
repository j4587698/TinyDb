using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Query;
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

    [Test]
    public async Task ExecuteAot_SelectThenWhere_ShouldUseNonGenericWherePath()
    {
        var items = new List<Item>
        {
            new() { Id = 1, Category = "A" },
            new() { Id = 2, Category = "B" }
        };

        var query = items.AsQueryable()
            .Select(x => new { x.Category })
            .Where(x => x.Category == "A");

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

        var query = items.AsQueryable().Where(x => x.Id > 1);

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

        var query = items.AsQueryable().GroupBy(x => x.Category);

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

        var query = items.AsQueryable().Reverse();

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

        var query = items.AsQueryable().OrderBy(x => new { x.Id });

        var result = (IEnumerable)QueryPipeline.ExecuteAotForTests<Item>(query.Expression, items, extractedPredicate: null)!;
        await Assert.That(result.Cast<object>().Count()).IsEqualTo(3);
    }
}

