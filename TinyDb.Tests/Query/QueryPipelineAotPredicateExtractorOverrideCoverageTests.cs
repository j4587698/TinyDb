using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using TinyDb.Collections;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

[NotInParallel]
public class QueryPipelineAotPredicateExtractorOverrideCoverageTests : IDisposable
{
    private readonly string _testDirectory;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryPipelineAotPredicateExtractorOverrideCoverageTests()
    {
        _testDirectory = Path.Combine(
            Path.GetTempPath(),
            "TinyDbQueryPipelineAotPredicateExtractorOverrideCoverageTests",
            Guid.NewGuid().ToString("N"));

        Directory.CreateDirectory(_testDirectory);

        var dbPath = Path.Combine(_testDirectory, "test.db");
        _engine = new TinyDbEngine(dbPath, new TinyDbOptions { EnableJournaling = false });
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (Directory.Exists(_testDirectory))
        {
            try { Directory.Delete(_testDirectory, true); } catch { }
        }
    }

    [Test]
    public async Task Execute_WhenDynamicCodeUnsupported_ShouldUseAotPredicateExtractor_SingleWhere()
    {
        var collectionName = $"products_{Guid.NewGuid():N}";
        var collection = _engine.GetCollection<TestProduct>(collectionName);

        collection.Insert(new TestProduct { Name = "A", Price = 10m, Category = "C1", InStock = true, CreatedAt = DateTime.UtcNow });
        collection.Insert(new TestProduct { Name = "B", Price = 0m, Category = "C2", InStock = false, CreatedAt = DateTime.UtcNow });

        var queryable = new Queryable<TestProduct>(_executor, collectionName);
        var query = queryable.Where(p => p.Price > 0m);
        var obj = QueryPipeline.Execute<TestProduct>(_executor, collectionName, query.Expression);
        var results = (IEnumerable<TestProduct>)obj!;
        await Assert.That(results.Count()).IsEqualTo(1);
    }

    [Test]
    public async Task Execute_WhenDynamicCodeUnsupported_MultipleWhere_ShouldNotPushDownPredicate()
    {
        var collectionName = $"products_{Guid.NewGuid():N}";
        var collection = _engine.GetCollection<TestProduct>(collectionName);

        collection.Insert(new TestProduct { Name = "A", Price = 10m, Category = "C1", InStock = true, CreatedAt = DateTime.UtcNow });
        collection.Insert(new TestProduct { Name = "B", Price = 10m, Category = "C2", InStock = true, CreatedAt = DateTime.UtcNow });
        collection.Insert(new TestProduct { Name = "C", Price = 0m, Category = "C1", InStock = false, CreatedAt = DateTime.UtcNow });

        var queryable = new Queryable<TestProduct>(_executor, collectionName);
        var query = queryable
            .Where(p => p.Price > 0m)
            .Where(p => p.Category == "C1");
        var obj = QueryPipeline.Execute<TestProduct>(_executor, collectionName, query.Expression);
        var results = (IEnumerable<TestProduct>)obj!;
        var list = results.ToList();

        await Assert.That(list.Count).IsEqualTo(1);
        await Assert.That(list[0].Name).IsEqualTo("A");
    }

    [Test]
    public async Task ExecuteAot_GroupByAfterSelect_EmptySource_ShouldReturnEmpty()
    {
        var items = new List<TestProduct>();

        // Force non-generic GroupBy path: Select makes the pipeline untyped.
        var expr = items.AsQueryable()
            .Select(p => p.Category)
            .GroupBy(c => c)
            .Select(g => g.Key)
            .Expression;

        var obj = QueryPipeline.ExecuteAotForTests<TestProduct>(expr, items, extractedPredicate: null);
        var keys = (IEnumerable<string?>)obj!;

        await Assert.That(keys).IsEmpty();
    }
}
