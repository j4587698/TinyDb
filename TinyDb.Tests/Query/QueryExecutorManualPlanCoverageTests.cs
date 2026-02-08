using System;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryExecutorManualPlanCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryExecutorManualPlanCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qe_plan_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath);
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task ExecutePrimaryKeyLookup_WhenNoKeys_ShouldYieldBreak()
    {
        var plan = new QueryExecutionPlan
        {
            CollectionName = "col",
            IndexScanKeys = new()
        };

        var method = typeof(QueryExecutor).GetMethod("ExecutePrimaryKeyLookup", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(method).IsNotNull();

        var enumerable = (System.Collections.IEnumerable)method!.MakeGenericMethod(typeof(BsonDocument)).Invoke(_executor, new object[] { plan })!;
        var enumerator = enumerable.GetEnumerator();
        await Assert.That(enumerator.MoveNext()).IsFalse();
    }

    [Test]
    public async Task ExecutePrimaryKeyLookup_WhenQueryExpressionMissing_ShouldHitOriginalExpressionBranch()
    {
        var col = _engine.GetCollection<BsonDocument>("col");
        col.Insert(new BsonDocument().Set("_id", new BsonInt32(1)).Set("x", 1));

        Expression<Func<BsonDocument, bool>> original = d => d != null;

        var plan = new QueryExecutionPlan
        {
            CollectionName = "col",
            OriginalExpression = original,
            QueryExpression = null,
            IndexScanKeys = new()
            {
                new IndexScanKey { FieldName = "_id", Value = new BsonInt32(1), ComparisonType = ComparisonType.Equal }
            }
        };

        var method = typeof(QueryExecutor).GetMethod("ExecutePrimaryKeyLookup", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(method).IsNotNull();

        var enumerable = (System.Collections.IEnumerable)method!.MakeGenericMethod(typeof(BsonDocument)).Invoke(_executor, new object[] { plan })!;
        int count = 0;
        foreach (var _ in enumerable) count++;

        await Assert.That(count).IsEqualTo(1);
    }

    [Test]
    public async Task ExecuteIndexScan_WhenUseIndexMissing_ShouldFallbackToFullScan()
    {
        var col = _engine.GetCollection<BsonDocument>("col");
        col.Insert(new BsonDocument().Set("_id", new BsonInt32(1)).Set("x", 1));

        var plan = new QueryExecutionPlan
        {
            CollectionName = "col",
            UseIndex = null
        };

        int count = 0;
        foreach (var _ in _executor.ExecuteIndexScanForTests(plan)) count++;

        await Assert.That(count).IsEqualTo(1);
    }
}

