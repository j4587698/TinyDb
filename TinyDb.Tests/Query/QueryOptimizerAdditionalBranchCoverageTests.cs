using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

[NotInParallel]
public class QueryOptimizerAdditionalBranchCoverageTests
{
    private sealed class DocWithId
    {
        public ObjectId Id { get; set; }
        public int Age { get; set; }
    }

    private string _testFile = null!;
    private TinyDbEngine _engine = null!;
    private QueryOptimizer _optimizer = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"optimizer_additional_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testFile);
        _optimizer = new QueryOptimizer(_engine);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        try
        {
            if (File.Exists(_testFile))
            {
                File.Delete(_testFile);
            }
        }
        catch
        {
        }
    }

    [Test]
    public async Task CreateExecutionPlan_NullExpression_ShouldUseFullTableScan()
    {
        var plan = _optimizer.CreateExecutionPlan<DocWithId>("DocWithId", null);
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.FullTableScan);
        await Assert.That(plan.QueryExpression).IsNull();
    }

    [Test]
    public async Task CreateExecutionPlan_WhenParseThrows_ShouldFallbackToFullTableScan()
    {
        Expression<Func<DocWithId, bool>> unsupported = x => new List<int> { x.Age }.Count > 0;

        var plan = _optimizer.CreateExecutionPlan<DocWithId>("DocWithId", unsupported);

        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.FullTableScan);
        await Assert.That(plan.QueryExpression).IsNull();
    }

    [Test]
    public async Task CreateExecutionPlan_PrimaryKeyOnRightSide_ShouldUsePrimaryKeyLookup()
    {
        var id = new ObjectId("507f1f77bcf86cd799439011");
        var plan = _optimizer.CreateExecutionPlan<DocWithId>("DocWithId", x => id == x.Id);

        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.PrimaryKeyLookup);
        await Assert.That(plan.IndexScanKeys.Count).IsEqualTo(1);
    }

    [Test]
    public async Task CreateExecutionPlan_FieldOnRightSide_ShouldExtractValueAndComparison()
    {
        _engine.EnsureIndex("DocWithId", "Age", "age_idx");
        var plan = _optimizer.CreateExecutionPlan<DocWithId>("DocWithId", x => 42 == x.Age);

        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys.Count).IsEqualTo(1);
        await Assert.That(plan.IndexScanKeys[0].Value.RawValue).IsEqualTo(42);
        await Assert.That(plan.IndexScanKeys[0].ComparisonType).IsEqualTo(ComparisonType.Equal);
    }

    [Test]
    public async Task QueryExecutionPlan_MetadataProperties_ShouldRoundTrip()
    {
        var plan = new QueryExecutionPlan { EstimatedCost = 12.34, EstimatedResultCount = 56 };
        await Assert.That(plan.EstimatedCost).IsEqualTo(12.34);
        await Assert.That(plan.EstimatedResultCount).IsEqualTo(56);
    }
}
