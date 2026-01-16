using System;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Tests.TestEntities;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

[NotInParallel]
public class QueryOptimizerTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;
    private QueryOptimizer _optimizer = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"optimizer_test_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testFile);
        _optimizer = new QueryOptimizer(_engine);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine?.Dispose();
        if (File.Exists(_testFile))
        {
            File.Delete(_testFile);
        }
    }

    [Test]
    public async Task CreateExecutionPlan_NoIndex_Should_Use_FullTableScan()
    {
        // Act
        var plan = _optimizer.CreateExecutionPlan<UserWithIntId>("UserWithIntId", u => u.Age > 30);

        // Assert
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.FullTableScan);
        await Assert.That(plan.UseIndex).IsNull();
    }

    [Test]
    public async Task CreateExecutionPlan_SingleIndex_ExactMatch_Should_Use_IndexScan()
    {
        // Arrange
        _engine.EnsureIndex("UserWithIntId", "Age", "age_idx");

        // Act
        var plan = _optimizer.CreateExecutionPlan<UserWithIntId>("UserWithIntId", u => u.Age == 25);

        // Assert
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.UseIndex).IsNotNull();
        await Assert.That(plan.UseIndex!.Name).IsEqualTo("age_idx");
        await Assert.That(plan.IndexScanKeys).Count().IsEqualTo(1);
        await Assert.That(plan.IndexScanKeys[0].FieldName).IsEqualTo("age");
    }

    [Test]
    public async Task CreateExecutionPlan_CompositeIndex_PrefixMatch_Should_Use_IndexScan()
    {
        // Arrange - Create a composite index
        _engine.GetIndexManager("UserWithIntId").CreateIndex("age_name_idx", new[] { "Age", "Name" }, false);

        // Act
        // Query matches the prefix (Age)
        var plan = _optimizer.CreateExecutionPlan<UserWithIntId>("UserWithIntId", u => u.Age == 30 && u.Name == "Test");

        // Assert
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.UseIndex!.Name).IsEqualTo("age_name_idx");
        // It might extract both fields if the optimizer is smart enough
        await Assert.That(plan.IndexScanKeys.Count).IsGreaterThanOrEqualTo(1);
    }

    [Test]
    public async Task CreateExecutionPlan_Prefer_Index_With_Fewer_Entries()
    {
        // Arrange
        var im = _engine.GetIndexManager("UserWithIntId");
        im.CreateIndex("idx_many", new[] { "Age" }, false);
        im.CreateIndex("idx_few", new[] { "Age" }, false);
        
        var idxMany = im.GetIndex("idx_many")!;
        var idxFew = im.GetIndex("idx_few")!;
        
        for (int i = 0; i < 100; i++) idxMany.Insert(new IndexKey(i), i);
        for (int i = 0; i < 10; i++) idxFew.Insert(new IndexKey(i), i);

        // Act
        var plan = _optimizer.CreateExecutionPlan<UserWithIntId>("UserWithIntId", u => u.Age == 5);

        // Assert
        await Assert.That(plan.UseIndex!.Name).IsEqualTo("idx_few");
    }

    [Test]
    public async Task CreateExecutionPlan_Prefer_Unique_Index()
    {
        // Arrange
        var im = _engine.GetIndexManager("UserWithIntId");
        im.CreateIndex("idx_nonunique", new[] { "Age" }, false);
        im.CreateIndex("idx_unique", new[] { "Age" }, true);

        // Act
        var plan = _optimizer.CreateExecutionPlan<UserWithIntId>("UserWithIntId", u => u.Age == 5);

        // Assert
        await Assert.That(plan.UseIndex!.Name).IsEqualTo("idx_unique");
    }
}
