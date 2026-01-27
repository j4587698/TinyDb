using System;
using System.IO;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

/// <summary>
/// Edge case tests for QueryOptimizer to improve coverage
/// Focuses on: AND expressions with Id, comparison types, value type conversion
/// </summary>
public class QueryOptimizerEdgeCaseTests
{
    private string _testFile = null!;
    private TinyDbEngine _engine = null!;
    private QueryOptimizer _optimizer = null!;

    [Before(Test)]
    public void Setup()
    {
        _testFile = Path.Combine(Path.GetTempPath(), $"optimizer_edge_{Guid.NewGuid():N}.db");
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

    #region Primary Key in AND Expression Tests

    [Test]
    [SkipInAot("ObjectId constructor expression requires Lambda.Compile() which is not available in AOT")]
    public async Task CreateExecutionPlan_IdOnLeftSideOfAnd_ShouldUsePrimaryKeyLookup()
    {
        // Arrange: x => x.Id == 123 && x.Name == "Test"
        // This should find the Id on the left side of AND
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", 
            e => e.Id == new ObjectId("507f1f77bcf86cd799439011") && e.Name == "Test");
        
        // Assert
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.PrimaryKeyLookup);
        await Assert.That(plan.IndexScanKeys).IsNotNull();
        await Assert.That(plan.IndexScanKeys!.Count).IsEqualTo(1);
        await Assert.That(plan.IndexScanKeys[0].FieldName).IsEqualTo("_id");
    }

    [Test]
    [SkipInAot("ObjectId constructor expression requires Lambda.Compile() which is not available in AOT")]
    public async Task CreateExecutionPlan_IdOnRightSideOfAnd_ShouldUsePrimaryKeyLookup()
    {
        // Arrange: x => x.Name == "Test" && x.Id == 123
        // This should find the Id on the right side of AND
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity",
            e => e.Name == "Test" && e.Id == new ObjectId("507f1f77bcf86cd799439011"));
        
        // Assert
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.PrimaryKeyLookup);
    }

    [Test]
    [SkipInAot("ObjectId constructor expression requires Lambda.Compile() which is not available in AOT")]
    public async Task CreateExecutionPlan_NestedAndWithId_ShouldUsePrimaryKeyLookup()
    {
        // Arrange: x => (x.Age > 20 && x.Id == 123) && x.Name == "Test"
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity",
            e => (e.Age > 20 && e.Id == new ObjectId("507f1f77bcf86cd799439011")) && e.Name == "Test");
        
        // Assert
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.PrimaryKeyLookup);
    }

    #endregion

    #region Comparison Type Tests

    [Test]
    public async Task CreateExecutionPlan_GreaterThan_ShouldExtractCorrectComparisonType()
    {
        _engine.EnsureIndex("TestEntity", "Age", "age_idx");
        
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", e => e.Age > 25);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].ComparisonType).IsEqualTo(ComparisonType.GreaterThan);
    }

    [Test]
    public async Task CreateExecutionPlan_GreaterThanOrEqual_ShouldExtractCorrectComparisonType()
    {
        _engine.EnsureIndex("TestEntity", "Age", "age_idx");
        
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", e => e.Age >= 25);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].ComparisonType).IsEqualTo(ComparisonType.GreaterThanOrEqual);
    }

    [Test]
    public async Task CreateExecutionPlan_LessThan_ShouldExtractCorrectComparisonType()
    {
        _engine.EnsureIndex("TestEntity", "Age", "age_idx");
        
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", e => e.Age < 25);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].ComparisonType).IsEqualTo(ComparisonType.LessThan);
    }

    [Test]
    public async Task CreateExecutionPlan_LessThanOrEqual_ShouldExtractCorrectComparisonType()
    {
        _engine.EnsureIndex("TestEntity", "Age", "age_idx");
        
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", e => e.Age <= 25);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].ComparisonType).IsEqualTo(ComparisonType.LessThanOrEqual);
    }

    [Test]
    public async Task CreateExecutionPlan_NotEqual_ShouldExtractCorrectComparisonType()
    {
        _engine.EnsureIndex("TestEntity", "Age", "age_idx");
        
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", e => e.Age != 25);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].ComparisonType).IsEqualTo(ComparisonType.NotEqual);
    }

    #endregion

    #region Value Type Conversion Tests

    [Test]
    public async Task CreateExecutionPlan_WithStringValue_ShouldConvertCorrectly()
    {
        _engine.EnsureIndex("TestEntity", "Name", "name_idx");
        
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", e => e.Name == "TestValue");
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].Value.RawValue).IsEqualTo("TestValue");
    }

    [Test]
    public async Task CreateExecutionPlan_WithIntValue_ShouldConvertCorrectly()
    {
        _engine.EnsureIndex("TestEntity", "Age", "age_idx");
        
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", e => e.Age == 42);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].Value.RawValue).IsEqualTo(42);
    }

    [Test]
    public async Task CreateExecutionPlan_WithLongValue_ShouldConvertCorrectly()
    {
        _engine.EnsureIndex("ProductEntity", "Stock", "stock_idx");
        
        var plan = _optimizer.CreateExecutionPlan<ProductEntity>("ProductEntity", p => p.Stock == 1000000000L);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].Value.RawValue).IsEqualTo(1000000000L);
    }

    [Test]
    public async Task CreateExecutionPlan_WithDoubleValue_ShouldConvertCorrectly()
    {
        _engine.EnsureIndex("ProductEntity", "Rating", "rating_idx");
        
        var plan = _optimizer.CreateExecutionPlan<ProductEntity>("ProductEntity", p => p.Rating == 4.5);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].Value.RawValue).IsEqualTo(4.5);
    }

    [Test]
    public async Task CreateExecutionPlan_WithDecimalValue_ShouldConvertCorrectly()
    {
        _engine.EnsureIndex("ProductEntity", "Price", "price_idx");
        
        var plan = _optimizer.CreateExecutionPlan<ProductEntity>("ProductEntity", p => p.Price == 99.99m);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        // RawValue is Decimal128, so convert to decimal for comparison
        var rawValue = plan.IndexScanKeys![0].Value.RawValue;
        if (rawValue is TinyDb.Bson.Decimal128 d128)
        {
            await Assert.That(d128.ToDecimal()).IsEqualTo(99.99m);
        }
        else
        {
            await Assert.That((decimal)rawValue!).IsEqualTo(99.99m);
        }
    }

    [Test]
    public async Task CreateExecutionPlan_WithBoolValue_ShouldConvertCorrectly()
    {
        _engine.EnsureIndex("ProductEntity", "IsActive", "active_idx");
        
        var plan = _optimizer.CreateExecutionPlan<ProductEntity>("ProductEntity", p => p.IsActive == true);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].Value.RawValue).IsEqualTo(true);
    }

    [Test]
    public async Task CreateExecutionPlan_WithDateTimeValue_ShouldConvertCorrectly()
    {
        _engine.EnsureIndex("ProductEntity", "CreatedAt", "date_idx");
        
        var testDate = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc);
        var plan = _optimizer.CreateExecutionPlan<ProductEntity>("ProductEntity", p => p.CreatedAt == testDate);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
        await Assert.That(plan.IndexScanKeys![0].Value.RawValue).IsEqualTo(testDate);
    }

    #endregion

    #region Composite Index Tests

    [Test]
    public async Task CreateExecutionPlan_CompositeIndex_NoMatchPrefix_ShouldFallbackToFullScan()
    {
        // Create composite index (Age, Name) but query only uses Name (not prefix match)
        _engine.GetIndexManager("TestEntity").CreateIndex("age_name_idx", new[] { "Age", "Name" }, false);
        
        // Query only uses Name which is not the prefix
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", e => e.Name == "Test");
        
        // Should not use the composite index because it doesn't match prefix
        // It should either use a different index or full table scan
        await Assert.That(plan.UseIndex?.Name ?? "").IsNotEqualTo("age_name_idx");
    }

    [Test]
    public async Task CreateExecutionPlan_CompositeIndex_PrefixMatchOnly_ShouldUseIndex()
    {
        // Create composite index (Age, Name)
        _engine.GetIndexManager("TestEntity").CreateIndex("age_name_composite", new[] { "Age", "Name" }, false);
        
        // Query only uses Age (prefix match)
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", e => e.Age == 30);
        
        // Should be able to use the composite index with prefix match
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexScan);
    }

    #endregion

    #region Unique Index Tests

    [Test]
    public async Task CreateExecutionPlan_UniqueIndex_EqualMatch_ShouldUseIndexSeek()
    {
        // Create unique index
        _engine.GetIndexManager("TestEntity").CreateIndex("email_unique", new[] { "Email" }, true);
        
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", e => e.Email == "test@example.com");
        
        // Unique index with exact match should use IndexSeek
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.IndexSeek);
    }

    #endregion

    #region Null and Edge Cases

    [Test]
    public async Task CreateExecutionPlan_NullExpression_ShouldUseFullTableScan()
    {
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("TestEntity", null);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.FullTableScan);
    }

    [Test]
    public async Task CreateExecutionPlan_NoIndexManager_ShouldUseFullTableScan()
    {
        // Query a collection that doesn't have an index manager set up
        var plan = _optimizer.CreateExecutionPlan<TestEntity>("NonExistentCollection", e => e.Age > 30);
        
        await Assert.That(plan.Strategy).IsEqualTo(QueryExecutionStrategy.FullTableScan);
    }

    #endregion

    #region Test Entities

    private class TestEntity
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public string Email { get; set; } = string.Empty;
    }

    private class ProductEntity
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public decimal Price { get; set; }
        public long Stock { get; set; }
        public double Rating { get; set; }
        public bool IsActive { get; set; }
        public DateTime CreatedAt { get; set; }
    }

    #endregion
}
