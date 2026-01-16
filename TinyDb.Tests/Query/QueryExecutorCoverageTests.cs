using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using TinyDb.Query;
using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExecutorCoverageTests
{
    private TinyDbEngine _engine = null!;
    private string _dbPath = null!;

    [Before(Test)]
    public void Setup()
    {
        _dbPath = $"executor_test_{Guid.NewGuid():N}.db";
        _engine = new TinyDbEngine(_dbPath);
    }

    [After(Test)]
    public void Cleanup()
    {
        _engine.Dispose();
        try { System.IO.File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task Execute_Validation_Coverage()
    {
        var executor = new QueryExecutor(_engine);
        
        try 
        {
            executor.Execute<object>(null!);
            Assert.Fail("Should throw ArgumentException for null collection");
        } 
        catch (ArgumentException) {}

        try 
        {
            executor.Execute<object>("   ");
            Assert.Fail("Should throw ArgumentException for empty collection");
        } 
        catch (ArgumentException) {}
    }

    [Test]
    public async Task BuildIndexScanRange_Coverage()
    {
        // Use reflection to invoke private static BuildIndexScanRange
        var method = typeof(QueryExecutor).GetMethod("BuildIndexScanRange", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        // Case 1: No keys
        var planEmpty = new QueryExecutionPlan { IndexScanKeys = new List<IndexScanKey>() };
        var rangeEmpty = (IndexScanRange)method!.Invoke(null, new object[] { planEmpty })!;
        await Assert.That(rangeEmpty.IncludeMin).IsTrue();
        await Assert.That(rangeEmpty.IncludeMax).IsTrue();

        // Case 2: GreaterThan
        var planGT = new QueryExecutionPlan 
        { 
            IndexScanKeys = new List<IndexScanKey> 
            { 
                new IndexScanKey { FieldName = "a", Value = new BsonInt32(10), ComparisonType = ComparisonType.GreaterThan }
            } 
        };
        var rangeGT = (IndexScanRange)method.Invoke(null, new object[] { planGT })!;
        await Assert.That(rangeGT.IncludeMin).IsFalse();
        
        // Case 3: LessThan
        var planLT = new QueryExecutionPlan 
        { 
            IndexScanKeys = new List<IndexScanKey> 
            { 
                new IndexScanKey { FieldName = "a", Value = new BsonInt32(10), ComparisonType = ComparisonType.LessThan }
            } 
        };
        var rangeLT = (IndexScanRange)method.Invoke(null, new object[] { planLT })!;
        await Assert.That(rangeLT.IncludeMax).IsFalse();
        
        // Case 4: LessThanOrEqual
        var planLTE = new QueryExecutionPlan 
        { 
            IndexScanKeys = new List<IndexScanKey> 
            { 
                new IndexScanKey { FieldName = "a", Value = new BsonInt32(10), ComparisonType = ComparisonType.LessThanOrEqual }
            } 
        };
        var rangeLTE = (IndexScanRange)method.Invoke(null, new object[] { planLTE })!;
        await Assert.That(rangeLTE.IncludeMax).IsTrue();
    }
    
    [Test]
    public async Task BuildExactIndexKey_Coverage()
    {
        // Use reflection to invoke private static BuildExactIndexKey
        var method = typeof(QueryExecutor).GetMethod("BuildExactIndexKey", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        // Case 1: Empty
        var planEmpty = new QueryExecutionPlan { IndexScanKeys = new List<IndexScanKey>() };
        var keyEmpty = method!.Invoke(null, new object[] { planEmpty });
        await Assert.That(keyEmpty).IsNull();

        // Case 2: Mixed comparisons (should return null)
        var planMixed = new QueryExecutionPlan 
        { 
            IndexScanKeys = new List<IndexScanKey> 
            { 
                new IndexScanKey { FieldName = "a", Value = new BsonInt32(10), ComparisonType = ComparisonType.Equal },
                new IndexScanKey { FieldName = "b", Value = new BsonInt32(5), ComparisonType = ComparisonType.GreaterThan }
            } 
        };
        var keyMixed = method.Invoke(null, new object[] { planMixed });
        await Assert.That(keyMixed).IsNull();
    }

    [Test]
    public async Task ExecuteIndexScan_MissingIndex_Coverage()
    {
        var collectionName = "test_col_fallback";
        var doc = new BsonDocument().Set("_id", 1).Set("name", "test");
        _engine.InsertDocument(collectionName, doc);

        var plan = new QueryExecutionPlan
        {
            CollectionName = collectionName,
            Strategy = QueryExecutionStrategy.IndexScan,
            UseIndex = new IndexStatistics { Name = "missing_index", Fields = new[] { "name" } },
            IndexScanKeys = new List<IndexScanKey>(),
            OriginalExpression = null
        };

        var executor = new QueryExecutor(_engine);
        var method = typeof(QueryExecutor).GetMethod("ExecuteIndexScan", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(method).IsNotNull();
        var genericMethod = method!.MakeGenericMethod(typeof(BsonDocument));
        
        var result = genericMethod.Invoke(executor, new object[] { plan }) as IEnumerable<BsonDocument>;
        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Count()).IsEqualTo(1);
    }
}