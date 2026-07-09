using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Query;
using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Index;
using TinyDb.Tests.Utils;
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
        // Case 1: No keys
        var planEmpty = new QueryExecutionPlan { IndexScanKeys = new List<IndexScanKey>() };
        var rangeEmpty = QueryExecutor.BuildIndexScanRange(planEmpty);
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
        var rangeGT = QueryExecutor.BuildIndexScanRange(planGT);
        await Assert.That(rangeGT.IncludeMin).IsFalse();

        var planCompoundGT = new QueryExecutionPlan
        {
            UseIndex = new IndexStatistics { Fields = new[] { "a", "b" } },
            IndexScanKeys = new List<IndexScanKey>
            {
                new IndexScanKey { FieldName = "a", Value = new BsonInt32(10), ComparisonType = ComparisonType.GreaterThan }
            }
        };
        var rangeCompoundGT = QueryExecutor.BuildIndexScanRange(planCompoundGT);
        await Assert.That(rangeCompoundGT.MinKey.Length).IsEqualTo(2);
        await Assert.That(rangeCompoundGT.MinKey.Values[0].RawValue).IsEqualTo(10);
        await Assert.That(rangeCompoundGT.MinKey.Values[1]).IsTypeOf<BsonMaxKey>();

        // Case 3: LessThan
        var planLT = new QueryExecutionPlan
        {
            IndexScanKeys = new List<IndexScanKey>
            {
                new IndexScanKey { FieldName = "a", Value = new BsonInt32(10), ComparisonType = ComparisonType.LessThan }
            }
        };
        var rangeLT = QueryExecutor.BuildIndexScanRange(planLT);
        await Assert.That(rangeLT.IncludeMax).IsFalse();

        // Case 4: LessThanOrEqual
        var planLTE = new QueryExecutionPlan
        {
            IndexScanKeys = new List<IndexScanKey>
            {
                new IndexScanKey { FieldName = "a", Value = new BsonInt32(10), ComparisonType = ComparisonType.LessThanOrEqual }
            }
        };
        var rangeLTE = QueryExecutor.BuildIndexScanRange(planLTE);
        await Assert.That(rangeLTE.IncludeMax).IsTrue();

        // Case 5: NotEqual (falls through switch without explicit handling)
        var planNE = new QueryExecutionPlan
        {
            IndexScanKeys = new List<IndexScanKey>
            {
                new IndexScanKey { FieldName = "a", Value = new BsonInt32(10), ComparisonType = ComparisonType.NotEqual }
            }
        };
        var rangeNE = QueryExecutor.BuildIndexScanRange(planNE);
        await Assert.That(rangeNE).IsNotNull();

        // Case 6: Bounded range on the same field
        var planRange = new QueryExecutionPlan
        {
            IndexScanKeys = new List<IndexScanKey>
            {
                new IndexScanKey
                {
                    FieldName = "a",
                    Value = new BsonInt32(18),
                    ComparisonType = ComparisonType.Range,
                    LowerValue = new BsonInt32(18),
                    UpperValue = new BsonInt32(65),
                    IncludeLower = false,
                    IncludeUpper = false
                }
            }
        };
        var rangeBounded = QueryExecutor.BuildIndexScanRange(planRange);
        await Assert.That(rangeBounded.MinKey.Values[0].RawValue).IsEqualTo(18);
        await Assert.That(rangeBounded.MaxKey.Values[0].RawValue).IsEqualTo(65);
        await Assert.That(rangeBounded.IncludeMin).IsFalse();
        await Assert.That(rangeBounded.IncludeMax).IsFalse();
    }

    [Test]
    public async Task ExecuteFullTableScan_ShouldFilterMismatchedCollectionDocuments_And_HandleNullDocumentIdKeys()
    {
        var colA = $"col_a_{Guid.NewGuid():N}";
        var colB = $"col_b_{Guid.NewGuid():N}";

        var a = _engine.GetBsonCollection(colA);
        var b = _engine.GetBsonCollection(colB);

        _ = a.Insert(new BsonDocument().Set("n", 1));
        _ = b.Insert(new BsonDocument().Set("n", 2));

        var stateA = _engine.GetCollectionState(colA);
        var stateB = _engine.GetCollectionState(colB);

        var foreignPage = stateB.OwnedPages.Keys.First();
        stateA.OwnedPages.TryAdd(foreignPage, 0);

        using var tx = (Transaction)_engine.BeginTransaction();
        tx.AddOperation(new TransactionOperation(TransactionOperationType.Delete, colA, documentId: null));
        tx.AddOperation(new TransactionOperation(TransactionOperationType.Delete, colA, documentId: ObjectId.NewObjectId()));

        var executor = new QueryExecutor(_engine);
        var results = executor.Execute<BsonDocument>(colA).ToList();

        await Assert.That(results.Count).IsEqualTo(1);
        await Assert.That(results[0].TryGetValue("_collection", out var c)).IsTrue();
        await Assert.That(c!.ToString()).IsEqualTo(colA);
    }

    [Test]
    public async Task BuildExactIndexKey_Coverage()
    {
        // Case 1: Empty
        var planEmpty = new QueryExecutionPlan { IndexScanKeys = new List<IndexScanKey>() };
        var keyEmpty = QueryExecutor.BuildExactIndexKey(planEmpty);
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
        var keyMixed = QueryExecutor.BuildExactIndexKey(planMixed);
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
        var result = QueryExecutorTestDriver.ExecuteIndexScan(executor, plan);
        await Assert.That(result).IsNotNull();
        await Assert.That(result!.Count()).IsEqualTo(1);
    }
}
