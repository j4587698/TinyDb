using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryExecutorIndexOverlayLineCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryExecutorIndexOverlayLineCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qe_idx_overlay_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath, new TinyDbOptions { EnableJournaling = false });
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
        var wal = Path.Combine(Path.GetDirectoryName(_dbPath)!, $"{Path.GetFileNameWithoutExtension(_dbPath)}-wal.db");
        try { if (File.Exists(wal)) File.Delete(wal); } catch { }
    }

    [Test]
    public async Task ExecuteIndexScanForTests_ShouldCoverTransactionOverlayYieldBranch()
    {
        var colName = "idx_scan_overlay";
        var idxMgr = _engine.GetIndexManager(colName);
        idxMgr.CreateIndex("idx_a", new[] { "a" }, unique: false);

        var col = _engine.GetBsonCollection(colName);
        col.Insert(new BsonDocument().Set("_id", 1).Set("a", 1));
        col.Insert(new BsonDocument().Set("_id", 2).Set("a", 2));

        var idxStats = idxMgr.GetAllStatistics().Single(s => s.Name == "idx_a");

        using var tx = (Transaction)_engine.BeginTransaction();
        tx.Operations.Add(new TransactionOperation(
            TransactionOperationType.Update,
            colName,
            documentId: new BsonInt32(1),
            newDocument: new BsonDocument().Set("_id", 1).Set("_collection", colName).Set("a", 100)));

        var plan = new QueryExecutionPlan
        {
            CollectionName = colName,
            OriginalExpression = (Expression<Func<BsonDocument, bool>>)(_ => true),
            UseIndex = idxStats,
            IndexScanKeys = new List<IndexScanKey>()
        };

        var result = _executor.ExecuteIndexScanForTests(plan).ToList();
        await Assert.That(result.Any(d => d["_id"].ToInt32() == 1 && d["a"].ToInt32() == 100)).IsTrue();

        tx.Rollback();
    }

    [Test]
    public async Task ExecuteIndexSeekForTests_ShouldCoverUniqueAndNonUniqueOverlayYieldBranches()
    {
        var uniqueCol = "idx_seek_unique";
        var uniqueMgr = _engine.GetIndexManager(uniqueCol);
        uniqueMgr.CreateIndex("idx_uk", new[] { "uk" }, unique: true);

        var unique = _engine.GetBsonCollection(uniqueCol);
        unique.Insert(new BsonDocument().Set("_id", 10).Set("uk", 7).Set("v", 1));

        var uniqueStats = uniqueMgr.GetAllStatistics().Single(s => s.Name == "idx_uk");

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            tx.Operations.Add(new TransactionOperation(
                TransactionOperationType.Update,
                uniqueCol,
                documentId: new BsonInt32(10),
                newDocument: new BsonDocument().Set("_id", 10).Set("_collection", uniqueCol).Set("uk", 7).Set("v", 99)));

            var uniquePlan = new QueryExecutionPlan
            {
                CollectionName = uniqueCol,
                OriginalExpression = (Expression<Func<BsonDocument, bool>>)(_ => true),
                UseIndex = uniqueStats,
                IndexScanKeys = new List<IndexScanKey>
                {
                    new() { FieldName = "uk", Value = new BsonInt32(7), ComparisonType = ComparisonType.Equal }
                }
            };

            var uniqueResult = _executor.ExecuteIndexSeekForTests<BsonDocument>(uniquePlan).ToList();
            await Assert.That(uniqueResult.Any(d => d["_id"].ToInt32() == 10 && d["v"].ToInt32() == 99)).IsTrue();
            tx.Rollback();
        }

        var nonUniqueCol = "idx_seek_non_unique";
        var nonUniqueMgr = _engine.GetIndexManager(nonUniqueCol);
        nonUniqueMgr.CreateIndex("idx_nk", new[] { "nk" }, unique: false);

        var nonUnique = _engine.GetBsonCollection(nonUniqueCol);
        nonUnique.Insert(new BsonDocument().Set("_id", 1).Set("nk", 5).Set("v", 1));
        nonUnique.Insert(new BsonDocument().Set("_id", 2).Set("nk", 5).Set("v", 2));

        var nonUniqueStats = nonUniqueMgr.GetAllStatistics().Single(s => s.Name == "idx_nk");

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            tx.Operations.Add(new TransactionOperation(
                TransactionOperationType.Update,
                nonUniqueCol,
                documentId: new BsonInt32(1),
                newDocument: new BsonDocument().Set("_id", 1).Set("_collection", nonUniqueCol).Set("nk", 5).Set("v", 500)));

            var nonUniquePlan = new QueryExecutionPlan
            {
                CollectionName = nonUniqueCol,
                OriginalExpression = (Expression<Func<BsonDocument, bool>>)(_ => true),
                UseIndex = nonUniqueStats,
                IndexScanKeys = new List<IndexScanKey>
                {
                    new() { FieldName = "nk", Value = new BsonInt32(5), ComparisonType = ComparisonType.Equal }
                }
            };

            var nonUniqueResult = _executor.ExecuteIndexSeekForTests<BsonDocument>(nonUniquePlan).ToList();
            await Assert.That(nonUniqueResult.Any(d => d["_id"].ToInt32() == 1 && d["v"].ToInt32() == 500)).IsTrue();
            tx.Rollback();
        }
    }
}
