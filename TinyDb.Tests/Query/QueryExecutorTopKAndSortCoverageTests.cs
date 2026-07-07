using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryExecutorTopKAndSortCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryExecutorTopKAndSortCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qe_topk_cov_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath, new TinyDbOptions { EnableJournaling = false });
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task ExecuteShaped_ShouldCoverValidationAndTopKBoundaryBranches()
    {
        var shape = new QueryShape<BsonDocument>
        {
            Sort = new[] { new QuerySortField("mixed", typeof(object), false) },
            Skip = 0,
            Take = 0
        };

        await Assert.That(() => _executor.ExecuteShaped<BsonDocument>(" ", shape, out _)).Throws<ArgumentException>();

        var empty = _executor.ExecuteShaped("col", shape, out var pushdown);
        await Assert.That(empty).IsEmpty();
        await Assert.That(pushdown.TakePushed).IsTrue();

        var tooLarge = new QueryShape<BsonDocument>
        {
            Sort = new[] { new QuerySortField("mixed", typeof(object), false) },
            Skip = int.MaxValue,
            Take = int.MaxValue
        };

        await Assert.That(() => _executor.ExecuteShaped<BsonDocument>("col", tooLarge, out _)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task ExecuteShaped_TopK_WithMixedBsonTypesAndTransactionOverlay_ShouldWork()
    {
        var col = _engine.GetBsonCollection("col");
        col.Insert(new BsonDocument().Set("_id", 1).Set("mixed", 3));
        col.Insert(new BsonDocument().Set("_id", 2).Set("mixed", 3L));
        col.Insert(new BsonDocument().Set("_id", 3).Set("mixed", 3.5));
        col.Insert(new BsonDocument().Set("_id", 4).Set("mixed", 2.25m));
        col.Insert(new BsonDocument().Set("_id", 5).Set("mixed", true));
        col.Insert(new BsonDocument().Set("_id", 6).Set("mixed", DateTime.UtcNow));
        col.Insert(new BsonDocument().Set("_id", 7).Set("mixed", "abc"));
        col.Insert(new BsonDocument().Set("_id", 8).Set("mixed", ObjectId.NewObjectId()));
        col.Insert(new BsonDocument().Set("_id", 9).Set("mixed", new BsonArray().AddValue(1)));
        col.Insert(new BsonDocument().Set("_id", 10).Set("mixed", BsonNull.Value));

        using var tx = (Transaction)_engine.BeginTransaction();
        tx.AddOperation(new TransactionOperation(
            TransactionOperationType.Delete,
            "col",
            new BsonInt32(1)));
        tx.AddOperation(new TransactionOperation(
            TransactionOperationType.Update,
            "col",
            new BsonInt32(2),
            originalDocument: null,
            newDocument: new BsonDocument().Set("_id", 2).Set("mixed", "tx-updated")));
        tx.AddOperation(new TransactionOperation(
            TransactionOperationType.Insert,
            "other_collection",
            new BsonInt32(100),
            newDocument: new BsonDocument().Set("_id", 100).Set("mixed", "ignored")));

        var shape = new QueryShape<BsonDocument>
        {
            Sort = new[] { new QuerySortField("mixed", typeof(object), false) },
            Skip = 1,
            Take = 6
        };

        var result = _executor.ExecuteShaped("col", shape, out var pushdown).ToList();

        await Assert.That(result.Count).IsEqualTo(6);
        await Assert.That(pushdown.OrderPushed).IsTrue();
        await Assert.That(pushdown.TakePushed).IsTrue();
        await Assert.That(result.Any(d => d.TryGetValue("_id", out var id) && id != null && id.ToInt32() == 2)).IsTrue();
    }

    [Test]
    public async Task ExecuteShaped_TopK_WithHighPrecisionDecimal128_ShouldPreserveOrder()
    {
        var col = _engine.GetBsonCollection("decimal_sort");
        col.Insert(new BsonDocument().Set("_id", 2).Set("amount", new BsonDecimal128(10000000000000001m)));
        col.Insert(new BsonDocument().Set("_id", 1).Set("amount", new BsonDecimal128(10000000000000000m)));

        var shape = new QueryShape<BsonDocument>
        {
            Sort = new[] { new QuerySortField("amount", typeof(decimal), false) },
            Skip = 0,
            Take = 2
        };

        var result = _executor.ExecuteShaped("decimal_sort", shape, out _).ToList();

        var ids = result.Select(doc => doc["_id"].ToInt32()).ToArray();
        await Assert.That(ids.Length).IsEqualTo(2);
        await Assert.That(ids[0]).IsEqualTo(1);
        await Assert.That(ids[1]).IsEqualTo(2);
    }

    [Test]
    public async Task QueryExecutor_InternalSortHelpers_ShouldCoverSortFieldBytesAndSortKeyFromValueBranches()
    {
        var idField = SortFieldBytes.Create("_id");
        await Assert.That(idField.Alternate).IsNotNull();
        await Assert.That(idField.SecondAlternate).IsNotNull();

        var nameField = SortFieldBytes.Create("name");
        await Assert.That(nameField.Alternate).IsNotNull();
        await Assert.That(nameField.SecondAlternate).IsNull();

        _ = SortKey.FromBsonValue(null);
        _ = SortKey.FromBsonValue(new BsonInt32(1));
        _ = SortKey.FromBsonValue(new BsonInt64(2L));
        _ = SortKey.FromBsonValue(new BsonDouble(3.14));
        _ = SortKey.FromBsonValue(new BsonDecimal128(1.5m));
        _ = SortKey.FromBsonValue(new BsonBoolean(true));
        _ = SortKey.FromBsonValue(new BsonDateTime(DateTime.UtcNow));
        _ = SortKey.FromBsonValue(new BsonString("x"));
        _ = SortKey.FromBsonValue(new BsonObjectId(ObjectId.NewObjectId()));
        _ = SortKey.FromBsonValue(new BsonArray().AddValue(1));

        var keys = QuerySortKeyReader.MaterializeKeysFromDocument(
            new BsonDocument().Set("score", 10),
            new List<QuerySortField> { new QuerySortField("score", typeof(int), false) });
        await Assert.That(keys).IsNotNull();
    }
}
