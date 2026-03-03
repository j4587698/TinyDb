using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
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
        tx.Operations.Add(new TransactionOperation(
            TransactionOperationType.Delete,
            "col",
            new BsonInt32(1)));
        tx.Operations.Add(new TransactionOperation(
            TransactionOperationType.Update,
            "col",
            new BsonInt32(2),
            originalDocument: null,
            newDocument: new BsonDocument().Set("_id", 2).Set("mixed", "tx-updated")));
        tx.Operations.Add(new TransactionOperation(
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
    public async Task QueryExecutor_InternalSortHelpers_ShouldCoverSortFieldBytesAndSortKeyFromValueBranches()
    {
        var qeType = typeof(QueryExecutor);

        var sortFieldType = qeType.GetNestedType("SortFieldBytes", BindingFlags.NonPublic);
        await Assert.That(sortFieldType).IsNotNull();
        var create = sortFieldType!.GetMethod("Create", BindingFlags.Public | BindingFlags.Static);
        await Assert.That(create).IsNotNull();

        var idField = create!.Invoke(null, new object[] { "_id" });
        var altProp = sortFieldType.GetProperty("Alternate", BindingFlags.Public | BindingFlags.Instance);
        var secondAltProp = sortFieldType.GetProperty("SecondAlternate", BindingFlags.Public | BindingFlags.Instance);
        await Assert.That(altProp).IsNotNull();
        await Assert.That(secondAltProp).IsNotNull();
        await Assert.That((byte[]?)altProp!.GetValue(idField)).IsNotNull();
        await Assert.That((byte[]?)secondAltProp!.GetValue(idField)).IsNotNull();

        var nameField = create.Invoke(null, new object[] { "name" });
        await Assert.That((byte[]?)altProp.GetValue(nameField)).IsNotNull();
        await Assert.That((byte[]?)secondAltProp.GetValue(nameField)).IsNull();

        var sortKeyType = qeType.GetNestedType("SortKey", BindingFlags.NonPublic);
        await Assert.That(sortKeyType).IsNotNull();
        var fromBsonValue = sortKeyType!.GetMethod("FromBsonValue", BindingFlags.Public | BindingFlags.Static);
        await Assert.That(fromBsonValue).IsNotNull();

        _ = fromBsonValue!.Invoke(null, new object?[] { null });
        _ = fromBsonValue.Invoke(null, new object?[] { new BsonInt32(1) });
        _ = fromBsonValue.Invoke(null, new object?[] { new BsonInt64(2L) });
        _ = fromBsonValue.Invoke(null, new object?[] { new BsonDouble(3.14) });
        _ = fromBsonValue.Invoke(null, new object?[] { new BsonDecimal128(1.5m) });
        _ = fromBsonValue.Invoke(null, new object?[] { new BsonBoolean(true) });
        _ = fromBsonValue.Invoke(null, new object?[] { new BsonDateTime(DateTime.UtcNow) });
        _ = fromBsonValue.Invoke(null, new object?[] { new BsonString("x") });
        _ = fromBsonValue.Invoke(null, new object?[] { new BsonObjectId(ObjectId.NewObjectId()) });
        _ = fromBsonValue.Invoke(null, new object?[] { new BsonArray().AddValue(1) });

        var materializeDoc = qeType.GetMethod("MaterializeKeysFromDocument", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(materializeDoc).IsNotNull();
        var keys = materializeDoc!.Invoke(null, new object[]
        {
            new BsonDocument().Set("score", 10),
            new List<QuerySortField> { new QuerySortField("score", typeof(int), false) }
        });
        await Assert.That(keys).IsNotNull();
    }
}
