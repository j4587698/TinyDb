using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

[SkipInAot]
public sealed class QueryExecutorPrivateHelperCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryExecutorPrivateHelperCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"qe_private_cov_{Guid.NewGuid():N}.db");
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
    public async Task ExecuteShaped_OrderIndexAndTopK_ParseAndTakeBranches_ShouldBeCovered()
    {
        var col = _engine.GetCollection<OrderEntity>("ord_idx");
        _engine.GetIndexManager("ord_idx").CreateIndex("idx_a", new[] { "A" }, unique: false);

        col.Insert(new OrderEntity { Id = 1, A = 1, Name = "n1" });
        col.Insert(new OrderEntity { Id = 2, A = 2, Name = "n2" });

        var takeZero = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("A", typeof(int), false) },
            Skip = 0,
            Take = 0
        };

        var noTx = _executor.ExecuteShaped("ord_idx", takeZero, out var noTxPushdown).ToList();
        await Assert.That(noTx).IsEmpty();
        await Assert.That(noTxPushdown.TakePushed).IsTrue();

        using (var tx = _engine.BeginTransaction())
        {
            var withTx = _executor.ExecuteShaped("ord_idx", takeZero, out var txPushdown).ToList();
            await Assert.That(withTx).IsEmpty();
            await Assert.That(txPushdown.TakePushed).IsTrue();
            tx.Rollback();
        }

        var invokePredicate = CreateInvokePredicate<OrderEntity>();
        var parseFailOrder = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("A", typeof(int), false) },
            Predicate = invokePredicate,
            Take = 2
        };

        await Assert.That(() => _executor.ExecuteShaped("ord_idx", parseFailOrder, out _).ToList())
            .Throws<NotSupportedException>();

        using (var tx = _engine.BeginTransaction())
        {
            await Assert.That(() => _executor.ExecuteShaped("ord_idx", parseFailOrder, out _).ToList())
                .Throws<NotSupportedException>();
            tx.Rollback();
        }

        var parseFailTopK = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("Name", typeof(string), false) },
            Predicate = invokePredicate,
            Take = 1
        };

        await Assert.That(() => _executor.ExecuteShaped("ord_idx", parseFailTopK, out _).ToList())
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task ExecuteShaped_TopKSliceAndDocumentPath_ShouldCoverSortComparisons()
    {
        var boolCol = _engine.GetBsonCollection("topk_bool");
        boolCol.Insert(new BsonDocument().Set("_id", 1).Set("v", false));
        boolCol.Insert(new BsonDocument().Set("_id", 2).Set("v", true));

        var dtCol = _engine.GetBsonCollection("topk_dt");
        dtCol.Insert(new BsonDocument().Set("_id", 1).Set("v", new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
        dtCol.Insert(new BsonDocument().Set("_id", 2).Set("v", new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc)));

        var strAltCol = _engine.GetBsonCollection("topk_alt");
        strAltCol.Insert(new BsonDocument().Set("_id", 1).Set("Name", "b"));
        strAltCol.Insert(new BsonDocument().Set("_id", 2).Set("Name", "a"));

        var missingCol = _engine.GetBsonCollection("topk_missing");
        missingCol.Insert(new BsonDocument().Set("_id", 1).Set("x", 1));
        missingCol.Insert(new BsonDocument().Set("_id", 2).Set("x", 2));

        var numericCol = _engine.GetBsonCollection("topk_num");
        numericCol.Insert(new BsonDocument().Set("_id", 1).Set("v", 1));
        numericCol.Insert(new BsonDocument().Set("_id", 2).Set("v", 2L));

        var decCol = _engine.GetBsonCollection("topk_dec");
        decCol.Insert(new BsonDocument().Set("_id", 1).Set("v", new BsonDecimal128(Decimal128.MaxValue)));
        decCol.Insert(new BsonDocument().Set("_id", 2).Set("v", new BsonArray().AddValue(1)));

        var idTypeCol = _engine.GetBsonCollection("topk_id_types");
        idTypeCol.Insert(new BsonDocument().Set("_id", new BsonInt64(100)).Set("v", 700));
        idTypeCol.Insert(new BsonDocument().Set("_id", new BsonDouble(101.5)).Set("v", 600));
        idTypeCol.Insert(new BsonDocument().Set("_id", new BsonBoolean(true)).Set("v", 500));
        idTypeCol.Insert(new BsonDocument().Set("_id", new BsonString("id-4")).Set("v", 400));
        idTypeCol.Insert(new BsonDocument().Set("_id", new BsonObjectId(ObjectId.NewObjectId())).Set("v", 300));
        idTypeCol.Insert(new BsonDocument().Set("_id", BsonNull.Value).Set("v", 200));
        idTypeCol.Insert(new BsonDocument().Set("_id", new BsonArray().AddValue(1)).Set("v", 100));

        var topKShape = new QueryShape<BsonDocument>
        {
            Sort = new[] { new QuerySortField("v", typeof(object), false) },
            Skip = 0,
            Take = 1
        };

        var boolRes = _executor.ExecuteShaped("topk_bool", topKShape, out _).ToList();
        var dtRes = _executor.ExecuteShaped("topk_dt", topKShape, out _).ToList();
        var numRes = _executor.ExecuteShaped("topk_num", topKShape, out _).ToList();
        var decRes = _executor.ExecuteShaped("topk_dec", topKShape, out _).ToList();
        var idTypeRes = _executor.ExecuteShaped("topk_id_types", topKShape, out _).ToList();
        await Assert.That(boolRes.Count).IsEqualTo(1);
        await Assert.That(dtRes.Count).IsEqualTo(1);
        await Assert.That(numRes.Count).IsEqualTo(1);
        await Assert.That(decRes.Count).IsEqualTo(1);
        await Assert.That(idTypeRes.Count).IsEqualTo(1);

        var altShape = new QueryShape<BsonDocument>
        {
            Sort = new[] { new QuerySortField("name", typeof(string), false) },
            Skip = 0,
            Take = 1
        };
        var altRes = _executor.ExecuteShaped("topk_alt", altShape, out _).ToList();
        await Assert.That(altRes.Count).IsEqualTo(1);

        var missingShape = new QueryShape<BsonDocument>
        {
            Sort = new[] { new QuerySortField("v", typeof(object), false) },
            Skip = 0,
            Take = 1
        };
        var missingRes = _executor.ExecuteShaped("topk_missing", missingShape, out _).ToList();
        await Assert.That(missingRes.Count).IsEqualTo(1);

        var typedCol = _engine.GetCollection<OrderEntity>("topk_doccmp");
        typedCol.Insert(new OrderEntity { Id = 10, A = 10, Name = "name10" });
        typedCol.Insert(new OrderEntity { Id = 20, A = 20, Name = "name20" });

        var docCmpAsc = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("A", typeof(int), false) },
            Predicate = x => x.A >= 0 && x.Name.StartsWith("name"),
            Skip = 0,
            Take = 1
        };
        var docCmpDesc = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("A", typeof(int), true) },
            Predicate = x => x.A >= 0 && x.Name.StartsWith("name"),
            Skip = 0,
            Take = 1
        };

        var asc = _executor.ExecuteShaped("topk_doccmp", docCmpAsc, out _).ToList();
        var desc = _executor.ExecuteShaped("topk_doccmp", docCmpDesc, out _).ToList();
        await Assert.That(asc.Count).IsEqualTo(1);
        await Assert.That(desc.Count).IsEqualTo(1);
    }

    [Test]
    public async Task PrivateOrderAndTopKMethods_ShouldCoverResidualBranches()
    {
        var col = _engine.GetCollection<OrderEntity>("ord_direct");
        _engine.GetIndexManager("ord_direct").CreateIndex("idx_a", new[] { "A" }, unique: false);

        col.Insert(new OrderEntity { Id = 1, A = 1, Name = "x1" });
        col.Insert(new OrderEntity { Id = 2, A = 2, Name = "x2" });

        var idx = _engine.GetIndexManager("ord_direct").GetIndex("idx_a");
        await Assert.That(idx).IsNotNull();

        var takeZeroShape = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("A", typeof(int), false) },
            Take = 0
        };

        var byOrderTakeZero = _executor.ExecuteByOrderIndex<OrderEntity>("ord_direct", takeZeroShape, idx!, descending: false, out _).ToList();
        await Assert.That(byOrderTakeZero).IsEmpty();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            var byOrderTxTakeZero = _executor.ExecuteByOrderIndexWithTransaction<OrderEntity>("ord_direct", takeZeroShape, idx, descending: false, tx, out _).ToList();
            await Assert.That(byOrderTxTakeZero).IsEmpty();
            tx.Rollback();
        }

        var invokePredicate = CreateInvokePredicate<OrderEntity>();
        var parseFailShape = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("A", typeof(int), false) },
            Predicate = invokePredicate,
            Take = 1
        };

        await Assert.That(() => _executor.ExecuteByOrderIndex<OrderEntity>("ord_direct", parseFailShape, idx, descending: false, out _).ToList())
            .Throws<NotSupportedException>();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            await Assert.That(() => _executor.ExecuteByOrderIndexWithTransaction<OrderEntity>("ord_direct", parseFailShape, idx, descending: false, tx, out _).ToList())
                .Throws<NotSupportedException>();
            tx.Rollback();
        }

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            tx.AddOperation(new TransactionOperation(
                TransactionOperationType.Insert,
                "ord_direct",
                new BsonInt32(20),
                newDocument: new BsonDocument().Set("_id", 20).Set("A", 20).Set("Name", "tx")));

            var txRowsFilteredOut = new QueryShape<OrderEntity>
            {
                Sort = new[] { new QuerySortField("A", typeof(int), false) },
                Predicate = x => x.A < 0,
                Take = 10
            };

            var filtered = _executor.ExecuteByOrderIndexWithTransaction<OrderEntity>("ord_direct", txRowsFilteredOut, idx, descending: false, tx, out _).ToList();
            await Assert.That(filtered).IsEmpty();
            tx.Rollback();
        }

        var topKEmptyShape = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("A", typeof(int), false) },
            Take = 0
        };
        var topKEmpty = _executor.ExecuteTopKScan<OrderEntity>("ord_direct", topKEmptyShape, out var topKPushdown).ToList();
        await Assert.That(topKEmpty).IsEmpty();
        await Assert.That(topKPushdown.TakePushed).IsTrue();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            tx.AddOperation(new TransactionOperation(TransactionOperationType.Insert, "other_col", new BsonInt32(99), newDocument: new BsonDocument().Set("_id", 99)));
            var fullScan = _executor.ExecuteFullTableScan<OrderEntity>("ord_direct", x => x.A > 0).ToList();
            await Assert.That(fullScan.Count > 0).IsTrue();
            tx.Rollback();
        }
    }

    [Test]
    public async Task ExecuteByOrderIndexWithTransaction_ShouldCoverIteratorSkipAndTakeBranches()
    {
        var col = _engine.GetCollection<OrderEntity>("ord_iter_cov");
        _engine.GetIndexManager("ord_iter_cov").CreateIndex("idx_a", new[] { "A" }, unique: false);
        col.Insert(new OrderEntity { Id = 10, A = 10, Name = "base" });

        var idx = _engine.GetIndexManager("ord_iter_cov").GetIndex("idx_a");
        await Assert.That(idx).IsNotNull();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            tx.AddOperation(new TransactionOperation(
                TransactionOperationType.Insert,
                "ord_iter_cov",
                new BsonInt32(100),
                newDocument: new BsonDocument().Set("_id", 100).Set("_collection", "ord_iter_cov").Set("A", 5).Set("Name", "tx-before")));
            tx.AddOperation(new TransactionOperation(
                TransactionOperationType.Insert,
                "ord_iter_cov",
                new BsonInt32(200),
                newDocument: new BsonDocument().Set("_id", 200).Set("_collection", "ord_iter_cov").Set("A", 20).Set("Name", "tx-after")));

            var shape = new QueryShape<OrderEntity>
            {
                Sort = new[] { new QuerySortField("A", typeof(int), false) },
                Skip = 0,
                Take = 3
            };

            var result = _executor.ExecuteByOrderIndexWithTransaction<OrderEntity>("ord_iter_cov", shape, idx!, descending: false, tx, out _).ToList();
            await Assert.That(result.Count > 0).IsTrue();
            tx.Rollback();
        }

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            var skipBaseShape = new QueryShape<OrderEntity>
            {
                Sort = new[] { new QuerySortField("A", typeof(int), false) },
                Skip = 1,
                Take = 2
            };

            _ = _executor.ExecuteByOrderIndexWithTransaction<OrderEntity>("ord_iter_cov", skipBaseShape, idx, descending: false, tx, out _).ToList();
            tx.Rollback();
        }

        var txOnly = _engine.GetCollection<OrderEntity>("ord_tx_only_cov");
        _engine.GetIndexManager("ord_tx_only_cov").CreateIndex("idx_a", new[] { "A" }, unique: false);
        _ = txOnly;
        var txOnlyIndex = _engine.GetIndexManager("ord_tx_only_cov").GetIndex("idx_a");
        await Assert.That(txOnlyIndex).IsNotNull();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            tx.AddOperation(new TransactionOperation(
                TransactionOperationType.Insert,
                "ord_tx_only_cov",
                new BsonInt32(1),
                newDocument: new BsonDocument().Set("_id", 1).Set("_collection", "ord_tx_only_cov").Set("A", 1).Set("Name", "tail-1")));
            tx.AddOperation(new TransactionOperation(
                TransactionOperationType.Insert,
                "ord_tx_only_cov",
                new BsonInt32(2),
                newDocument: new BsonDocument().Set("_id", 2).Set("_collection", "ord_tx_only_cov").Set("A", 2).Set("Name", "tail-2")));

            var txOnlyShape = new QueryShape<OrderEntity>
            {
                Sort = new[] { new QuerySortField("A", typeof(int), false) },
                Skip = 1,
                Take = 3
            };

            var txOnlyResult = _executor.ExecuteByOrderIndexWithTransaction<OrderEntity>("ord_tx_only_cov", txOnlyShape, txOnlyIndex!, descending: false, tx, out _).ToList();
            await Assert.That(txOnlyResult.Count).IsEqualTo(1);
            tx.Rollback();
        }
    }

    [Test]
    public async Task ExecuteTopKScan_WithTransactionOverlayAndLargeDocumentStub_ShouldCoverPredicateBranches()
    {
        var col = _engine.GetCollection<OrderEntity>("topk_overlay_cov");
        col.Insert(new OrderEntity { Id = 1, A = 1, Name = "small" });
        col.Insert(new OrderEntity { Id = 2, A = 2, Name = new string('x', 30000) });

        using var tx = (Transaction)_engine.BeginTransaction();
        tx.AddOperation(new TransactionOperation(
            TransactionOperationType.Insert,
            "topk_overlay_cov",
            new BsonInt32(3),
            newDocument: new BsonDocument()
                .Set("_id", 3)
                .Set("_collection", "topk_overlay_cov")
                .Set("A", 3)
                .Set("Name", "tx-doc")));

        var shape = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("A", typeof(int), false) },
            Predicate = x => x.Id > 0,
            Skip = 0,
            Take = 2
        };

        var result = _executor.ExecuteTopKScan<OrderEntity>("topk_overlay_cov", shape, out _).ToList();
        await Assert.That(result.Count > 0).IsTrue();

        tx.Rollback();
    }

    [Test]
    public async Task ExecuteTopKScan_WithFalseLargeDocumentFlag_ShouldHitStubCheckReturnLine()
    {
        var col = _engine.GetBsonCollection("topk_stub_flag_false_cov");
        col.Insert(new BsonDocument()
            .Set("_id", 1)
            .Set("_collection", "topk_stub_flag_false_cov")
            .Set("A", 1)
            .Set("Name", "n1")
            .Set("_isLargeDocument", false));
        col.Insert(new BsonDocument()
            .Set("_id", 2)
            .Set("_collection", "topk_stub_flag_false_cov")
            .Set("A", 2)
            .Set("Name", "n2"));

        var shape = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("A", typeof(int), false) },
            Predicate = x => x.A >= 0,
            Skip = 0,
            Take = 2
        };

        var result = _executor.ExecuteTopKScan<OrderEntity>("topk_stub_flag_false_cov", shape, out _).ToList();
        await Assert.That(result.Count).IsEqualTo(2);
    }

    [Test]
    public async Task PrivateHelpers_ShouldCoverConversionsCollectionsTransactionsAndSortBranches()
    {
        await Assert.That(QueryExecutor.ReverseComparisonOperator(ExpressionType.GreaterThan)).IsEqualTo(ExpressionType.LessThan);
        await Assert.That(QueryExecutor.ReverseComparisonOperator(ExpressionType.GreaterThanOrEqual)).IsEqualTo(ExpressionType.LessThanOrEqual);
        await Assert.That(QueryExecutor.ReverseComparisonOperator(ExpressionType.LessThan)).IsEqualTo(ExpressionType.GreaterThan);
        await Assert.That(QueryExecutor.ReverseComparisonOperator(ExpressionType.LessThanOrEqual)).IsEqualTo(ExpressionType.GreaterThanOrEqual);
        await Assert.That(QueryExecutor.ReverseComparisonOperator(ExpressionType.Equal)).IsEqualTo(ExpressionType.Equal);

        var ok = QueryExecutor.TryConvertConstant(new TinyDb.Query.ConstantExpression("123"), typeof(int), out var converted);
        await Assert.That(ok).IsTrue();
        await Assert.That(converted.Value).IsEqualTo(123);

        var formatFail = QueryExecutor.TryConvertConstant(new TinyDb.Query.ConstantExpression("not-a-number"), typeof(int), out _);
        await Assert.That(formatFail).IsFalse();

        var overflowFail = QueryExecutor.TryConvertConstant(new TinyDb.Query.ConstantExpression(999), typeof(byte), out _);
        await Assert.That(overflowFail).IsFalse();

        var castFail = QueryExecutor.TryConvertConstant(new TinyDb.Query.ConstantExpression(new object()), typeof(DateTime), out _);
        await Assert.That(castFail).IsFalse();

        await Assert.That(() => QueryExecutor.DeserializeDocumentOrThrow(new byte[] { 1, 2, 3 }))
            .Throws<InvalidOperationException>();

        var noCollectionField = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1));
        var wrongCollectionType = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1).Set("_collection", 123));
        var mismatchCollection = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1).Set("_collection", "other"));

        var collectionFieldName = Encoding.UTF8.GetBytes("_collection");
        var expectedCollection = Encoding.UTF8.GetBytes("target");

        await Assert.That(QueryExecutor.MatchesCollection(noCollectionField, collectionFieldName, expectedCollection)).IsTrue();
        await Assert.That(QueryExecutor.MatchesCollection(wrongCollectionType, collectionFieldName, expectedCollection)).IsFalse();
        await Assert.That(QueryExecutor.MatchesCollection(mismatchCollection, collectionFieldName, expectedCollection)).IsFalse();

        var malformedLength = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1).Set("_collection", "target"));
        var located = BsonScanner.TryLocateField(malformedLength, collectionFieldName, out var valueOffset, out _);
        await Assert.That(located).IsTrue();
        BinaryPrimitives.WriteInt32LittleEndian(malformedLength.AsSpan(valueOffset, 4), 0);
        await Assert.That(QueryExecutor.MatchesCollection(malformedLength, collectionFieldName, expectedCollection)).IsFalse();

        var oversizedLength = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1).Set("_collection", "target"));
        BsonScanner.TryLocateField(oversizedLength, collectionFieldName, out var oversizedOffset, out _);
        BinaryPrimitives.WriteInt32LittleEndian(oversizedLength.AsSpan(oversizedOffset, 4), 1000);
        await Assert.That(QueryExecutor.MatchesCollection(oversizedLength, collectionFieldName, expectedCollection)).IsFalse();

        var shortRead = QuerySortKeyReader.ReadString(new byte[] { 1, 2, 3 }, 0);
        await Assert.That(shortRead is null).IsTrue();

        var readStringBytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("s", "abc"));
        BsonScanner.TryLocateField(readStringBytes, Encoding.UTF8.GetBytes("s"), out var stringOffset, out _);
        var parsed = QuerySortKeyReader.ReadString(readStringBytes, stringOffset);
        await Assert.That(parsed).IsNotNull();
        await Assert.That(parsed!.Value).IsEqualTo("abc");

        BinaryPrimitives.WriteInt32LittleEndian(readStringBytes.AsSpan(stringOffset, 4), 0);
        var zeroLengthRead = QuerySortKeyReader.ReadString(readStringBytes, stringOffset);
        await Assert.That(zeroLengthRead is null).IsTrue();

        var sortDoc = new BsonDocument().Set("Name", "Alice").Set("id", 99);
        var nameValue = QuerySortKeyReader.TryGetSortValue(sortDoc, "name");
        var idValue = QuerySortKeyReader.TryGetSortValue(sortDoc, "_id");
        var idPascalOnlyValue = QuerySortKeyReader.TryGetSortValue(new BsonDocument().Set("Id", 100), "_id");
        var noneValue = QuerySortKeyReader.TryGetSortValue(sortDoc, "notExists");
        await Assert.That(nameValue!.ToString()).IsEqualTo("Alice");
        await Assert.That(idValue!.ToInt32()).IsEqualTo(99);
        await Assert.That(idPascalOnlyValue!.ToInt32()).IsEqualTo(100);
        await Assert.That(noneValue).IsNull();

        var member = new TinyDb.Query.MemberExpression("A", new TinyDb.Query.ParameterExpression("x"));

        var case1 = new TinyDb.Query.BinaryExpression(ExpressionType.Equal, member, new TinyDb.Query.ConstantExpression(7));
        var case1Result = _executor.ExtractComparison(case1);
        await Assert.That(case1Result.member).IsNotNull();
        await Assert.That(case1Result.constant).IsNotNull();
        await Assert.That(case1Result.op).IsEqualTo(ExpressionType.Equal);

        var case2 = new TinyDb.Query.BinaryExpression(
            ExpressionType.GreaterThan,
            member,
            new TinyDb.Query.UnaryExpression(ExpressionType.Convert, new TinyDb.Query.ConstantExpression("8"), typeof(int)));
        var case2Result = _executor.ExtractComparison(case2);
        await Assert.That(case2Result.member).IsNotNull();
        await Assert.That(case2Result.constant).IsNotNull();
        await Assert.That(case2Result.constant!.Value).IsEqualTo(8);
        await Assert.That(case2Result.op).IsEqualTo(ExpressionType.GreaterThan);

        var case2Fail = new TinyDb.Query.BinaryExpression(
            ExpressionType.Equal,
            member,
            new TinyDb.Query.UnaryExpression(ExpressionType.Convert, new TinyDb.Query.ConstantExpression("bad"), typeof(int)));
        var case2FailResult = _executor.ExtractComparison(case2Fail);
        await Assert.That(case2FailResult.member).IsNull();
        await Assert.That(case2FailResult.constant).IsNull();

        var case3 = new TinyDb.Query.BinaryExpression(ExpressionType.GreaterThan, new TinyDb.Query.ConstantExpression(5), member);
        var case3Result = _executor.ExtractComparison(case3);
        await Assert.That(case3Result.member).IsNotNull();
        await Assert.That(case3Result.constant).IsNotNull();
        await Assert.That(case3Result.op).IsEqualTo(ExpressionType.LessThan);

        var case4 = new TinyDb.Query.BinaryExpression(
            ExpressionType.GreaterThanOrEqual,
            new TinyDb.Query.UnaryExpression(ExpressionType.Convert, new TinyDb.Query.ConstantExpression("10"), typeof(int)),
            member);
        var case4Result = _executor.ExtractComparison(case4);
        await Assert.That(case4Result.member).IsNotNull();
        await Assert.That(case4Result.constant).IsNotNull();
        await Assert.That(case4Result.op).IsEqualTo(ExpressionType.LessThanOrEqual);

        var case4Fail = new TinyDb.Query.BinaryExpression(
            ExpressionType.GreaterThanOrEqual,
            new TinyDb.Query.UnaryExpression(ExpressionType.Convert, new TinyDb.Query.ConstantExpression(999), typeof(byte)),
            member);
        var case4FailResult = _executor.ExtractComparison(case4Fail);
        await Assert.That(case4FailResult.member).IsNull();
        await Assert.That(case4FailResult.constant).IsNull();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            var doc2 = new BsonDocument().Set("_id", 2).Set("A", 20);

            tx.AddOperation(new TransactionOperation(TransactionOperationType.Delete, "tx_col", new BsonInt32(1)));
            tx.AddOperation(new TransactionOperation(TransactionOperationType.Update, "tx_col", new BsonInt32(2), newDocument: doc2));
            tx.AddOperation(new TransactionOperation(TransactionOperationType.Update, "tx_col", new BsonInt32(3), newDocument: null));
            tx.AddOperation(new TransactionOperation(TransactionOperationType.Insert, "other_col", new BsonInt32(9), newDocument: new BsonDocument().Set("_id", 9)));

            var foundDelete = QueryTransactionOverlay.TryGetDocument(tx, "tx_col", new BsonInt32(1), out var deleteDoc);
            var foundUpdate = QueryTransactionOverlay.TryGetDocument(tx, "tx_col", new BsonInt32(2), out var updateDoc);
            var foundNullUpdate = QueryTransactionOverlay.TryGetDocument(tx, "tx_col", new BsonInt32(3), out var nullUpdateDoc);
            var foundMissing = QueryTransactionOverlay.TryGetDocument(tx, "tx_col", new BsonInt32(404), out _);

            await Assert.That(foundDelete).IsTrue();
            await Assert.That(deleteDoc).IsNull();
            await Assert.That(foundUpdate).IsTrue();
            await Assert.That(updateDoc).IsNotNull();
            await Assert.That(foundNullUpdate).IsTrue();
            await Assert.That(nullUpdateDoc).IsNull();
            await Assert.That(foundMissing).IsFalse();

            var overlay = QueryTransactionOverlay.Build(tx, "tx_col");
            await Assert.That(overlay).IsNotNull();
            await Assert.That(overlay!.Count).IsEqualTo(2);
            await Assert.That(overlay[new BsonInt32(1)]).IsNull();
            await Assert.That(overlay[new BsonInt32(2)]).IsNotNull();

            tx.Rollback();
        }

        using (var emptyTx = (Transaction)_engine.BeginTransaction())
        {
            var nullOverlay = QueryTransactionOverlay.Build(emptyTx, "missing");
            await Assert.That(nullOverlay).IsNull();
            emptyTx.Rollback();
        }

        var idxCol = _engine.GetBsonCollection("idx_build");
        idxCol.Insert(new BsonDocument().Set("_id", 1).Set("A", 1).Set("B", 2));
        _engine.GetIndexManager("idx_build").CreateIndex("idx_ab", new[] { "A", "B" }, unique: false);
        var idx = _engine.GetIndexManager("idx_build").GetIndex("idx_ab");
        await Assert.That(idx).IsNotNull();

        var keyFull = OrderIndexTransactionRows.BuildKey(idx!, new BsonDocument().Set("A", 9).Set("a", 9).Set("B", 8).Set("b", 8));
        await Assert.That(keyFull.Length).IsEqualTo(2);

        var keyMissing = OrderIndexTransactionRows.BuildKey(idx, new BsonDocument().Set("A", 9).Set("a", 9));
        await Assert.That(keyMissing.Length).IsEqualTo(2);
        await Assert.That(keyMissing.Values[1].IsNull).IsTrue();

        var sharedKey = new IndexKey(new BsonInt32(1), new BsonInt32(1));
        var rowA = new OrderIndexRow(new BsonInt32(1), sharedKey, new BsonDocument().Set("_id", 1));
        var rowB = new OrderIndexRow(new BsonInt32(2), sharedKey, new BsonDocument().Set("_id", 2));

        var cmpAsc = OrderIndexTransactionRows.Compare(rowA, rowB, descending: false);
        var cmpDesc = OrderIndexTransactionRows.Compare(rowA, rowB, descending: true);
        await Assert.That(cmpAsc < 0).IsTrue();
        await Assert.That(cmpDesc > 0).IsTrue();

        var cmpToBaseAsc = OrderIndexTransactionRows.CompareToBase(rowA, sharedKey, new BsonInt32(2), descending: false);
        var cmpToBaseDesc = OrderIndexTransactionRows.CompareToBase(rowA, sharedKey, new BsonInt32(2), descending: true);
        await Assert.That(cmpToBaseAsc).IsEqualTo(0);
        await Assert.That(cmpToBaseDesc).IsEqualTo(0);

        var boolKeyFalse = SortKey.FromBsonValue(new BsonBoolean(false));
        var boolKeyTrue = SortKey.FromBsonValue(new BsonBoolean(true));
        var dateKey1 = SortKey.FromBsonValue(new BsonDateTime(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
        var dateKey2 = SortKey.FromBsonValue(new BsonDateTime(new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc)));
        var strKeyA = SortKey.FromBsonValue(new BsonString("a"));
        var strKeyB = SortKey.FromBsonValue(new BsonString("b"));

        _ = SortKey.Compare(boolKeyTrue, boolKeyFalse);
        _ = SortKey.Compare(dateKey1, dateKey2);
        _ = SortKey.Compare(strKeyA, strKeyB);
        _ = SortKey.Compare(boolKeyTrue, dateKey1);

        var descendingSort = (IReadOnlyList<QuerySortField>)new List<QuerySortField> { new QuerySortField("A", typeof(int), true) };
        var row = new TopKRow(
            new BsonInt32(1),
            new[] { SortKey.FromBsonValue(new BsonInt32(1)) },
            0L,
            null);
        _ = QuerySortKeyReader.CompareDocumentToRow(new BsonDocument().Set("A", 2), row, 1L, descendingSort);

        var maxDecimalKey = SortKey.FromBsonValue(new BsonDecimal128(Decimal128.MaxValue));
        await Assert.That(maxDecimalKey.Type).IsEqualTo(BsonType.Decimal128);
    }

    [Test]
    public async Task PrivateReadersAndSortKeyFallbacks_ShouldCoverRemainingBranches()
    {
        var noIdBytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("x", 1));
        var noIdResult = QuerySortKeyReader.TryReadBsonValue(noIdBytes, SortFieldBytes.Id, out _);
        await Assert.That(noIdResult).IsFalse();

        var nullIdBytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", BsonNull.Value));
        var nullIdResult = QuerySortKeyReader.TryReadBsonValue(nullIdBytes, SortFieldBytes.Id, out _);
        await Assert.That(nullIdResult).IsTrue();

        var malformedObjectIdBytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", new BsonObjectId(ObjectId.NewObjectId())));
        Array.Resize(ref malformedObjectIdBytes, malformedObjectIdBytes.Length - 5);
        var malformedObjectIdResult = QuerySortKeyReader.TryReadBsonValue(malformedObjectIdBytes, SortFieldBytes.Id, out _);
        await Assert.That(malformedObjectIdResult).IsFalse();

        var sortKeyRefResult = TryReadSortKeyRef(new byte[] { 1, 2, 3 }, 32, BsonType.String);
        await Assert.That(sortKeyRefResult).IsFalse();

        await Assert.That(MaterializeUnsupportedSortKeyRef()).IsTrue();

        var compareResult = CompareSortKeyRefToSortKey();
        await Assert.That(compareResult).IsNotEqualTo(0);
    }

    private static bool TryReadSortKeyRef(byte[] bytes, int valueOffset, BsonType type)
    {
        return SortKeyRef.TryRead(bytes, valueOffset, type, out _);
    }

    private static bool MaterializeUnsupportedSortKeyRef()
    {
        var keyRef = new SortKeyRef(BsonType.Array, 0, 0, default, default(ReadOnlySpan<byte>));
        _ = SortKey.Materialize(keyRef);
        return true;
    }

    private static int CompareSortKeyRefToSortKey()
    {
        var keyRef = new SortKeyRef(BsonType.Int32, 1d, 0L, default, default(ReadOnlySpan<byte>));
        var sortKey = SortKey.FromBsonValue(new BsonString("s"));
        return SortKey.Compare(keyRef, sortKey);
    }

    private static Expression<Func<T, bool>> CreateInvokePredicate<T>()
    {
        var p = System.Linq.Expressions.Expression.Parameter(typeof(T), "x");
        var invoked = System.Linq.Expressions.Expression.Invoke(
            System.Linq.Expressions.Expression.Constant((Func<bool>)(() => true)));
        return System.Linq.Expressions.Expression.Lambda<Func<T, bool>>(invoked, p);
    }

    [Entity]
    public sealed class OrderEntity
    {
        public int Id { get; set; }
        public int A { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
