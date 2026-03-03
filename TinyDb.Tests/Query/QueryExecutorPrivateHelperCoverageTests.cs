using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
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

    private delegate ExpressionType ReverseComparisonOperatorDelegate(ExpressionType op);
    private delegate bool TryConvertConstantDelegate(TinyDb.Query.ConstantExpression constant, Type targetType, out TinyDb.Query.ConstantExpression converted);
    private delegate BsonDocument DeserializeDocumentOrThrowDelegate(ReadOnlyMemory<byte> slice);
    private delegate bool TryGetTransactionDocumentDelegate(Transaction tx, string collectionName, BsonValue id, out BsonDocument? document);
    private delegate Dictionary<BsonValue, BsonDocument?>? BuildTransactionOverlayDelegate(Transaction tx, string collectionName);
    private delegate IndexKey BuildIndexKeyForOrderDelegate(BTreeIndex index, BsonDocument doc);
    private delegate bool MatchesCollectionDelegate(ReadOnlySpan<byte> document, byte[] collectionFieldNameBytes, byte[] collectionNameBytes);
    private delegate BsonString? ReadStringDelegate(ReadOnlySpan<byte> document, int valueOffset);
    private delegate BsonValue? TryGetSortValueDelegate(BsonDocument doc, string fieldName);
    private delegate (TinyDb.Query.MemberExpression? member, TinyDb.Query.ConstantExpression? constant, ExpressionType op) ExtractComparisonDelegate(
        QueryExecutor executor,
        TinyDb.Query.BinaryExpression binary);
    private delegate bool TryConvertDecimal128ToDoubleDelegate(Decimal128 value, out double converted);

    private static readonly ReverseComparisonOperatorDelegate ReverseComparisonOperator = CreateStaticDelegate<ReverseComparisonOperatorDelegate>("ReverseComparisonOperator");
    private static readonly TryConvertConstantDelegate TryConvertConstant = CreateStaticDelegate<TryConvertConstantDelegate>("TryConvertConstant");
    private static readonly DeserializeDocumentOrThrowDelegate DeserializeDocumentOrThrow = CreateStaticDelegate<DeserializeDocumentOrThrowDelegate>("DeserializeDocumentOrThrow");
    private static readonly TryGetTransactionDocumentDelegate TryGetTransactionDocument = CreateStaticDelegate<TryGetTransactionDocumentDelegate>("TryGetTransactionDocument");
    private static readonly BuildTransactionOverlayDelegate BuildTransactionOverlay = CreateStaticDelegate<BuildTransactionOverlayDelegate>("BuildTransactionOverlay");
    private static readonly BuildIndexKeyForOrderDelegate BuildIndexKeyForOrder = CreateStaticDelegate<BuildIndexKeyForOrderDelegate>("BuildIndexKeyForOrder");
    private static readonly MatchesCollectionDelegate MatchesCollection = CreateStaticDelegate<MatchesCollectionDelegate>("MatchesCollection");
    private static readonly ReadStringDelegate ReadString = CreateStaticDelegate<ReadStringDelegate>("ReadString");
    private static readonly TryGetSortValueDelegate TryGetSortValue = CreateStaticDelegate<TryGetSortValueDelegate>("TryGetSortValue");
    private static readonly ExtractComparisonDelegate ExtractComparison = CreateInstanceDelegate<ExtractComparisonDelegate>("ExtractComparison");
    private static readonly TryConvertDecimal128ToDoubleDelegate TryConvertDecimal128ToDouble = CreateStaticDelegate<TryConvertDecimal128ToDoubleDelegate>("TryConvertDecimal128ToDouble");

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

        var byOrderTakeZero = InvokeExecuteByOrderIndex("ord_direct", takeZeroShape, idx!, descending: false, out _).ToList();
        await Assert.That(byOrderTakeZero).IsEmpty();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            var byOrderTxTakeZero = InvokeExecuteByOrderIndexWithTransaction("ord_direct", takeZeroShape, idx, descending: false, tx, out _).ToList();
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

        await Assert.That(() => InvokeExecuteByOrderIndex("ord_direct", parseFailShape, idx, descending: false, out _).ToList())
            .Throws<NotSupportedException>();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            await Assert.That(() => InvokeExecuteByOrderIndexWithTransaction("ord_direct", parseFailShape, idx, descending: false, tx, out _).ToList())
                .Throws<NotSupportedException>();
            tx.Rollback();
        }

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            tx.Operations.Add(new TransactionOperation(
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

            var filtered = InvokeExecuteByOrderIndexWithTransaction("ord_direct", txRowsFilteredOut, idx, descending: false, tx, out _).ToList();
            await Assert.That(filtered).IsEmpty();
            tx.Rollback();
        }

        var topKEmptyShape = new QueryShape<OrderEntity>
        {
            Sort = new[] { new QuerySortField("A", typeof(int), false) },
            Take = 0
        };
        var topKEmpty = InvokeExecuteTopKScan("ord_direct", topKEmptyShape, out var topKPushdown).ToList();
        await Assert.That(topKEmpty).IsEmpty();
        await Assert.That(topKPushdown.TakePushed).IsTrue();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            tx.Operations.Add(new TransactionOperation(TransactionOperationType.Insert, "other_col", new BsonInt32(99), newDocument: new BsonDocument().Set("_id", 99)));
            var fullScan = InvokeExecuteFullTableScan("ord_direct", x => x.A > 0).ToList();
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
            tx.Operations.Add(new TransactionOperation(
                TransactionOperationType.Insert,
                "ord_iter_cov",
                new BsonInt32(100),
                newDocument: new BsonDocument().Set("_id", 100).Set("_collection", "ord_iter_cov").Set("A", 5).Set("Name", "tx-before")));
            tx.Operations.Add(new TransactionOperation(
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

            var result = InvokeExecuteByOrderIndexWithTransaction("ord_iter_cov", shape, idx!, descending: false, tx, out _).ToList();
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

            _ = InvokeExecuteByOrderIndexWithTransaction("ord_iter_cov", skipBaseShape, idx, descending: false, tx, out _).ToList();
            tx.Rollback();
        }

        var txOnly = _engine.GetCollection<OrderEntity>("ord_tx_only_cov");
        _engine.GetIndexManager("ord_tx_only_cov").CreateIndex("idx_a", new[] { "A" }, unique: false);
        _ = txOnly;
        var txOnlyIndex = _engine.GetIndexManager("ord_tx_only_cov").GetIndex("idx_a");
        await Assert.That(txOnlyIndex).IsNotNull();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            tx.Operations.Add(new TransactionOperation(
                TransactionOperationType.Insert,
                "ord_tx_only_cov",
                new BsonInt32(1),
                newDocument: new BsonDocument().Set("_id", 1).Set("_collection", "ord_tx_only_cov").Set("A", 1).Set("Name", "tail-1")));
            tx.Operations.Add(new TransactionOperation(
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

            var txOnlyResult = InvokeExecuteByOrderIndexWithTransaction("ord_tx_only_cov", txOnlyShape, txOnlyIndex!, descending: false, tx, out _).ToList();
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
        tx.Operations.Add(new TransactionOperation(
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

        var result = InvokeExecuteTopKScan("topk_overlay_cov", shape, out _).ToList();
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

        var result = InvokeExecuteTopKScan("topk_stub_flag_false_cov", shape, out _).ToList();
        await Assert.That(result.Count).IsEqualTo(2);
    }

    [Test]
    public async Task PrivateHelpers_ShouldCoverConversionsCollectionsTransactionsAndSortBranches()
    {
        await Assert.That(ReverseComparisonOperator(ExpressionType.GreaterThan)).IsEqualTo(ExpressionType.LessThan);
        await Assert.That(ReverseComparisonOperator(ExpressionType.GreaterThanOrEqual)).IsEqualTo(ExpressionType.LessThanOrEqual);
        await Assert.That(ReverseComparisonOperator(ExpressionType.LessThan)).IsEqualTo(ExpressionType.GreaterThan);
        await Assert.That(ReverseComparisonOperator(ExpressionType.LessThanOrEqual)).IsEqualTo(ExpressionType.GreaterThanOrEqual);
        await Assert.That(ReverseComparisonOperator(ExpressionType.Equal)).IsEqualTo(ExpressionType.Equal);

        var ok = TryConvertConstant(new TinyDb.Query.ConstantExpression("123"), typeof(int), out var converted);
        await Assert.That(ok).IsTrue();
        await Assert.That(converted.Value).IsEqualTo(123);

        var formatFail = TryConvertConstant(new TinyDb.Query.ConstantExpression("not-a-number"), typeof(int), out _);
        await Assert.That(formatFail).IsFalse();

        var overflowFail = TryConvertConstant(new TinyDb.Query.ConstantExpression(999), typeof(byte), out _);
        await Assert.That(overflowFail).IsFalse();

        var castFail = TryConvertConstant(new TinyDb.Query.ConstantExpression(new object()), typeof(DateTime), out _);
        await Assert.That(castFail).IsFalse();

        var overflowDouble = TryConvertDecimal128ToDouble(Decimal128.MaxValue, out _);
        await Assert.That(overflowDouble).IsFalse();

        await Assert.That(() => DeserializeDocumentOrThrow(new byte[] { 1, 2, 3 }))
            .Throws<InvalidOperationException>();

        var noCollectionField = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1));
        var wrongCollectionType = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1).Set("_collection", 123));
        var mismatchCollection = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1).Set("_collection", "other"));

        var collectionFieldName = Encoding.UTF8.GetBytes("_collection");
        var expectedCollection = Encoding.UTF8.GetBytes("target");

        await Assert.That(MatchesCollection(noCollectionField, collectionFieldName, expectedCollection)).IsTrue();
        await Assert.That(MatchesCollection(wrongCollectionType, collectionFieldName, expectedCollection)).IsFalse();
        await Assert.That(MatchesCollection(mismatchCollection, collectionFieldName, expectedCollection)).IsFalse();

        var malformedLength = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1).Set("_collection", "target"));
        var located = BsonScanner.TryLocateField(malformedLength, collectionFieldName, out var valueOffset, out _);
        await Assert.That(located).IsTrue();
        BinaryPrimitives.WriteInt32LittleEndian(malformedLength.AsSpan(valueOffset, 4), 0);
        await Assert.That(MatchesCollection(malformedLength, collectionFieldName, expectedCollection)).IsFalse();

        var oversizedLength = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", 1).Set("_collection", "target"));
        BsonScanner.TryLocateField(oversizedLength, collectionFieldName, out var oversizedOffset, out _);
        BinaryPrimitives.WriteInt32LittleEndian(oversizedLength.AsSpan(oversizedOffset, 4), 1000);
        await Assert.That(MatchesCollection(oversizedLength, collectionFieldName, expectedCollection)).IsFalse();

        var shortRead = ReadString(new byte[] { 1, 2, 3 }, 0);
        await Assert.That(shortRead is null).IsTrue();

        var readStringBytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("s", "abc"));
        BsonScanner.TryLocateField(readStringBytes, Encoding.UTF8.GetBytes("s"), out var stringOffset, out _);
        var parsed = ReadString(readStringBytes, stringOffset);
        await Assert.That(parsed).IsNotNull();
        await Assert.That(parsed!.Value).IsEqualTo("abc");

        BinaryPrimitives.WriteInt32LittleEndian(readStringBytes.AsSpan(stringOffset, 4), 0);
        var zeroLengthRead = ReadString(readStringBytes, stringOffset);
        await Assert.That(zeroLengthRead is null).IsTrue();

        var sortDoc = new BsonDocument().Set("Name", "Alice").Set("id", 99);
        var nameValue = TryGetSortValue(sortDoc, "name");
        var idValue = TryGetSortValue(sortDoc, "_id");
        var idPascalOnlyValue = TryGetSortValue(new BsonDocument().Set("Id", 100), "_id");
        var noneValue = TryGetSortValue(sortDoc, "notExists");
        await Assert.That(nameValue!.ToString()).IsEqualTo("Alice");
        await Assert.That(idValue!.ToInt32()).IsEqualTo(99);
        await Assert.That(idPascalOnlyValue!.ToInt32()).IsEqualTo(100);
        await Assert.That(noneValue).IsNull();

        var member = new TinyDb.Query.MemberExpression("A", new TinyDb.Query.ParameterExpression("x"));

        var case1 = new TinyDb.Query.BinaryExpression(ExpressionType.Equal, member, new TinyDb.Query.ConstantExpression(7));
        var case1Result = ExtractComparison(_executor, case1);
        await Assert.That(case1Result.member).IsNotNull();
        await Assert.That(case1Result.constant).IsNotNull();
        await Assert.That(case1Result.op).IsEqualTo(ExpressionType.Equal);

        var case2 = new TinyDb.Query.BinaryExpression(
            ExpressionType.GreaterThan,
            member,
            new TinyDb.Query.UnaryExpression(ExpressionType.Convert, new TinyDb.Query.ConstantExpression("8"), typeof(int)));
        var case2Result = ExtractComparison(_executor, case2);
        await Assert.That(case2Result.member).IsNotNull();
        await Assert.That(case2Result.constant).IsNotNull();
        await Assert.That(case2Result.constant!.Value).IsEqualTo(8);
        await Assert.That(case2Result.op).IsEqualTo(ExpressionType.GreaterThan);

        var case2Fail = new TinyDb.Query.BinaryExpression(
            ExpressionType.Equal,
            member,
            new TinyDb.Query.UnaryExpression(ExpressionType.Convert, new TinyDb.Query.ConstantExpression("bad"), typeof(int)));
        var case2FailResult = ExtractComparison(_executor, case2Fail);
        await Assert.That(case2FailResult.member).IsNull();
        await Assert.That(case2FailResult.constant).IsNull();

        var case3 = new TinyDb.Query.BinaryExpression(ExpressionType.GreaterThan, new TinyDb.Query.ConstantExpression(5), member);
        var case3Result = ExtractComparison(_executor, case3);
        await Assert.That(case3Result.member).IsNotNull();
        await Assert.That(case3Result.constant).IsNotNull();
        await Assert.That(case3Result.op).IsEqualTo(ExpressionType.LessThan);

        var case4 = new TinyDb.Query.BinaryExpression(
            ExpressionType.GreaterThanOrEqual,
            new TinyDb.Query.UnaryExpression(ExpressionType.Convert, new TinyDb.Query.ConstantExpression("10"), typeof(int)),
            member);
        var case4Result = ExtractComparison(_executor, case4);
        await Assert.That(case4Result.member).IsNotNull();
        await Assert.That(case4Result.constant).IsNotNull();
        await Assert.That(case4Result.op).IsEqualTo(ExpressionType.LessThanOrEqual);

        var case4Fail = new TinyDb.Query.BinaryExpression(
            ExpressionType.GreaterThanOrEqual,
            new TinyDb.Query.UnaryExpression(ExpressionType.Convert, new TinyDb.Query.ConstantExpression(999), typeof(byte)),
            member);
        var case4FailResult = ExtractComparison(_executor, case4Fail);
        await Assert.That(case4FailResult.member).IsNull();
        await Assert.That(case4FailResult.constant).IsNull();

        using (var tx = (Transaction)_engine.BeginTransaction())
        {
            var doc2 = new BsonDocument().Set("_id", 2).Set("A", 20);

            tx.Operations.Add(new TransactionOperation(TransactionOperationType.Delete, "tx_col", new BsonInt32(1)));
            tx.Operations.Add(new TransactionOperation(TransactionOperationType.Update, "tx_col", new BsonInt32(2), newDocument: doc2));
            tx.Operations.Add(new TransactionOperation(TransactionOperationType.Update, "tx_col", new BsonInt32(3), newDocument: null));
            tx.Operations.Add(new TransactionOperation(TransactionOperationType.Insert, "other_col", new BsonInt32(9), newDocument: new BsonDocument().Set("_id", 9)));

            var foundDelete = TryGetTransactionDocument(tx, "tx_col", new BsonInt32(1), out var deleteDoc);
            var foundUpdate = TryGetTransactionDocument(tx, "tx_col", new BsonInt32(2), out var updateDoc);
            var foundNullUpdate = TryGetTransactionDocument(tx, "tx_col", new BsonInt32(3), out var nullUpdateDoc);
            var foundMissing = TryGetTransactionDocument(tx, "tx_col", new BsonInt32(404), out _);

            await Assert.That(foundDelete).IsTrue();
            await Assert.That(deleteDoc).IsNull();
            await Assert.That(foundUpdate).IsTrue();
            await Assert.That(updateDoc).IsNotNull();
            await Assert.That(foundNullUpdate).IsTrue();
            await Assert.That(nullUpdateDoc).IsNull();
            await Assert.That(foundMissing).IsFalse();

            var overlay = BuildTransactionOverlay(tx, "tx_col");
            await Assert.That(overlay).IsNotNull();
            await Assert.That(overlay!.Count).IsEqualTo(2);
            await Assert.That(overlay[new BsonInt32(1)]).IsNull();
            await Assert.That(overlay[new BsonInt32(2)]).IsNotNull();

            tx.Rollback();
        }

        using (var emptyTx = (Transaction)_engine.BeginTransaction())
        {
            var nullOverlay = BuildTransactionOverlay(emptyTx, "missing");
            await Assert.That(nullOverlay).IsNull();
            emptyTx.Rollback();
        }

        var idxCol = _engine.GetBsonCollection("idx_build");
        idxCol.Insert(new BsonDocument().Set("_id", 1).Set("A", 1).Set("B", 2));
        _engine.GetIndexManager("idx_build").CreateIndex("idx_ab", new[] { "A", "B" }, unique: false);
        var idx = _engine.GetIndexManager("idx_build").GetIndex("idx_ab");
        await Assert.That(idx).IsNotNull();

        var keyFull = BuildIndexKeyForOrder(idx!, new BsonDocument().Set("A", 9).Set("a", 9).Set("B", 8).Set("b", 8));
        await Assert.That(keyFull.Length).IsEqualTo(2);

        var keyMissing = BuildIndexKeyForOrder(idx, new BsonDocument().Set("A", 9).Set("a", 9));
        await Assert.That(keyMissing.Length).IsEqualTo(2);
        await Assert.That(keyMissing.Values[1].IsNull).IsTrue();

        var qeType = typeof(QueryExecutor);
        var txOrderRowType = qeType.GetNestedType("TxOrderRow", BindingFlags.NonPublic);
        await Assert.That(txOrderRowType).IsNotNull();
        var txOrderRowCtor = txOrderRowType!.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
            .First();

        var sharedKey = new IndexKey(new BsonInt32(1), new BsonInt32(1));
        var rowA = txOrderRowCtor.Invoke(new object[] { new BsonInt32(1), sharedKey, new BsonDocument().Set("_id", 1) });
        var rowB = txOrderRowCtor.Invoke(new object[] { new BsonInt32(2), sharedKey, new BsonDocument().Set("_id", 2) });

        var compareTxRows = qeType.GetMethod("CompareTxRows", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(compareTxRows).IsNotNull();
        var cmpAsc = (int)compareTxRows!.Invoke(null, new[] { rowA, rowB, (object)false })!;
        var cmpDesc = (int)compareTxRows.Invoke(null, new[] { rowA, rowB, (object)true })!;
        await Assert.That(cmpAsc < 0).IsTrue();
        await Assert.That(cmpDesc > 0).IsTrue();

        var compareTxRowToBase = qeType.GetMethod("CompareTxRowToBase", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(compareTxRowToBase).IsNotNull();
        var cmpToBaseAsc = (int)compareTxRowToBase!.Invoke(null, new object[] { rowA, sharedKey, new BsonInt32(2), false })!;
        var cmpToBaseDesc = (int)compareTxRowToBase.Invoke(null, new object[] { rowA, sharedKey, new BsonInt32(2), true })!;
        await Assert.That(cmpToBaseAsc < 0).IsTrue();
        await Assert.That(cmpToBaseDesc > 0).IsTrue();

        var sortKeyType = qeType.GetNestedType("SortKey", BindingFlags.NonPublic);
        await Assert.That(sortKeyType).IsNotNull();
        var fromBsonValue = sortKeyType!.GetMethod("FromBsonValue", BindingFlags.Public | BindingFlags.Static);
        await Assert.That(fromBsonValue).IsNotNull();

        var boolKeyFalse = fromBsonValue!.Invoke(null, new object?[] { new BsonBoolean(false) });
        var boolKeyTrue = fromBsonValue.Invoke(null, new object?[] { new BsonBoolean(true) });
        var dateKey1 = fromBsonValue.Invoke(null, new object?[] { new BsonDateTime(new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc)) });
        var dateKey2 = fromBsonValue.Invoke(null, new object?[] { new BsonDateTime(new DateTime(2021, 1, 1, 0, 0, 0, DateTimeKind.Utc)) });
        var strKeyA = fromBsonValue.Invoke(null, new object?[] { new BsonString("a") });
        var strKeyB = fromBsonValue.Invoke(null, new object?[] { new BsonString("b") });

        var compareSortKeys = sortKeyType.GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m =>
            {
                if (m.Name != "Compare") return false;
                var p = m.GetParameters();
                return p.Length == 2 &&
                       p[0].ParameterType.IsByRef &&
                       p[1].ParameterType.IsByRef &&
                       p[0].ParameterType.GetElementType() == sortKeyType &&
                       p[1].ParameterType.GetElementType() == sortKeyType;
            });

        _ = (int)compareSortKeys.Invoke(null, new[] { boolKeyTrue, boolKeyFalse })!;
        _ = (int)compareSortKeys.Invoke(null, new[] { dateKey1, dateKey2 })!;
        _ = (int)compareSortKeys.Invoke(null, new[] { strKeyA, strKeyB })!;
        _ = (int)compareSortKeys.Invoke(null, new[] { boolKeyTrue, dateKey1 })!;

        var topKRowType = qeType.GetNestedType("TopKRow", BindingFlags.NonPublic);
        await Assert.That(topKRowType).IsNotNull();
        var topKRowCtor = topKRowType!.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance).Single();
        var keyArray = Array.CreateInstance(sortKeyType, 1);
        keyArray.SetValue(fromBsonValue.Invoke(null, new object?[] { new BsonInt32(1) }), 0);
        var row = topKRowCtor.Invoke(new object?[] { new BsonInt32(1), keyArray, 0L, null });

        var compareDocumentToRow = qeType.GetMethod("CompareDocumentToRow", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(compareDocumentToRow).IsNotNull();
        var descendingSort = (IReadOnlyList<QuerySortField>)new List<QuerySortField> { new QuerySortField("A", typeof(int), true) };
        _ = (int)compareDocumentToRow!.Invoke(null, new object[] { new BsonDocument().Set("A", 2), row, 1L, descendingSort })!;

        await Assert.That(() => fromBsonValue.Invoke(null, new object?[] { new BsonDecimal128(Decimal128.MaxValue) }))
            .Throws<TargetInvocationException>();
    }

    [Test]
    public async Task PrivateReadersAndSortKeyFallbacks_ShouldCoverRemainingBranches()
    {
        var tryReadBsonValueId = CreateTryReadBsonValueIdBridge();
        var sortKeyRefTryRead = CreateSortKeyRefTryReadBridge();
        var materializeUnsupported = CreateSortKeyMaterializeUnsupportedBridge();
        var compareSortKeyRefToSortKey = CreateSortKeyRefToSortKeyCompareBridge();

        var noIdBytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("x", 1));
        var noIdResult = tryReadBsonValueId(noIdBytes);
        await Assert.That(noIdResult).IsFalse();

        var nullIdBytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", BsonNull.Value));
        var nullIdResult = tryReadBsonValueId(nullIdBytes);
        await Assert.That(nullIdResult).IsTrue();

        var malformedObjectIdBytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("_id", new BsonObjectId(ObjectId.NewObjectId())));
        Array.Resize(ref malformedObjectIdBytes, malformedObjectIdBytes.Length - 5);
        var malformedObjectIdResult = tryReadBsonValueId(malformedObjectIdBytes);
        await Assert.That(malformedObjectIdResult).IsFalse();

        var sortKeyRefResult = sortKeyRefTryRead(new byte[] { 1, 2, 3 }, 32, BsonType.String);
        await Assert.That(sortKeyRefResult).IsFalse();

        await Assert.That(materializeUnsupported()).IsTrue();

        var compareResult = compareSortKeyRefToSortKey();
        await Assert.That(compareResult).IsNotEqualTo(0);
    }

    private static Func<byte[], bool> CreateTryReadBsonValueIdBridge()
    {
        var queryExecutorType = typeof(QueryExecutor);
        var sortFieldBytesType = queryExecutorType.GetNestedType("SortFieldBytes", BindingFlags.NonPublic)
            ?? throw new MissingMemberException(queryExecutorType.FullName, "SortFieldBytes");
        var tryReadBsonValue = queryExecutorType.GetMethod("TryReadBsonValue", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new MissingMethodException(queryExecutorType.FullName, "TryReadBsonValue");
        var idGetter = sortFieldBytesType.GetProperty("Id", BindingFlags.Public | BindingFlags.Static)?.GetGetMethod()
            ?? throw new MissingMethodException(sortFieldBytesType.FullName, "get_Id");

        var dynamicMethod = new DynamicMethod(
            "TryReadBsonValueIdBridge",
            typeof(bool),
            new[] { typeof(byte[]) },
            typeof(QueryExecutorPrivateHelperCoverageTests).Module,
            skipVisibility: true);

        var il = dynamicMethod.GetILGenerator();
        var spanCtor = typeof(ReadOnlySpan<byte>).GetConstructor(new[] { typeof(byte[]) })
            ?? throw new MissingMethodException(typeof(ReadOnlySpan<byte>).FullName, ".ctor(byte[])");

        var fieldLocal = il.DeclareLocal(sortFieldBytesType);
        var valueLocal = il.DeclareLocal(typeof(BsonValue));

        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Newobj, spanCtor);
        il.Emit(OpCodes.Call, idGetter);
        il.Emit(OpCodes.Stloc, fieldLocal);
        il.Emit(OpCodes.Ldloca_S, fieldLocal);
        il.Emit(OpCodes.Ldloca_S, valueLocal);
        il.Emit(OpCodes.Call, tryReadBsonValue);
        il.Emit(OpCodes.Ret);

        return (Func<byte[], bool>)dynamicMethod.CreateDelegate(typeof(Func<byte[], bool>));
    }

    private static Func<byte[], int, BsonType, bool> CreateSortKeyRefTryReadBridge()
    {
        var queryExecutorType = typeof(QueryExecutor);
        var sortKeyRefType = queryExecutorType.GetNestedType("SortKeyRef", BindingFlags.NonPublic)
            ?? throw new MissingMemberException(queryExecutorType.FullName, "SortKeyRef");
        var tryRead = sortKeyRefType.GetMethod("TryRead", BindingFlags.Public | BindingFlags.Static)
            ?? throw new MissingMethodException(sortKeyRefType.FullName, "TryRead");

        var dynamicMethod = new DynamicMethod(
            "SortKeyRefTryReadBridge",
            typeof(bool),
            new[] { typeof(byte[]), typeof(int), typeof(BsonType) },
            typeof(QueryExecutorPrivateHelperCoverageTests).Module,
            skipVisibility: true);

        var il = dynamicMethod.GetILGenerator();
        var spanCtor = typeof(ReadOnlySpan<byte>).GetConstructor(new[] { typeof(byte[]) })
            ?? throw new MissingMethodException(typeof(ReadOnlySpan<byte>).FullName, ".ctor(byte[])");

        var keyLocal = il.DeclareLocal(sortKeyRefType);

        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Newobj, spanCtor);
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Ldarg_2);
        il.Emit(OpCodes.Ldloca_S, keyLocal);
        il.Emit(OpCodes.Call, tryRead);
        il.Emit(OpCodes.Ret);

        return (Func<byte[], int, BsonType, bool>)dynamicMethod.CreateDelegate(typeof(Func<byte[], int, BsonType, bool>));
    }

    private static Func<bool> CreateSortKeyMaterializeUnsupportedBridge()
    {
        var queryExecutorType = typeof(QueryExecutor);
        var sortKeyRefType = queryExecutorType.GetNestedType("SortKeyRef", BindingFlags.NonPublic)
            ?? throw new MissingMemberException(queryExecutorType.FullName, "SortKeyRef");
        var sortKeyType = queryExecutorType.GetNestedType("SortKey", BindingFlags.NonPublic)
            ?? throw new MissingMemberException(queryExecutorType.FullName, "SortKey");
        var sortKeyRefCtor = sortKeyRefType.GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single();
        var materialize = sortKeyType.GetMethod("Materialize", BindingFlags.Public | BindingFlags.Static)
            ?? throw new MissingMethodException(sortKeyType.FullName, "Materialize");

        var dynamicMethod = new DynamicMethod(
            "SortKeyMaterializeUnsupportedBridge",
            typeof(bool),
            Type.EmptyTypes,
            typeof(QueryExecutorPrivateHelperCoverageTests).Module,
            skipVisibility: true);

        var il = dynamicMethod.GetILGenerator();
        var spanType = typeof(ReadOnlySpan<byte>);
        var spanLocal = il.DeclareLocal(spanType);
        var keyRefLocal = il.DeclareLocal(sortKeyRefType);

        il.Emit(OpCodes.Ldloca_S, spanLocal);
        il.Emit(OpCodes.Initobj, spanType);
        il.Emit(OpCodes.Ldc_I4, (int)BsonType.Array);
        il.Emit(OpCodes.Ldc_R8, 0d);
        il.Emit(OpCodes.Ldc_I8, 0L);
        il.Emit(OpCodes.Ldloc, spanLocal);
        il.Emit(OpCodes.Newobj, sortKeyRefCtor);
        il.Emit(OpCodes.Stloc, keyRefLocal);
        il.Emit(OpCodes.Ldloca_S, keyRefLocal);
        il.Emit(OpCodes.Call, materialize);
        il.Emit(OpCodes.Pop);
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Ret);

        return (Func<bool>)dynamicMethod.CreateDelegate(typeof(Func<bool>));
    }

    private static Func<int> CreateSortKeyRefToSortKeyCompareBridge()
    {
        var queryExecutorType = typeof(QueryExecutor);
        var sortKeyRefType = queryExecutorType.GetNestedType("SortKeyRef", BindingFlags.NonPublic)
            ?? throw new MissingMemberException(queryExecutorType.FullName, "SortKeyRef");
        var sortKeyType = queryExecutorType.GetNestedType("SortKey", BindingFlags.NonPublic)
            ?? throw new MissingMemberException(queryExecutorType.FullName, "SortKey");
        var sortKeyRefCtor = sortKeyRefType.GetConstructors(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single();
        var fromBsonValue = sortKeyType.GetMethod("FromBsonValue", BindingFlags.Public | BindingFlags.Static)
            ?? throw new MissingMethodException(sortKeyType.FullName, "FromBsonValue");
        var compareRefAndValue = sortKeyType.GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m =>
            {
                if (m.Name != "Compare") return false;
                var parameters = m.GetParameters();
                return parameters.Length == 2 &&
                       parameters[0].ParameterType.IsByRef &&
                       parameters[1].ParameterType.IsByRef &&
                       parameters[0].ParameterType.GetElementType() == sortKeyRefType &&
                       parameters[1].ParameterType.GetElementType() == sortKeyType;
            });
        var bsonStringCtor = typeof(BsonString).GetConstructor(new[] { typeof(string) })
            ?? throw new MissingMethodException(typeof(BsonString).FullName, ".ctor(string)");

        var dynamicMethod = new DynamicMethod(
            "SortKeyRefToSortKeyCompareBridge",
            typeof(int),
            Type.EmptyTypes,
            typeof(QueryExecutorPrivateHelperCoverageTests).Module,
            skipVisibility: true);

        var il = dynamicMethod.GetILGenerator();
        var spanType = typeof(ReadOnlySpan<byte>);
        var spanLocal = il.DeclareLocal(spanType);
        var keyRefLocal = il.DeclareLocal(sortKeyRefType);
        var sortKeyLocal = il.DeclareLocal(sortKeyType);

        il.Emit(OpCodes.Ldloca_S, spanLocal);
        il.Emit(OpCodes.Initobj, spanType);
        il.Emit(OpCodes.Ldc_I4, (int)BsonType.Int32);
        il.Emit(OpCodes.Ldc_R8, 1d);
        il.Emit(OpCodes.Ldc_I8, 0L);
        il.Emit(OpCodes.Ldloc, spanLocal);
        il.Emit(OpCodes.Newobj, sortKeyRefCtor);
        il.Emit(OpCodes.Stloc, keyRefLocal);

        il.Emit(OpCodes.Ldstr, "s");
        il.Emit(OpCodes.Newobj, bsonStringCtor);
        il.Emit(OpCodes.Call, fromBsonValue);
        il.Emit(OpCodes.Stloc, sortKeyLocal);

        il.Emit(OpCodes.Ldloca_S, keyRefLocal);
        il.Emit(OpCodes.Ldloca_S, sortKeyLocal);
        il.Emit(OpCodes.Call, compareRefAndValue);
        il.Emit(OpCodes.Ret);

        return (Func<int>)dynamicMethod.CreateDelegate(typeof(Func<int>));
    }

    private static Expression<Func<T, bool>> CreateInvokePredicate<T>()
    {
        var p = System.Linq.Expressions.Expression.Parameter(typeof(T), "x");
        var invoked = System.Linq.Expressions.Expression.Invoke(
            System.Linq.Expressions.Expression.Constant((Func<bool>)(() => true)));
        return System.Linq.Expressions.Expression.Lambda<Func<T, bool>>(invoked, p);
    }

    private IEnumerable<OrderEntity> InvokeExecuteByOrderIndex(
        string collectionName,
        QueryShape<OrderEntity> shape,
        BTreeIndex orderIndex,
        bool descending,
        out QueryPushdownInfo pushdown)
    {
        var method = typeof(QueryExecutor)
            .GetMethod("ExecuteByOrderIndex", BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(typeof(OrderEntity));

        var args = new object?[] { collectionName, shape, orderIndex, descending, null };
        IEnumerable<OrderEntity> result;
        try
        {
            result = (IEnumerable<OrderEntity>)method.Invoke(_executor, args)!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw ex.InnerException;
        }
        pushdown = (QueryPushdownInfo)args[4]!;
        return result;
    }

    private IEnumerable<OrderEntity> InvokeExecuteByOrderIndexWithTransaction(
        string collectionName,
        QueryShape<OrderEntity> shape,
        BTreeIndex orderIndex,
        bool descending,
        Transaction tx,
        out QueryPushdownInfo pushdown)
    {
        var method = typeof(QueryExecutor)
            .GetMethod("ExecuteByOrderIndexWithTransaction", BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(typeof(OrderEntity));

        var args = new object?[] { collectionName, shape, orderIndex, descending, tx, null };
        IEnumerable<OrderEntity> result;
        try
        {
            result = (IEnumerable<OrderEntity>)method.Invoke(_executor, args)!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw ex.InnerException;
        }
        pushdown = (QueryPushdownInfo)args[5]!;
        return result;
    }

    private IEnumerable<OrderEntity> InvokeExecuteTopKScan(
        string collectionName,
        QueryShape<OrderEntity> shape,
        out QueryPushdownInfo pushdown)
    {
        var method = typeof(QueryExecutor)
            .GetMethod("ExecuteTopKScan", BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(typeof(OrderEntity));

        var args = new object?[] { collectionName, shape, null };
        IEnumerable<OrderEntity> result;
        try
        {
            result = (IEnumerable<OrderEntity>)method.Invoke(_executor, args)!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw ex.InnerException;
        }
        pushdown = (QueryPushdownInfo)args[2]!;
        return result;
    }

    private IEnumerable<OrderEntity> InvokeExecuteFullTableScan(
        string collectionName,
        Expression<Func<OrderEntity, bool>>? expression)
    {
        var method = typeof(QueryExecutor)
            .GetMethod("ExecuteFullTableScan", BindingFlags.NonPublic | BindingFlags.Instance)!
            .MakeGenericMethod(typeof(OrderEntity));

        IEnumerable<OrderEntity> result;
        try
        {
            result = (IEnumerable<OrderEntity>)method.Invoke(_executor, new object?[] { collectionName, expression })!;
        }
        catch (TargetInvocationException ex) when (ex.InnerException != null)
        {
            throw ex.InnerException;
        }
        return result;
    }

    private static TDelegate CreateStaticDelegate<TDelegate>(string methodName)
        where TDelegate : Delegate
    {
        var method = typeof(QueryExecutor).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new MissingMethodException(typeof(QueryExecutor).FullName, methodName);
        return (TDelegate)method.CreateDelegate(typeof(TDelegate));
    }

    private static TDelegate CreateInstanceDelegate<TDelegate>(string methodName)
        where TDelegate : Delegate
    {
        var method = typeof(QueryExecutor).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingMethodException(typeof(QueryExecutor).FullName, methodName);
        return (TDelegate)method.CreateDelegate(typeof(TDelegate));
    }

    [Entity]
    public sealed class OrderEntity
    {
        public int Id { get; set; }
        public int A { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
