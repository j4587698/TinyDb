using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Core;

/// <summary>
/// 回归测试：主键 _id 缺失或为 null 时的处理。
/// - 事务内插入 _id 为 null 的文档时，返回给调用方的 id 必须与最终存储的一致；
/// - 缺失或为 null 的 _id 绝不能以 null 键写入主键索引。
/// </summary>
[NotInParallel]
public sealed class NullIdHardeningTests
{
    private string _dbPath = null!;

    [Before(Test)]
    public void Setup()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"null_id_{Guid.NewGuid():N}.db");
    }

    [After(Test)]
    public void Cleanup()
    {
        foreach (var path in new[] { _dbPath, _dbPath + ".wal", _dbPath + "-wal", _dbPath + "-shm" })
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    // ---------- 事务内插入：返回的 id 必须与存储的一致 ----------

    [Test]
    public async Task TransactionalInsert_WithNullId_ReturnedIdShouldMatchStoredDocument()
    {
        using var engine = new TinyDbEngine(_dbPath);
        var col = engine.GetBsonCollection("null_id_tx");

        BsonValue returnedId;
        using (var tx = engine.BeginTransaction())
        {
            returnedId = col.Insert(new BsonDocument().Set("_id", BsonNull.Value).Set("v", 1));
            tx.Commit();
        }

        await Assert.That(returnedId.IsNull).IsFalse();
        var stored = col.FindById(returnedId);
        await Assert.That(stored).IsNotNull();
        await Assert.That(stored!["v"].ToInt32(null)).IsEqualTo(1);
    }

    [Test]
    public async Task TransactionalInsert_TwoNullIdDocuments_ShouldCommitWithDistinctIds()
    {
        using var engine = new TinyDbEngine(_dbPath);
        var col = engine.GetBsonCollection("null_id_tx_two");

        BsonValue first;
        BsonValue second;
        using (var tx = engine.BeginTransaction())
        {
            first = col.Insert(new BsonDocument().Set("_id", BsonNull.Value).Set("v", 1));
            second = col.Insert(new BsonDocument().Set("_id", BsonNull.Value).Set("v", 2));
            tx.Commit();
        }

        await Assert.That(first.IsNull).IsFalse();
        await Assert.That(second.IsNull).IsFalse();
        await Assert.That(first.Equals(second)).IsFalse();
        await Assert.That(col.FindById(first)!["v"].ToInt32(null)).IsEqualTo(1);
        await Assert.That(col.FindById(second)!["v"].ToInt32(null)).IsEqualTo(2);
    }

    [Test]
    public async Task NonTransactionalInsert_WithNullId_ReturnedIdShouldMatchStoredDocument()
    {
        using var engine = new TinyDbEngine(_dbPath);
        var col = engine.GetBsonCollection("null_id_direct");

        var returnedId = col.Insert(new BsonDocument().Set("_id", BsonNull.Value).Set("v", 7));

        await Assert.That(returnedId.IsNull).IsFalse();
        await Assert.That(col.FindById(returnedId)!["v"].ToInt32(null)).IsEqualTo(7);
    }

    [Test]
    public async Task RecordInsert_WithMissingOrNullId_ShouldWriteReturnedIdIntoRecordedDocument()
    {
        using var engine = new TinyDbEngine(_dbPath);
        using var _ = engine.BeginTransaction();
        var tx = engine.GetCurrentTransaction()!;

        var missingId = tx.RecordInsert("col", new BsonDocument().Set("v", 1));
        var nullId = tx.RecordInsert("col", new BsonDocument().Set("_id", BsonNull.Value).Set("v", 2));

        await Assert.That(missingId.IsNull).IsFalse();
        await Assert.That(nullId.IsNull).IsFalse();
        await Assert.That(tx.Operations[0].NewDocument!["_id"]).IsEqualTo(missingId);
        await Assert.That(tx.Operations[1].NewDocument!["_id"]).IsEqualTo(nullId);
        await Assert.That(tx.Operations[0].DocumentId).IsEqualTo(missingId);
        await Assert.That(tx.Operations[1].DocumentId).IsEqualTo(nullId);
    }

    // ---------- 主键索引：缺失或为 null 的 _id 不得以 null 键入索引 ----------

    [Test]
    public async Task DeletingNeighbour_OnPageWithIdLessDocument_ShouldNotIndexItUnderNullKey()
    {
        const string colName = "idless_rewrite";
        using var engine = new TinyDbEngine(_dbPath, new TinyDbOptions { EnableJournaling = false });
        var col = engine.GetBsonCollection(colName);
        col.Insert(new BsonDocument().Set("_id", 1).Set("v", 1));
        col.Insert(new BsonDocument().Set("_id", 2).Set("v", 2));

        // 模拟外部工具或文件损坏：在同一数据页上追加一条缺 _id 的文档。
        var state = engine.GetCollectionState(colName);
        var page = engine.PageManager.GetPage(state.OwnedPages.Keys.Single());
        page.Append(BsonSerializer.SerializeDocument(
            new BsonDocument().Set("_collection", colName).Set("v", 3)));

        await Assert.That(engine.GetCachedDocumentCount(colName)).IsEqualTo(2);

        // 删除同页的另一条文档会触发整页重写。
        col.Delete(new BsonInt32(1));

        await Assert.That(engine.GetCachedDocumentCount(colName)).IsEqualTo(1);
        await Assert.That(state.Index.TryGet(BsonNull.Value, out _)).IsFalse();
    }

    [Test]
    public async Task BuildDocumentLocationCache_ShouldNotIndexNullId()
    {
        const string colName = "null_id_rebuild";
        using var engine = new TinyDbEngine(_dbPath, new TinyDbOptions { EnableJournaling = false });
        var col = engine.GetBsonCollection(colName);
        col.Insert(new BsonDocument().Set("_id", 1).Set("v", 1));

        var state = engine.GetCollectionState(colName);
        var page = engine.PageManager.GetPage(state.OwnedPages.Keys.Single());
        page.Append(BsonSerializer.SerializeDocument(
            new BsonDocument().Set("_id", BsonNull.Value).Set("_collection", colName).Set("v", 2)));
        page.Append(BsonSerializer.SerializeDocument(
            new BsonDocument().Set("_collection", colName).Set("v", 3)));

        engine.BuildDocumentLocationCache(colName, state);

        await Assert.That(state.Index.Count).IsEqualTo(1);
        await Assert.That(state.Index.TryGet(new BsonInt32(1), out _)).IsTrue();
        await Assert.That(state.Index.TryGet(BsonNull.Value, out _)).IsFalse();
    }
}
