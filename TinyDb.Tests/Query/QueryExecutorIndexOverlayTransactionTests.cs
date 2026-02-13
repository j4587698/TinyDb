using System;
using System.Linq;
using TinyDb.Attributes;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExecutorIndexOverlayTransactionTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;

    public QueryExecutorIndexOverlayTransactionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"tx_idx_overlay_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Entity]
    public class Item
    {
        public int Id { get; set; }
        public bool Flag { get; set; }
    }

    [Entity]
    public class Product
    {
        public int Id { get; set; }
        public string Code { get; set; } = "";
    }

    [Test]
    public async Task IndexScan_Transaction_OverlayInsertUpdateDelete_ShouldBeCorrect()
    {
        var col = _engine.GetCollection<Item>("items");
        _engine.GetIndexManager("items").CreateIndex("idx_flag", new[] { "Flag" }, unique: false);

        col.Insert(new Item { Id = 1, Flag = true });
        col.Insert(new Item { Id = 2, Flag = true });
        col.Insert(new Item { Id = 3, Flag = false });

        using var tx = _engine.BeginTransaction();

        col.Insert(new Item { Id = 4, Flag = true });

        var item3 = col.FindById(3)!;
        item3.Flag = true;
        col.Update(item3);

        col.Delete(2);

        var res = col.Query().Where(x => x.Flag == true).ToList();
        var ids = res.Select(x => x.Id).OrderBy(x => x).ToList();

        await Assert.That(ids.SequenceEqual(new[] { 1, 3, 4 })).IsTrue();

        tx.Commit();
    }

    [Test]
    public async Task IndexSeekUnique_Transaction_UpdateKey_ShouldBeCorrect()
    {
        var col = _engine.GetCollection<Product>("products");
        _engine.EnsureIndex("products", "Code", "idx_code", unique: true);

        col.Insert(new Product { Id = 1, Code = "A" });

        using var tx = _engine.BeginTransaction();

        col.Update(new Product { Id = 1, Code = "B" });

        var oldKey = col.Query().Where(x => x.Code == "A").ToList();
        await Assert.That(oldKey.Count).IsEqualTo(0);

        var newKey = col.Query().Where(x => x.Code == "B").ToList();
        await Assert.That(newKey.Count).IsEqualTo(1);
        await Assert.That(newKey[0].Id).IsEqualTo(1);

        tx.Commit();
    }

    [Test]
    public async Task PrimaryKeyLookup_Transaction_OverlayInsert_ShouldBeVisible()
    {
        var col = _engine.GetCollection<Item>("pk_items");
        col.Insert(new Item { Id = 1, Flag = false });

        using var tx = _engine.BeginTransaction();

        col.Insert(new Item { Id = 2, Flag = true });

        var found = col.Query().Where(x => x.Id == 2).SingleOrDefault();
        await Assert.That(found).IsNotNull();
        await Assert.That(found!.Id).IsEqualTo(2);

        tx.Commit();
    }
}

