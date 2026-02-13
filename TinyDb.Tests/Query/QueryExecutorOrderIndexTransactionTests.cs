using System;
using System.Linq;
using TinyDb.Attributes;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExecutorOrderIndexTransactionTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;

    public QueryExecutorOrderIndexTransactionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"tx_order_idx_{Guid.NewGuid()}.db");
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
        public double Score { get; set; }
        public bool Flag { get; set; }
    }

    [Test]
    public async Task OrderByIndex_Transaction_OverlayInsert_ShouldBeVisibleAndOrdered()
    {
        var col = _engine.GetCollection<Item>("items");
        _engine.GetIndexManager("items").CreateIndex("idx_score", new[] { "Score" }, unique: false);
        _engine.GetIndexManager("items").CreateIndex("idx_flag", new[] { "Flag" }, unique: false);

        col.Insert(new Item { Id = 1, Score = 10, Flag = true });
        col.Insert(new Item { Id = 2, Score = 20, Flag = false });

        using var tx = _engine.BeginTransaction();

        col.Insert(new Item { Id = 3, Score = 5, Flag = true });

        var res = col.Query()
            .Where(x => x.Flag == true)
            .OrderBy(x => x.Score)
            .ToList();

        var ids = res.Select(x => x.Id).ToList();
        await Assert.That(ids.SequenceEqual(new[] { 3, 1 })).IsTrue();

        tx.Commit();
    }

    [Test]
    public async Task OrderByIndex_Transaction_OverlayInsertUpdateDelete_SkipTake_ShouldBeCorrect()
    {
        var col = _engine.GetCollection<Item>("items2");
        _engine.GetIndexManager("items2").CreateIndex("idx_score", new[] { "Score" }, unique: false);

        col.Insert(new Item { Id = 1, Score = 10, Flag = true });
        col.Insert(new Item { Id = 2, Score = 20, Flag = true });
        col.Insert(new Item { Id = 3, Score = 30, Flag = true });

        using var tx = _engine.BeginTransaction();

        col.Insert(new Item { Id = 4, Score = 5, Flag = true });

        var item3 = col.FindById(3)!;
        item3.Score = 15;
        col.Update(item3);

        col.Delete(2);

        var res = col.Query()
            .OrderBy(x => x.Score)
            .Skip(1)
            .Take(2)
            .ToList();

        var ids = res.Select(x => x.Id).ToList();
        await Assert.That(ids.SequenceEqual(new[] { 1, 3 })).IsTrue();

        tx.Commit();
    }
}
