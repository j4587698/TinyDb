using System;
using System.Linq;
using TinyDb.Core;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExecutorTransactionTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;

    public QueryExecutorTransactionTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"tx_exec_{Guid.NewGuid()}.db");
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
        public string Name { get; set; } = "";
        public bool Flag { get; set; }
    }

    [Test]
    public async Task FullTableScan_Transaction_Inserts_ShouldBeVisible()
    {
        var col = _engine.GetCollection<Item>("items");
        col.Insert(new Item { Id = 1, Name = "A", Flag = true });
        
        using var tx = _engine.BeginTransaction();
        
        col.Insert(new Item { Id = 2, Name = "B", Flag = true });
        
        // Find with predicate (Full Scan)
        var res = col.Find(x => x.Flag == true).ToList();
        
        await Assert.That(res.Count).IsEqualTo(2);
        await Assert.That(res.Any(x => x.Id == 2)).IsTrue();
        
        tx.Commit();
    }

    [Test]
    public async Task FullTableScan_Transaction_Deletes_ShouldBeHidden()
    {
        var col = _engine.GetCollection<Item>("items");
        col.Insert(new Item { Id = 1, Name = "A", Flag = true });
        
        using var tx = _engine.BeginTransaction();
        
        col.Delete(1); // Id
        
        var res = col.Find(x => x.Flag == true).ToList();
        
        await Assert.That(res.Count).IsEqualTo(0);
        
        tx.Commit();
    }

    [Test]
    public async Task FullTableScan_Transaction_Updates_ShouldBeVisible()
    {
        var col = _engine.GetCollection<Item>("items");
        col.Insert(new Item { Id = 1, Name = "A", Flag = true });
        
        using var tx = _engine.BeginTransaction();
        
        var item = col.FindById(1);
        item!.Name = "B";
        col.Update(item);
        
        var res = col.Find(x => x.Name == "B").ToList();
        
        await Assert.That(res.Count).IsEqualTo(1);
        
        var old = col.Find(x => x.Name == "A").ToList();
        await Assert.That(old.Count).IsEqualTo(0);
        
        tx.Commit();
    }
}
