using System;
using System.Linq;
using TinyDb.Core;
using TinyDb.Bson;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public class QueryExecutorIndexTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;

    public QueryExecutorIndexTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"idx_exec_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Entity]
    public class Product
    {
        public int Id { get; set; }
        public string Code { get; set; } = "";
        public int Group { get; set; }
        public double Score { get; set; }
    }

    [Test]
    public async Task IndexSeek_Unique_ShouldWork()
    {
        var col = _engine.GetCollection<Product>("products");
        _engine.EnsureIndex("products", "Code", "idx_code", unique: true);
        
        col.Insert(new Product { Id = 1, Code = "A", Group = 1, Score = 10 });
        col.Insert(new Product { Id = 2, Code = "B", Group = 1, Score = 20 });
        
        var res = col.Find(p => p.Code == "A").ToList();
        await Assert.That(res.Count).IsEqualTo(1);
        await Assert.That(res[0].Code).IsEqualTo("A");
    }

    [Test]
    public async Task IndexScan_Composite_Prefix_ShouldWork()
    {
        var col = _engine.GetCollection<Product>("products");
        _engine.GetIndexManager("products").CreateIndex("idx_group_score", new[] { "Group", "Score" }, unique: false);
        
        col.Insert(new Product { Id = 1, Group = 1, Score = 10 });
        col.Insert(new Product { Id = 2, Group = 1, Score = 20 });
        col.Insert(new Product { Id = 3, Group = 2, Score = 10 });
        
        // Prefix match
        var res = col.Find(p => p.Group == 1).ToList();
        await Assert.That(res.Count).IsEqualTo(2);
    }

    [Test]
    public async Task IndexScan_Composite_Range_ShouldWork()
    {
        var col = _engine.GetCollection<Product>("products");
        _engine.GetIndexManager("products").CreateIndex("idx_group_score", new[] { "Group", "Score" }, unique: false);
        
        col.Insert(new Product { Id = 1, Group = 1, Score = 10 });
        col.Insert(new Product { Id = 2, Group = 1, Score = 20 });
        col.Insert(new Product { Id = 3, Group = 1, Score = 30 });
        
        // Range on second field
        var res = col.Find(p => p.Group == 1 && p.Score > 15).ToList();
        await Assert.That(res.Count).IsEqualTo(2); // 20, 30
    }

    [Test]
    public async Task IndexScan_Range_MinMax_ShouldWork()
    {
        var col = _engine.GetCollection<Product>("products");
        _engine.EnsureIndex("products", "Score", "idx_score");
        
        col.Insert(new Product { Id = 1, Score = 10 });
        col.Insert(new Product { Id = 2, Score = 20 });
        col.Insert(new Product { Id = 3, Score = 30 });
        
        var res = col.Find(p => p.Score >= 20).ToList();
        await Assert.That(res.Count).IsEqualTo(2);
        
        res = col.Find(p => p.Score < 20).ToList();
        await Assert.That(res.Count).IsEqualTo(1);
    }
}
