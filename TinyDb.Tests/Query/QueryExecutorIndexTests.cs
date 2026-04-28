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
    public async Task EnsureIndex_AfterExistingDocuments_ShouldBackfillIndex()
    {
        var col = _engine.GetCollection<Product>("products_backfill");
        col.Insert(new Product { Id = 1, Code = "A", Group = 1, Score = 10 });
        col.Insert(new Product { Id = 2, Code = "B", Group = 1, Score = 20 });

        _engine.EnsureIndex("products_backfill", "Code", "idx_code_backfill", unique: true);

        var res = col.Find(p => p.Code == "B").ToList();
        await Assert.That(res.Count).IsEqualTo(1);
        await Assert.That(res[0].Id).IsEqualTo(2);
    }

    [Test]
    public async Task EnsureIndex_AfterReopen_ShouldLoadPersistedRootPage()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"idx_persist_{Guid.NewGuid():N}.db");
        const string collectionName = "persisted_products";
        const string indexName = "idx_code_persisted";
        uint rootPageId;

        try
        {
            using (var engine = new TinyDbEngine(dbPath))
            {
                var col = engine.GetCollection<Product>(collectionName);
                col.Insert(new Product { Id = 1, Code = "A", Group = 1, Score = 10 });
                col.Insert(new Product { Id = 2, Code = "B", Group = 1, Score = 20 });

                engine.EnsureIndex(collectionName, "Code", indexName, unique: true);
                var index = engine.GetIndexManager(collectionName).GetIndex(indexName);

                await Assert.That(index).IsNotNull();
                await Assert.That(index!.EntryCount).IsEqualTo(2);
                rootPageId = index.RootPageId;
                engine.Flush();
            }

            using (var engine = new TinyDbEngine(dbPath))
            {
                var indexManager = engine.GetIndexManager(collectionName);
                await Assert.That(indexManager.IndexExists(indexName)).IsTrue();

                var index = indexManager.GetIndex(indexName);
                await Assert.That(index).IsNotNull();
                await Assert.That(index!.RootPageId).IsEqualTo(rootPageId);
                await Assert.That(index.EntryCount).IsEqualTo(2);

                var col = engine.GetCollection<Product>(collectionName);
                var res = col.Find(p => p.Code == "B").ToList();
                await Assert.That(res.Count).IsEqualTo(1);
                await Assert.That(res[0].Id).IsEqualTo(2);
            }
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    [Test]
    public async Task DropCollection_AfterPersistedIndex_ShouldRemoveIndexDefinitionOnReopen()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"idx_drop_{Guid.NewGuid():N}.db");
        const string collectionName = "drop_index_products";
        const string indexName = "idx_code_drop";

        try
        {
            using (var engine = new TinyDbEngine(dbPath))
            {
                var col = engine.GetCollection<Product>(collectionName);
                col.Insert(new Product { Id = 1, Code = "A", Group = 1, Score = 10 });

                engine.EnsureIndex(collectionName, "Code", indexName, unique: true);
                await Assert.That(engine.GetIndexManager(collectionName).IndexExists(indexName)).IsTrue();

                await Assert.That(engine.DropCollection(collectionName)).IsTrue();
                engine.Flush();
            }

            using (var engine = new TinyDbEngine(dbPath))
            {
                await Assert.That(engine.CollectionExists(collectionName)).IsFalse();

                engine.GetCollection<Product>(collectionName);
                await Assert.That(engine.GetIndexManager(collectionName).IndexExists(indexName)).IsFalse();
            }
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
    }

    [Test]
    public async Task EnsureIndex_WhenBackfillFails_ShouldNotPersistIndexDefinition()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"idx_failed_backfill_{Guid.NewGuid():N}.db");
        const string collectionName = "failed_backfill_products";
        const string indexName = "idx_duplicate_code";

        try
        {
            using (var engine = new TinyDbEngine(dbPath))
            {
                var col = engine.GetCollection<Product>(collectionName);
                col.Insert(new Product { Id = 1, Code = "DUP", Group = 1, Score = 10 });
                col.Insert(new Product { Id = 2, Code = "DUP", Group = 1, Score = 20 });

                await Assert.That(() => engine.EnsureIndex(collectionName, "Code", indexName, unique: true))
                    .Throws<InvalidOperationException>();
                await Assert.That(engine.GetIndexManager(collectionName).IndexExists(indexName)).IsFalse();

                engine.Flush();
            }

            using (var engine = new TinyDbEngine(dbPath))
            {
                await Assert.That(engine.GetIndexManager(collectionName).IndexExists(indexName)).IsFalse();
            }
        }
        finally
        {
            if (File.Exists(dbPath)) File.Delete(dbPath);
        }
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
