using System;
using System.IO;
using System.Linq;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryExecutorSparseOrderIndexTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;
    private readonly QueryExecutor _executor;

    public QueryExecutorSparseOrderIndexTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"sparse_order_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath, new TinyDbOptions { EnableJournaling = false });
        _executor = new QueryExecutor(_engine);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
    }

    [Test]
    public async Task ExecuteShaped_OrderBy_ShouldNotUseSparseIndex()
    {
        var collection = _engine.GetBsonCollection("docs");
        _engine.GetIndexManager("docs").CreateIndex("idx_score_sparse", new[] { "Score" }, unique: false, sparse: true);

        collection.Insert(new BsonDocument().Set("_id", 1).Set("Name", "missing"));
        collection.Insert(new BsonDocument().Set("_id", 2).Set("Score", 10));
        collection.Insert(new BsonDocument().Set("_id", 3).Set("Score", 20));

        var shape = new QueryShape<BsonDocument>
        {
            Sort = new[] { new QuerySortField("Score", typeof(int), descending: false) },
            Take = 10
        };

        var results = _executor.ExecuteShaped("docs", shape, out _).ToList();
        var ids = results.Select(doc => doc["_id"].ToInt32(null)).ToList();

        await Assert.That(ids.Count).IsEqualTo(3);
        await Assert.That(ids.Contains(1)).IsTrue();
        await Assert.That(ids.Contains(2)).IsTrue();
        await Assert.That(ids.Contains(3)).IsTrue();
    }
}
