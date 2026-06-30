using System;
using System.IO;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Collections;

public sealed class DocumentCollectionAsyncBranchCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly TinyDbEngine _engine;

    public DocumentCollectionAsyncBranchCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"doc_col_async_cov_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_dbPath, new TinyDbOptions { EnableJournaling = false });
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }

        var walFile = Path.Combine(
            Path.GetDirectoryName(_dbPath)!,
            $"{Path.GetFileNameWithoutExtension(_dbPath)}-wal.db");
        try { if (File.Exists(walFile)) File.Delete(walFile); } catch { }
    }

    [Test]
    public async Task FindByIdAsync_WhenIdNullOrBsonNull_ShouldReturnNull()
    {
        var col = _engine.GetCollection<AsyncTestEntity>();

        await Assert.That(await col.FindByIdAsync(null!)).IsNull();
        await Assert.That(await col.FindByIdAsync(BsonNull.Value)).IsNull();
    }

    [Test]
    public async Task FindOneAsync_WhenNoDocuments_ShouldReturnNull()
    {
        var col = _engine.GetCollection<AsyncTestEntity>();

        var result = await col.FindOneAsync(x => x.Name == "missing");

        await Assert.That(result).IsNull();
    }

    [Test]
    public async Task AsyncPredicateNullGuards_ShouldThrow()
    {
        var col = _engine.GetCollection<AsyncTestEntity>();

        Expression<Func<AsyncTestEntity, bool>> predicate = null!;

        await Assert.That(async () => { await col.FindAsync(predicate); }).ThrowsExactly<ArgumentNullException>();
        await Assert.That(async () => { await col.FindAsync(predicate, 0, 10); }).ThrowsExactly<ArgumentNullException>();
        await Assert.That(async () => { await col.FindOneAsync(predicate); }).ThrowsExactly<ArgumentNullException>();
        await Assert.That(async () => { await col.CountAsync(predicate); }).ThrowsExactly<ArgumentNullException>();
        await Assert.That(async () => { await col.ExistsAsync(predicate); }).ThrowsExactly<ArgumentNullException>();
    }

    [Test]
    public async Task CountAsync_WhenInTransaction_ShouldUseFindAllAsyncPath()
    {
        var col = _engine.GetCollection<AsyncTestEntity>();
        col.Insert(new AsyncTestEntity { Name = "outside" });

        using (_engine.BeginTransaction())
        {
            col.Insert(new AsyncTestEntity { Name = "inside" });

            var count = await col.CountAsync();
            await Assert.That(count).IsEqualTo(2);
        }
    }

    [Test]
    public async Task AsyncPredicateMethods_ShouldMatchSyncQueryPipeline()
    {
        var col = _engine.GetCollection<AsyncTestEntity>();
        col.GetIndexManager().CreateIndex("idx_value", new[] { nameof(AsyncTestEntity.Value) }, unique: false);

        col.Insert(new AsyncTestEntity { Name = "low", Value = 1 });
        col.Insert(new AsyncTestEntity { Name = "match-one", Value = 10 });
        col.Insert(new AsyncTestEntity { Name = "match-two", Value = 20 });

        Expression<Func<AsyncTestEntity, bool>> predicate = x => x.Value >= 10;

        var syncFirst = col.FindOne(predicate);
        var asyncFirst = await col.FindOneAsync(predicate);
        var syncPage = col.Find(predicate, 1, 1).ToList();
        var asyncPage = await col.FindAsync(predicate, 1, 1);

        await Assert.That(asyncFirst?.Id).IsEqualTo(syncFirst?.Id);
        await Assert.That(await col.ExistsAsync(predicate)).IsEqualTo(col.Exists(predicate));
        await Assert.That(await col.CountAsync(predicate)).IsEqualTo(col.Count(predicate));
        await Assert.That(asyncPage.Select(x => x.Id).SequenceEqual(syncPage.Select(x => x.Id))).IsTrue();
    }

    [Test]
    public async Task InsertAsyncBatch_InTransaction_ShouldRollback()
    {
        var col = _engine.GetCollection<AsyncTestEntity>();

        using var tx = _engine.BeginTransaction();

        var inserted = await col.InsertAsync(new[]
        {
            new AsyncTestEntity { Name = "batch-one" },
            new AsyncTestEntity { Name = "batch-two" }
        });

        await Assert.That(inserted).IsEqualTo(2);
        await Assert.That(await col.CountAsync()).IsEqualTo(2);

        tx.Rollback();

        await Assert.That(await col.CountAsync()).IsEqualTo(0);
    }
}
