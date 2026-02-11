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

        await Assert.That(() => col.FindAsync(predicate)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => col.FindOneAsync(predicate)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => col.CountAsync(predicate)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => col.ExistsAsync(predicate)).ThrowsExactly<ArgumentNullException>();
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
}

