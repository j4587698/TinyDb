using TinyDb.Attributes;
using TinyDb.Collections;
using TinyDb.Core;
using TinyDb.Storage;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Collections;

public sealed class AsyncQueryExecutionTests
{
    [Test]
    public async Task FindAsync_FullTableScan_ShouldUseAsyncPageReads()
    {
        using var disk = new TrackingDiskStream();
        using var engine = CreateEngine(disk);
        var collection = engine.GetCollection<AsyncQueryDoc>("async_query_full_scan");

        collection.Insert(Enumerable.Range(1, 40)
            .Select(i => new AsyncQueryDoc { Id = i, Category = i % 2 == 0 ? "even" : "odd", Value = i }));

        using var _ = ForceDiskReads(engine, disk);

        var result = await collection.FindAsync(x => x.Value >= 10 && x.Value < 20);

        await Assert.That(result.Count).IsEqualTo(10);
        await Assert.That(disk.AsyncReadCount).IsGreaterThan(0);
    }

    [Test]
    public async Task ExistsAsync_ShouldShortCircuitAfterFirstMatch()
    {
        using var disk = new TrackingDiskStream();
        using var engine = CreateEngine(disk);
        var collection = engine.GetCollection<AsyncQueryDoc>("async_query_exists");

        collection.Insert(new AsyncQueryDoc { Id = 1, Category = "hit", Value = 1, Payload = new string('a', 512) });
        collection.Insert(Enumerable.Range(2, 40)
            .Select(i => new AsyncQueryDoc { Id = i, Category = "miss", Value = i, Payload = new string('b', 3000) }));

        using var _ = ForceDiskReads(engine, disk);

        var exists = await collection.ExistsAsync(x => x.Category == "hit");

        await Assert.That(exists).IsTrue();
        await Assert.That(disk.AsyncReadCount).IsLessThan(5);
    }

    [Test]
    public async Task CountAsync_ShouldStreamAndMatchSyncResult()
    {
        using var disk = new TrackingDiskStream();
        using var engine = CreateEngine(disk);
        var collection = engine.GetCollection<AsyncQueryDoc>("async_query_count");

        collection.Insert(Enumerable.Range(1, 60)
            .Select(i => new AsyncQueryDoc { Id = i, Category = i % 3 == 0 ? "match" : "other", Value = i }));

        var expected = collection.Count(x => x.Category == "match");
        using var _ = ForceDiskReads(engine, disk);

        var actual = await collection.CountAsync(x => x.Category == "match");

        await Assert.That(actual).IsEqualTo(expected);
        await Assert.That(disk.AsyncReadCount).IsGreaterThan(0);
    }

    [Test]
    public async Task FindAsync_IndexSeekAndScan_ShouldUseAsyncIndexAndDocumentReads()
    {
        using var disk = new TrackingDiskStream();
        using var engine = CreateEngine(disk);
        var collection = engine.GetCollection<AsyncQueryDoc>("async_query_index");

        engine.EnsureIndex(collection.CollectionName, "Category", "idx_category");
        engine.EnsureIndex(collection.CollectionName, "Value", "idx_value");
        engine.EnsureIndex(collection.CollectionName, "Code", "idx_code", unique: true);

        collection.Insert(Enumerable.Range(1, 50)
            .Select(i => new AsyncQueryDoc { Id = i, Code = $"C{i}", Category = i % 2 == 0 ? "even" : "odd", Value = i }));

        using var _ = ForceDiskReads(engine, disk);

        var unique = await collection.FindAsync(x => x.Code == "C10");
        var exact = await collection.FindAsync(x => x.Category == "even");
        var range = await collection.FindAsync(x => x.Value >= 45);

        await Assert.That(unique.Count).IsEqualTo(1);
        await Assert.That(unique[0].Id).IsEqualTo(10);
        await Assert.That(exact.Count).IsEqualTo(25);
        await Assert.That(range.Select(x => x.Id).OrderBy(x => x).SequenceEqual(new[] { 45, 46, 47, 48, 49, 50 })).IsTrue();
        await Assert.That(disk.AsyncReadCount).IsGreaterThan(0);
    }

    [Test]
    public async Task FindAsync_IndexMatchWithResidualPredicate_ShouldFilterCommittedDocuments()
    {
        using var disk = new TrackingDiskStream();
        using var engine = CreateEngine(disk);
        var collection = engine.GetCollection<AsyncQueryDoc>("async_query_index_residual");

        engine.EnsureIndex(collection.CollectionName, "Category", "idx_category_residual");

        collection.Insert(new[]
        {
            new AsyncQueryDoc { Id = 1, Category = "hit", Value = 1 },
            new AsyncQueryDoc { Id = 2, Category = "hit", Value = 2 },
            new AsyncQueryDoc { Id = 3, Category = "miss", Value = 3 },
            new AsyncQueryDoc { Id = 4, Category = "hit", Value = 4 },
        });

        using var _ = ForceDiskReads(engine, disk);

        var result = await collection.FindAsync(x => x.Category == "hit" && x.Value > 2);

        await Assert.That(result.Select(x => x.Id).OrderBy(x => x).SequenceEqual(new[] { 4 })).IsTrue();
        await Assert.That(disk.AsyncReadCount).IsGreaterThan(0);
    }

    [Test]
    public async Task FindAsync_TransactionOverlay_ShouldMatchSyncQuery()
    {
        using var disk = new TrackingDiskStream();
        using var engine = CreateEngine(disk);
        var collection = engine.GetCollection<AsyncQueryDoc>("async_query_tx");

        engine.EnsureIndex(collection.CollectionName, "Category", "idx_category");
        collection.Insert(new AsyncQueryDoc { Id = 1, Category = "hit", Value = 1 });
        collection.Insert(new AsyncQueryDoc { Id = 2, Category = "hit", Value = 2 });
        collection.Insert(new AsyncQueryDoc { Id = 3, Category = "miss", Value = 3 });

        using var tx = engine.BeginTransaction();
        collection.Insert(new AsyncQueryDoc { Id = 4, Category = "hit", Value = 4 });
        collection.Update(new AsyncQueryDoc { Id = 3, Category = "hit", Value = 3 });
        collection.Delete(2);

        var expected = collection.Query().Where(x => x.Category == "hit").Select(x => x.Id).OrderBy(x => x).ToArray();
        var actual = (await collection.FindAsync(x => x.Category == "hit")).Select(x => x.Id).OrderBy(x => x).ToArray();

        await Assert.That(actual.SequenceEqual(expected)).IsTrue();
    }

    [Test]
    public async Task FindAsync_LargeDocument_ShouldResolveThroughAsyncPath()
    {
        using var disk = new TrackingDiskStream();
        using var engine = CreateEngine(disk);
        var collection = engine.GetCollection<AsyncQueryDoc>("async_query_large");
        var payload = new string('x', 20_000);

        collection.Insert(new AsyncQueryDoc { Id = 1, Category = "large", Value = 1, Payload = payload });

        using var _ = ForceDiskReads(engine, disk);

        var result = await collection.FindAsync(x => x.Category == "large");

        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(result[0].Payload.Length).IsEqualTo(payload.Length);
        await Assert.That(disk.AsyncReadCount).IsGreaterThan(0);
    }

    [Test]
    public async Task FindAsync_ShouldObserveCancellationDuringPageRead()
    {
        using var disk = new TrackingDiskStream { AsyncReadDelay = TimeSpan.FromSeconds(30) };
        using var engine = CreateEngine(disk);
        var collection = engine.GetCollection<AsyncQueryDoc>("async_query_cancel");

        collection.Insert(Enumerable.Range(1, 20)
            .Select(i => new AsyncQueryDoc { Id = i, Category = "value", Value = i, Payload = new string('c', 3000) }));

        using var _ = ForceDiskReads(engine, disk);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
        {
            await collection.FindAsync(x => x.Value > 0, cts.Token);
        });
    }

    [Test]
    public async Task ConcurrentFindAsync_ShouldRemainConsistent()
    {
        using var disk = new TrackingDiskStream();
        using var engine = CreateEngine(disk);
        var collection = engine.GetCollection<AsyncQueryDoc>("async_query_concurrent");

        collection.Insert(Enumerable.Range(1, 100)
            .Select(i => new AsyncQueryDoc { Id = i, Category = i % 2 == 0 ? "even" : "odd", Value = i }));

        var tasks = Enumerable.Range(0, 8)
            .Select(_ => collection.FindAsync(x => x.Category == "even"))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        await Assert.That(results.All(r => r.Count == 50)).IsTrue();
        await Assert.That(results.All(r => r.Select(x => x.Id).OrderBy(x => x).SequenceEqual(Enumerable.Range(1, 100).Where(i => i % 2 == 0)))).IsTrue();
    }

    private static TinyDbEngine CreateEngine(TrackingDiskStream disk)
    {
        var options = new TinyDbOptions
        {
            EnableJournaling = false,
            CacheSize = 4,
            PageSize = 4096
        };

        return new TinyDbEngine("async-query-tests.db", options, disk);
    }

    private static IDisposable ForceDiskReads(TinyDbEngine engine, TrackingDiskStream disk)
    {
        engine.PageManager.ClearCache();
        disk.ResetCounts();
        disk.ThrowOnSyncRead = true;
        return new SyncReadGuard(disk);
    }

    private sealed class SyncReadGuard : IDisposable
    {
        private readonly TrackingDiskStream _disk;

        public SyncReadGuard(TrackingDiskStream disk)
        {
            _disk = disk;
        }

        public void Dispose()
        {
            _disk.ThrowOnSyncRead = false;
        }
    }

    [Entity]
    public sealed class AsyncQueryDoc
    {
        public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
        public string Category { get; set; } = string.Empty;
        public int Value { get; set; }
        public string Payload { get; set; } = string.Empty;
    }

    private sealed class TrackingDiskStream : IDiskStream
    {
        private readonly MemoryStream _stream = new();
        private readonly object _syncRoot = new();

        public bool ThrowOnSyncRead { get; set; }
        public TimeSpan AsyncReadDelay { get; set; }
        public int SyncReadCount { get; private set; }
        public int AsyncReadCount { get; private set; }

        public string FilePath => "tracking-memory";

        public long Size
        {
            get
            {
                lock (_syncRoot)
                {
                    return _stream.Length;
                }
            }
        }

        public bool IsReadable => true;
        public bool IsWritable => true;

        public void ResetCounts()
        {
            SyncReadCount = 0;
            AsyncReadCount = 0;
        }

        public byte[] ReadPage(long pageOffset, int pageSize)
        {
            SyncReadCount++;
            if (ThrowOnSyncRead) throw new IOException("Synchronous read is not allowed during async query tests.");
            return ReadPageCore(pageOffset, pageSize);
        }

        public async Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default)
        {
            AsyncReadCount++;
            if (AsyncReadDelay > TimeSpan.Zero)
            {
                await Task.Delay(AsyncReadDelay, cancellationToken).ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            return ReadPageCore(pageOffset, pageSize);
        }

        public void WritePage(long pageOffset, byte[] pageData)
        {
            lock (_syncRoot)
            {
                if (pageOffset + pageData.Length > _stream.Length)
                {
                    _stream.SetLength(pageOffset + pageData.Length);
                }

                _stream.Position = pageOffset;
                _stream.Write(pageData, 0, pageData.Length);
            }
        }

        public Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            WritePage(pageOffset, pageData);
            return Task.CompletedTask;
        }

        public void Flush()
        {
        }

        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public void SetLength(long length)
        {
            lock (_syncRoot)
            {
                _stream.SetLength(length);
            }
        }

        public DiskStreamStatistics GetStatistics()
        {
            return new DiskStreamStatistics
            {
                FilePath = FilePath,
                Size = Size,
                Position = _stream.Position,
                IsReadable = IsReadable,
                IsWritable = IsWritable,
                IsSeekable = true
            };
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        private byte[] ReadPageCore(long pageOffset, int pageSize)
        {
            var buffer = new byte[pageSize];
            lock (_syncRoot)
            {
                if (pageOffset >= _stream.Length) return buffer;

                _stream.Position = pageOffset;
                _stream.Read(buffer, 0, pageSize);
            }

            return buffer;
        }
    }
}
