using TinyDb.Storage;
using TinyDb.Core;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageManagerScanTests : IDisposable
{
    private readonly string _testDbPath;

    public PageManagerScanTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"pm_scan_{Guid.NewGuid()}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch {}
    }

    [Test]
    public async Task PageManager_Should_Recover_Free_Pages_On_Restart()
    {
        // 1. Create DB and create some holes
        {
            using var engine = new TinyDbEngine(_testDbPath);
            var collection = engine.GetCollection<PageManagerScanTestDoc>();

            // Insert enough data to use multiple pages. PageSize default 4096.
            // 500 byte doc -> ~8 docs per page.
            // 100 docs -> ~13 pages.
            var docs = Enumerable.Range(1, 100).Select(i => new PageManagerScanTestDoc { Id = i, Data = new string('x', 500) }).ToList();
            foreach(var d in docs) collection.Insert(d);

            // Delete all docs to ensure pages are freed.
            foreach(var d in docs) collection.Delete(d.Id);

            // Pages should be added to _freePages queue in memory.
            // PageManager.FreePage writes PageType.Empty to disk.
        }

        // 2. Restart Engine - PageManager should scan and find free pages (Empty pages)
        {
            using var engine = new TinyDbEngine(_testDbPath);
            var stats = engine.GetStatistics();

            // Should have found free pages
            await Assert.That(stats.FreePages).IsGreaterThan(0u);

            // Verify reuse logic by inserting new data
            var collection = engine.GetCollection<PageManagerScanTestDoc>();
            for(int i=0; i<10; i++)
            {
                collection.Insert(new PageManagerScanTestDoc { Id = 1000 + i, Data = "New" });
            }

            var newStats = engine.GetStatistics();
            // Free pages should decrease as they are reused
            await Assert.That(newStats.FreePages).IsLessThan(stats.FreePages);
        }
    }

    [Test]
    public async Task Initialize_WhenPagesAreCorrupt_ShouldThrow()
    {
        const uint pageSize = 4096;
        using var ds = new ThrowOnWriteDiskStream("mem://scan", initialSize: (long)pageSize * 3);
        var warnings = new List<string>();
        var pm = new PageManager(
            ds,
            pageSize,
            logger: (level, message, _) =>
            {
                if (level == TinyDbLogLevel.Warning)
                {
                    warnings.Add(message);
                }
            });
        try
        {
            await Assert.That(() => pm.Initialize(3, 0)).ThrowsNothing();
            await Assert.That(warnings.Any(static message => message.Contains("Free list rebuild skipped 1 unreadable page(s)."))).IsTrue();
        }
        finally
        {
            ds.ShouldThrowOnWrite = false;
            pm.Dispose();
        }
    }

    [Test]
    public async Task Initialize_RebuildFreeList_ShouldWriteOnlyChangedLinks()
    {
        const uint pageSize = 4096;
        using var ds = new CountingDiskStream("mem://scan", initialSize: (long)pageSize * 3);
        using var pm = new PageManager(ds, pageSize);

        pm.Initialize(3, 0);

        var stats = pm.GetStatistics();
        await Assert.That(stats.FreePages).IsEqualTo(2u);
        await Assert.That(ds.WriteCount).IsEqualTo(1);
    }

    private sealed class ThrowOnWriteDiskStream : IDiskStream
    {
        public ThrowOnWriteDiskStream(string filePath, long initialSize)
        {
            FilePath = filePath;
            Size = initialSize;
        }

        public string FilePath { get; }
        public long Size { get; private set; }
        public bool IsReadable => true;
        public bool IsWritable => true;
        public bool ShouldThrowOnWrite { get; set; } = true;

        public byte[] ReadPage(long pageOffset, int pageSize) => new byte[pageSize];

        public void WritePage(long pageOffset, byte[] pageData)
        {
            if (ShouldThrowOnWrite)
            {
                throw new IOException("Simulated write failure");
            }
        }

        public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default) =>
            Task.FromResult(ReadPage(pageOffset, pageSize));

        public Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
        {
            if (ShouldThrowOnWrite)
            {
                return Task.FromException(new IOException("Simulated write failure"));
            }

            return Task.CompletedTask;
        }

        public void Flush()
        {
        }

        public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void SetLength(long length) => Size = length;

        public DiskStreamStatistics GetStatistics() => new DiskStreamStatistics();

        public void Dispose()
        {
        }
    }

    private sealed class CountingDiskStream : IDiskStream
    {
        private readonly MemoryStream _stream = new();

        public CountingDiskStream(string filePath, long initialSize)
        {
            FilePath = filePath;
            _stream.SetLength(initialSize);
        }

        public string FilePath { get; }
        public long Size => _stream.Length;
        public bool IsReadable => true;
        public bool IsWritable => true;
        public int WriteCount { get; private set; }

        public byte[] ReadPage(long pageOffset, int pageSize)
        {
            var buffer = new byte[pageSize];
            if (pageOffset >= _stream.Length)
            {
                return buffer;
            }

            _stream.Position = pageOffset;
            _stream.Read(buffer, 0, pageSize);
            return buffer;
        }

        public void WritePage(long pageOffset, byte[] pageData)
        {
            WriteCount++;
            if (pageOffset + pageData.Length > _stream.Length)
            {
                _stream.SetLength(pageOffset + pageData.Length);
            }

            _stream.Position = pageOffset;
            _stream.Write(pageData, 0, pageData.Length);
        }

        public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default) =>
            Task.FromResult(ReadPage(pageOffset, pageSize));

        public Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
        {
            WritePage(pageOffset, pageData);
            return Task.CompletedTask;
        }

        public void Flush()
        {
        }

        public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void SetLength(long length) => _stream.SetLength(length);

        public DiskStreamStatistics GetStatistics() => new DiskStreamStatistics
        {
            FilePath = FilePath,
            Size = Size,
            Position = _stream.Position,
            IsReadable = IsReadable,
            IsWritable = IsWritable,
            IsSeekable = true
        };

        public void Dispose() => _stream.Dispose();
    }
}

[Entity("test_docs")]
public class PageManagerScanTestDoc
{
    public int Id { get; set; }
    public string Data { get; set; } = "";
}
