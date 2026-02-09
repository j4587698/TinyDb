using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

[NotInParallel]
public class FlushSchedulerTests
{
    private string _dbFile = null!;
    private DiskStream _diskStream = null!;
    private PageManager _pageManager = null!;
    private WriteAheadLog _wal = null!;

    [Before(Test)]
    public void Setup()
    {
        _dbFile = Path.Combine(Path.GetTempPath(), $"fs_test_{Guid.NewGuid():N}.db");
        _diskStream = new DiskStream(_dbFile);
        _pageManager = new PageManager(_diskStream);
        _wal = new WriteAheadLog(_dbFile, 8192, true);
    }

    [After(Test)]
    public void Cleanup()
    {
        _wal.Dispose();
        _pageManager.Dispose();
        _diskStream.Dispose();
        if (File.Exists(_dbFile)) File.Delete(_dbFile);
        var walFile = Path.Combine(Path.GetDirectoryName(_dbFile)!, $"{Path.GetFileNameWithoutExtension(_dbFile)}-wal.db");
        if (File.Exists(walFile)) File.Delete(walFile);
    }

    [Test]
    public async Task FlushScheduler_EnsureDurability_ShouldWork()
    {
        using var fs = new FlushScheduler(_pageManager, _wal, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(10));
        
        var page = _pageManager.GetPage(1);
        page.WriteData(0, new byte[] { 1 });
        _pageManager.SavePage(page);
        _wal.AppendPage(page);
        
        // Test Journaled
        await fs.EnsureDurabilityAsync(WriteConcern.Journaled);
        await Assert.That(_wal.HasPendingEntries).IsTrue();
        
        // Test Synced
        await fs.EnsureDurabilityAsync(WriteConcern.Synced);
        await Assert.That(_wal.HasPendingEntries).IsFalse();
    }

    [Test]
    public async Task FlushScheduler_Ctor_WithNullArgs_ShouldThrow()
    {
        await Assert.That(() => new FlushScheduler(null!, _wal, TimeSpan.Zero, TimeSpan.Zero)).Throws<ArgumentNullException>();
        await Assert.That(() => new FlushScheduler(_pageManager, null!, TimeSpan.Zero, TimeSpan.Zero)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task FlushScheduler_EnsureDurability_None_And_Invalid_ShouldCoverBranches()
    {
        using var fs = new FlushScheduler(_pageManager, _wal, TimeSpan.Zero, TimeSpan.Zero);

        await fs.EnsureDurabilityAsync(WriteConcern.None, CancellationToken.None);

        await Assert.That(() => fs.EnsureDurabilityAsync((WriteConcern)123, CancellationToken.None))
            .ThrowsExactly<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task FlushScheduler_Journaled_WhenWalDisabled_ShouldFallbackToPageFlush()
    {
        using var walDisabled = new WriteAheadLog(_dbFile, 8192, false);
        using var fs = new FlushScheduler(_pageManager, walDisabled, TimeSpan.Zero, TimeSpan.Zero);

        await fs.EnsureDurabilityAsync(WriteConcern.Journaled, CancellationToken.None);
    }

    [Test]
    public async Task FlushScheduler_Journaled_WhenDelayCanceledAndNoPendingEntries_ShouldNotThrow()
    {
        await Assert.That(_wal.HasPendingEntries).IsFalse();

        using var fs = new FlushScheduler(_pageManager, _wal, TimeSpan.Zero, TimeSpan.FromMilliseconds(50));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await fs.EnsureDurabilityAsync(WriteConcern.Journaled, cts.Token);
    }

    [Test]
    public async Task FlushScheduler_Dispose_Twice_ShouldReturnEarly()
    {
        var fs = new FlushScheduler(_pageManager, _wal, TimeSpan.Zero, TimeSpan.Zero);
        fs.Dispose();

        await Assert.That(() => fs.Dispose()).ThrowsNothing();
    }

    [Test]
    public async Task FlushScheduler_BackgroundLoop_ShouldWork()
    {
        using var fs = new FlushScheduler(_pageManager, _wal, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(10));
        
        var page = _pageManager.GetPage(1);
        page.WriteData(0, new byte[] { 1 });
        _pageManager.SavePage(page);
        _wal.AppendPage(page);
        
        await Assert.That(_wal.HasPendingEntries).IsTrue();
        
        // Wait for background loop to run (it calls FlushPendingAsync -> SynchronizeAsync)
        await Task.Delay(500);
        
        await Assert.That(_wal.HasPendingEntries).IsFalse();
    }

    [Test]
    public async Task FlushScheduler_FlushAsync_ShouldWork()
    {
        using var fs = new FlushScheduler(_pageManager, _wal, TimeSpan.Zero, TimeSpan.Zero);
        var page = _pageManager.GetPage(1);
        page.WriteData(0, new byte[] { 1 });
        _pageManager.SavePage(page);
        _wal.AppendPage(page);
        
        await fs.FlushAsync();
        await Assert.That(_wal.HasPendingEntries).IsFalse();
    }

    [Test]
    public async Task FlushScheduler_BackgroundLoop_WhenFlushThrows_ShouldSwallowException()
    {
        var dbFile = Path.Combine(Path.GetTempPath(), $"fs_throw_{Guid.NewGuid():N}.db");
        try
        {
            using var throwingStream = new ThrowingWriteAsyncDiskStream(new DiskStream(dbFile));
            using var pageManager = new PageManager(throwingStream);
            using var walDisabled = new WriteAheadLog(dbFile, 8192, false);

            using var fs = new FlushScheduler(pageManager, walDisabled, TimeSpan.FromMilliseconds(25), TimeSpan.Zero);

            var page = pageManager.GetPage(1);
            page.WriteData(0, new byte[] { 1 }); // Mark dirty; don't save, let background loop try

            await Task.Delay(250);
        }
        finally
        {
            try { if (File.Exists(dbFile)) File.Delete(dbFile); } catch { }
            var walFile = Path.Combine(Path.GetDirectoryName(dbFile)!, $"{Path.GetFileNameWithoutExtension(dbFile)}-wal.db");
            try { if (File.Exists(walFile)) File.Delete(walFile); } catch { }
        }
    }

    [Test]
    public async Task FlushScheduler_BackgroundLoop_WhenHasDirtyPagesThrows_ShouldSwallowException()
    {
        using var walDisabled = new WriteAheadLog(_dbFile, 8192, false);
        using var fs = new FlushScheduler(_pageManager, walDisabled, TimeSpan.FromMilliseconds(10), TimeSpan.Zero);

        _pageManager.Dispose();

        // Give the background loop a chance to run and hit the catch-all branch.
        await Task.Delay(200);
    }

    private sealed class ThrowingWriteAsyncDiskStream : IDiskStream
    {
        private readonly IDiskStream _inner;

        public ThrowingWriteAsyncDiskStream(IDiskStream inner)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        }

        public string FilePath => _inner.FilePath;
        public long Size => _inner.Size;
        public bool IsReadable => _inner.IsReadable;
        public bool IsWritable => _inner.IsWritable;

        public byte[] ReadPage(long pageOffset, int pageSize) => _inner.ReadPage(pageOffset, pageSize);
        public void WritePage(long pageOffset, byte[] pageData) => _inner.WritePage(pageOffset, pageData);

        public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default)
            => _inner.ReadPageAsync(pageOffset, pageSize, cancellationToken);

        public Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("Simulated WritePageAsync failure");

        public void Flush() => _inner.Flush();
        public Task FlushAsync(CancellationToken cancellationToken = default) => _inner.FlushAsync(cancellationToken);
        public void SetLength(long length) => _inner.SetLength(length);
        public DiskStreamStatistics GetStatistics() => _inner.GetStatistics();

        public void Dispose() => _inner.Dispose();
    }
}
