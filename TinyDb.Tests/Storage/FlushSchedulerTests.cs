using System;
using System.IO;
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
}