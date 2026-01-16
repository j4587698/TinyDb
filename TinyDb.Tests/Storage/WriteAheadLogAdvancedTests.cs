using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

[NotInParallel]
public class WriteAheadLogAdvancedTests
{
    private string _dbFile = null!;
    private const int PageSize = 8192;

    [Before(Test)]
    public void Setup()
    {
        _dbFile = Path.Combine(Path.GetTempPath(), $"wal_adv_{Guid.NewGuid():N}.db");
    }

    private string GetWalFile(string dbFile)
    {
        var directory = Path.GetDirectoryName(dbFile) ?? string.Empty;
        var name = Path.GetFileNameWithoutExtension(dbFile);
        var ext = Path.GetExtension(dbFile).TrimStart('.');
        return Path.Combine(directory, $"{name}-wal.{ext}");
    }

    [After(Test)]
    public void Cleanup()
    {
        var walFile = GetWalFile(_dbFile);
        if (File.Exists(_dbFile)) File.Delete(_dbFile);
        if (File.Exists(walFile)) File.Delete(walFile);
    }

    [Test]
    public async Task WAL_Replay_WithCorruption_Should_StopAtCorruption()
    {
        var walFile = GetWalFile(_dbFile);
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page1 = new Page(1, PageSize);
            page1.WriteData(0, new byte[] { 1, 2, 3 });
            wal.AppendPage(page1);

            var page2 = new Page(2, PageSize);
            page2.WriteData(0, new byte[] { 4, 5, 6 });
            wal.AppendPage(page2);
            
            await wal.FlushLogAsync();
        }

        // Corrupt the WAL file between entry 1 and 2
        var bytes = File.ReadAllBytes(walFile);
        // Page 1 header starts at 0. 
        // Page 1 data length is stored at index 5 (4 bytes).
        int p1DataLen = BitConverter.ToInt32(bytes, 5);
        int p2HeaderStart = 13 + p1DataLen;
        
        // Corrupt page 2 header type
        bytes[p2HeaderStart] = 0xFF; 
        File.WriteAllBytes(walFile, bytes);

        var replayedPages = new List<uint>();
        using (var wal2 = new WriteAheadLog(_dbFile, PageSize, true))
        {
            await wal2.ReplayAsync((pageId, data) => 
            {
                replayedPages.Add(pageId);
                return Task.CompletedTask;
            });
        }

        // Should only have replayed page 1
        await Assert.That(replayedPages).Count().IsEqualTo(1);
        await Assert.That(replayedPages[0]).IsEqualTo(1u);
    }

    [Test]
    public async Task WAL_Truncate_Should_Work()
    {
        var walFile = GetWalFile(_dbFile);
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page1 = new Page(1, PageSize);
            page1.WriteData(0, new byte[] { 1 });
            wal.AppendPage(page1);
            await wal.FlushLogAsync();
            
            await Assert.That(wal.HasPendingEntries).IsTrue();
            
            await wal.TruncateAsync();
            await Assert.That(wal.HasPendingEntries).IsFalse();
        }
        
        await Assert.That(new FileInfo(walFile).Length).IsEqualTo(0);
    }

    [Test]
    public async Task WAL_Synchronize_Should_FlushAndTruncate()
    {
        bool dataFlushed = false;
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page1 = new Page(1, PageSize);
            page1.WriteData(0, new byte[] { 1 });
            wal.AppendPage(page1);
            
            await wal.SynchronizeAsync(ct => 
            {
                dataFlushed = true;
                return Task.CompletedTask;
            });
            
            await Assert.That(dataFlushed).IsTrue();
            await Assert.That(wal.HasPendingEntries).IsFalse();
        }
    }

    [Test]
    public async Task WAL_AppendPageAsync_ShouldWork()
    {
        var walFile = GetWalFile(_dbFile);
        using (var wal = new WriteAheadLog(_dbFile, PageSize, true))
        {
            var page1 = new Page(1, PageSize);
            page1.WriteData(0, new byte[] { 1, 2, 3 });
            await wal.AppendPageAsync(page1); // Async call

            await wal.FlushLogAsync();
            await Assert.That(wal.HasPendingEntries).IsTrue();
        }
        
        // Verify content
        await Assert.That(new FileInfo(walFile).Length).IsGreaterThan(0);
    }
}
