using TinyDb.Storage;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class WriteAheadLogTests : IDisposable
{
    private readonly string _testDbPath;

    public WriteAheadLogTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"wal_test_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
        
        var directory = Path.GetDirectoryName(_testDbPath)!;
        var fileNameNoExt = Path.GetFileNameWithoutExtension(_testDbPath);
        foreach (var file in Directory.GetFiles(directory, $"{fileNameNoExt}*"))
        {
            try { File.Delete(file); } catch { }
        }
    }

    [Test]
    public async Task WAL_Should_Record_And_Replay_Pages()
    {
        using var wal = new WriteAheadLog(_testDbPath, 4096, true);
        var page = new Page(1, 4096, PageType.Data);
        page.WriteData(0, new byte[] { 1, 2, 3 });
        
        wal.AppendPage(page);
        await Assert.That(wal.HasPendingEntries).IsTrue();
        
        var replayedPages = new Dictionary<uint, byte[]>();
        await wal.ReplayAsync((id, data) =>
        {
            replayedPages[id] = data;
            return Task.CompletedTask;
        });
        
        await Assert.That(replayedPages.Count).IsEqualTo(1);
        // Page.ReadData(0, 1) reads from PageHeader.Size
        await Assert.That(replayedPages[1][PageHeader.Size]).IsEqualTo((byte)1);
    }

    [Test]
    public async Task WAL_Truncate_Should_Clear_Entries()
    {
        using var wal = new WriteAheadLog(_testDbPath, 4096, true);
        var page = new Page(1, 4096, PageType.Data);
        wal.AppendPage(page);
        
        await wal.TruncateAsync();
        await Assert.That(wal.HasPendingEntries).IsFalse();
    }
}