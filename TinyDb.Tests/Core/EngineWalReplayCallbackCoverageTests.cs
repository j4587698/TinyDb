using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineWalReplayCallbackCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _walPath;

    public EngineWalReplayCallbackCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"wal_replay_{Guid.NewGuid():N}.db");
        _walPath = GetDefaultWalPath(_dbPath);
    }

    public void Dispose()
    {
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch { }
        try { if (File.Exists(_walPath + ".bak")) File.Delete(_walPath + ".bak"); } catch { }
    }

    [Test]
    public async Task Startup_WithValidWalEntry_ShouldReplayAndClearWal()
    {
        // Ensure a valid database file exists
        using (var engine = new TinyDbEngine(_dbPath, new TinyDbOptions { EnableJournaling = false }))
        {
            var col = engine.GetBsonCollection("wal_col");
            col.Insert(new BsonDocument().Set("_id", 1).Set("val", 1));
        }

        // Create a WAL entry that replays a real page snapshot (safe no-op restore)
        var pageSize = (int)TinyDbOptions.DefaultPageSize;
        var pageBytes = new byte[pageSize];
        using (var fs = new FileStream(_dbPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            var read = fs.Read(pageBytes, 0, pageBytes.Length);
            await Assert.That(read).IsEqualTo(pageBytes.Length);
        }

        try { if (File.Exists(_walPath)) File.Delete(_walPath); } catch { }

        using (var wal = new WriteAheadLog(_dbPath, pageSize, enabled: true))
        using (var page = new Page(1, pageBytes))
        {
            wal.AppendPage(page);
            await wal.FlushLogAsync();
        }

        await Assert.That(new FileInfo(_walPath).Length).IsGreaterThan(0);

        // On startup, engine should replay WAL and truncate it
        using (var engine = new TinyDbEngine(_dbPath, new TinyDbOptions
        {
            EnableJournaling = true,
            BackgroundFlushInterval = TimeSpan.FromHours(1)
        }))
        {
            _ = engine.Header;
        }

        await Assert.That(new FileInfo(_walPath).Length).IsEqualTo(0);
    }

    private static string GetDefaultWalPath(string dbPath)
    {
        var directory = Path.GetDirectoryName(dbPath)!;
        var fileNameNoExt = Path.GetFileNameWithoutExtension(dbPath);
        var ext = Path.GetExtension(dbPath).TrimStart('.');
        return Path.Combine(directory, $"{fileNameNoExt}-wal.{ext}");
    }
}
