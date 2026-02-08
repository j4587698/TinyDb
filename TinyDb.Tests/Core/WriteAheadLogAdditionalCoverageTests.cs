using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class WriteAheadLogAdditionalCoverageTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _defaultWalPath;
    private readonly string _formatlessWalPath;
    private readonly string _emptyFormatWalPath;

    public WriteAheadLogAdditionalCoverageTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"wal_cov_{Guid.NewGuid():N}.db");
        _defaultWalPath = GetWalPath(_dbPath, "{name}-wal.{ext}");
        _formatlessWalPath = GetWalPath(_dbPath, "{name}-wal");
        _emptyFormatWalPath = Path.ChangeExtension(_dbPath, ".wal");
    }

    public void Dispose()
    {
        try { if (File.Exists(_dbPath)) File.Delete(_dbPath); } catch { }
        try { if (File.Exists(_defaultWalPath)) File.Delete(_defaultWalPath); } catch { }
        try { if (File.Exists(_formatlessWalPath)) File.Delete(_formatlessWalPath); } catch { }
        try { if (File.Exists(_emptyFormatWalPath)) File.Delete(_emptyFormatWalPath); } catch { }
    }

    [Test]
    public async Task Constructor_WithNullOrWhitespacePath_ShouldThrow()
    {
        await Assert.That(() => new WriteAheadLog(null!, 8192, enabled: true)).Throws<ArgumentException>();
        await Assert.That(() => new WriteAheadLog("   ", 8192, enabled: true)).Throws<ArgumentException>();
    }

    [Test]
    public async Task GenerateWalFilePath_WithEmptyFormat_ShouldUseWalExtension()
    {
        using (var wal = new WriteAheadLog(_dbPath, 8192, enabled: true, walFileNameFormat: ""))
        {
            await Assert.That(wal.IsEnabled).IsTrue();
        }

        await Assert.That(File.Exists(_emptyFormatWalPath)).IsTrue();
    }

    [Test]
    public async Task GenerateWalFilePath_WithoutExtension_ShouldAppendDatabaseExtension()
    {
        using (var wal = new WriteAheadLog(_dbPath, 8192, enabled: true, walFileNameFormat: "{name}-wal"))
        {
            await Assert.That(wal.IsEnabled).IsTrue();
        }

        await Assert.That(File.Exists(_formatlessWalPath)).IsTrue();
    }

    [Test]
    public async Task WhenDisabled_ShouldDeleteExistingWalFile()
    {
        File.WriteAllText(_defaultWalPath, "x");
        await Assert.That(File.Exists(_defaultWalPath)).IsTrue();

        using (var wal = new WriteAheadLog(_dbPath, 8192, enabled: false))
        {
            await Assert.That(wal.IsEnabled).IsFalse();
        }

        await Assert.That(File.Exists(_defaultWalPath)).IsFalse();
    }

    [Test]
    public async Task WhenDisabled_DeleteFailure_ShouldBeSwallowed()
    {
        File.WriteAllText(_defaultWalPath, "x");
        await Assert.That(File.Exists(_defaultWalPath)).IsTrue();

        using (new FileStream(_defaultWalPath, FileMode.Open, FileAccess.Read, FileShare.None))
        {
            using (var wal = new WriteAheadLog(_dbPath, 8192, enabled: false))
            {
                await Assert.That(wal.IsEnabled).IsFalse();
            }
        }

        await Assert.That(File.Exists(_defaultWalPath)).IsTrue();
    }

    [Test]
    public async Task GenerateWalFilePath_WhenDatabasePathHasNoDirectory_ShouldUseCurrentDirectory()
    {
        var dbFileNameOnly = $"wal_rel_{Guid.NewGuid():N}.db";
        var expectedWal = GetWalPath(dbFileNameOnly, "{name}-wal.{ext}");

        try { if (File.Exists(expectedWal)) File.Delete(expectedWal); } catch { }

        using var wal = new WriteAheadLog(dbFileNameOnly, 8192, enabled: false, walFileNameFormat: "{name}-wal.{ext}");
        await Assert.That(wal.IsEnabled).IsFalse();
    }

    [Test]
    public async Task AppendFlushSynchronizeTruncateAndDispose_ShouldCoverBranches()
    {
        using var wal = new WriteAheadLog(_dbPath, 8192, enabled: true, walFileNameFormat: "{name}-wal.{ext}");

        await wal.FlushLogAsync(); // no pending entries

        var page = new Page(1, 8192, PageType.Data);
        wal.AppendPage(page);
        await Assert.That(wal.HasPendingEntries).IsTrue();

        var called = 0;
        await wal.SynchronizeAsync(_ =>
        {
            called++;
            return Task.CompletedTask;
        });

        await Assert.That(called).IsEqualTo(1);
        await Assert.That(wal.HasPendingEntries).IsFalse();

        await wal.TruncateAsync();
        await wal.FlushLogAsync();

        wal.Dispose();
        wal.Dispose(); // idempotent
    }

    [Test]
    public async Task SynchronizeAsync_WithNullDelegate_ShouldThrow()
    {
        using var wal = new WriteAheadLog(_dbPath, 8192, enabled: false);
        await Assert.That(() => wal.SynchronizeAsync(null!)).Throws<ArgumentNullException>();
    }

    private static string GetWalPath(string dbPath, string format)
    {
        var directory = Path.GetDirectoryName(dbPath) ?? string.Empty;
        var fileNameWithoutExt = Path.GetFileNameWithoutExtension(dbPath);
        var extension = Path.GetExtension(dbPath).TrimStart('.');

        var formattedFileName = format
            .Replace("{name}", fileNameWithoutExt)
            .Replace("{ext}", extension);

        if (!Path.HasExtension(formattedFileName) && !string.IsNullOrEmpty(extension))
        {
            formattedFileName += $".{extension}";
        }

        return Path.Combine(directory, formattedFileName);
    }
}
