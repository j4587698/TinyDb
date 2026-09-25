using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

/// <summary>
/// 回归测试：WAL 在把日志标记为已持久化之前，必须真正刷到磁盘（FlushFileBuffers）。
/// FileStream.FlushAsync 只把托管缓冲区交给操作系统，不会落盘；
/// 曾导致异步事务提交与“日志先于数据”的异步写盘路径在断电时丢失已确认的日志。
/// </summary>
public sealed class WriteAheadLogDurabilityTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"wal_durability_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var path in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task FlushToLSNAsync_ShouldFlushToDiskBeforeMarkingLogDurable()
    {
        using var wal = new WriteAheadLog(_dbPath, 8192, enabled: true);
        wal.AppendPage(new Page(1, 8192, PageType.Data));
        var before = wal.DiskFlushCount;

        await wal.FlushToLSNAsync(long.MaxValue);

        await Assert.That(wal.DiskFlushCount).IsGreaterThan(before);
        await Assert.That(wal.FlushedLSN).IsGreaterThan(0);
    }

    [Test]
    public async Task FlushLogAsync_ShouldFlushToDisk()
    {
        using var wal = new WriteAheadLog(_dbPath, 8192, enabled: true);
        wal.AppendPage(new Page(1, 8192, PageType.Data));
        var before = wal.DiskFlushCount;

        await wal.FlushLogAsync();

        await Assert.That(wal.DiskFlushCount).IsGreaterThan(before);
    }

    [Test]
    public async Task TruncateAsync_ShouldFlushToDisk()
    {
        using var wal = new WriteAheadLog(_dbPath, 8192, enabled: true);
        wal.AppendPage(new Page(1, 8192, PageType.Data));
        var before = wal.DiskFlushCount;

        await wal.TruncateAsync();

        await Assert.That(wal.DiskFlushCount).IsGreaterThan(before);
        await Assert.That(wal.HasPendingEntries).IsFalse();
    }

    // 对照组：同步路径本来就会落盘。
    [Test]
    public async Task SyncFlushPaths_ShouldFlushToDisk()
    {
        using var wal = new WriteAheadLog(_dbPath, 8192, enabled: true);

        wal.AppendPage(new Page(1, 8192, PageType.Data));
        var beforeFlushLog = wal.DiskFlushCount;
        wal.FlushLog();
        await Assert.That(wal.DiskFlushCount).IsGreaterThan(beforeFlushLog);

        wal.AppendPage(new Page(2, 8192, PageType.Data));
        var beforeFlushToLsn = wal.DiskFlushCount;
        wal.FlushToLSN(long.MaxValue);
        await Assert.That(wal.DiskFlushCount).IsGreaterThan(beforeFlushToLsn);
    }
}
