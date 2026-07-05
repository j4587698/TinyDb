using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Storage;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

[NotInParallel]
public sealed class WriteAheadLogFlushToLsnCoverageTests
{
    private string _dbFile = null!;
    private const int PageSize = 4096;

    [Before(Test)]
    public void Setup()
    {
        _dbFile = Path.Combine(Path.GetTempPath(), $"wal_lsn_{Guid.NewGuid():N}.db");
    }

    [After(Test)]
    public void Cleanup()
    {
        var walFile = GetWalFile(_dbFile);
        try { if (File.Exists(_dbFile)) File.Delete(_dbFile); } catch { }
        try { if (File.Exists(walFile)) File.Delete(walFile); } catch { }
    }

    [Test]
    public async Task FlushToLSNAsync_WhenWalDisabled_ShouldReturnEarly()
    {
        using var wal = new WriteAheadLog(_dbFile, PageSize, enabled: false);

        await wal.FlushToLSNAsync(1);

        await Assert.That(wal.FlushedLSN).IsEqualTo(0L);
    }

    [Test]
    public async Task FlushToLSNAsync_ShouldFlushAndUpdateFlushedLSN()
    {
        using var wal = new WriteAheadLog(_dbFile, PageSize, enabled: true);

        var page = new Page(1, PageSize, PageType.Data);
        page.WriteData(0, new byte[] { 1 });
        wal.AppendPage(page);

        await wal.FlushToLSNAsync(1);

        await Assert.That(wal.FlushedLSN).IsGreaterThan(0L);

        // Early return: target <= flushed
        await wal.FlushToLSNAsync(0);
    }

    [Test]
    public async Task FlushToLSNAsync_ShouldSkipFlushInsideLock_WhenFlushedLSNUpdatedConcurrently()
    {
        using var wal = new WriteAheadLog(_dbFile, PageSize, enabled: true);

        var page = new Page(1, PageSize, PageType.Data);
        page.WriteData(0, new byte[] { 1 });
        wal.AppendPage(page);

        var mutex = UnsafeAccessors.WriteAheadLogAccessor.Mutex(wal);
        mutex.Wait();

        var released = false;
        try
        {
            var flushTask = wal.FlushToLSNAsync(1);

            await Task.Delay(25);
            UnsafeAccessors.WriteAheadLogAccessor.FlushedLsn(wal) = 9999L;

            mutex.Release();
            released = true;

            await flushTask;

            await Assert.That(wal.FlushedLSN).IsEqualTo(9999L);
        }
        finally
        {
            if (!released)
            {
                mutex.Release();
            }
        }
    }

    [Test]
    public async Task SynchronizeAsync_ChildAppend_ShouldNotInheritWriteLock()
    {
        using var wal = new WriteAheadLog(_dbFile, PageSize, enabled: true);
        var page = new Page(1, PageSize, PageType.Data);
        page.WriteData(0, new byte[] { 1 });

        var appendStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var appendCompleted = 0;
        Task? appendTask = null;

        await wal.SynchronizeAsync(async _ =>
        {
            appendTask = Task.Run(async () =>
            {
                appendStarted.SetResult();
                await wal.AppendPageAsync(page).ConfigureAwait(false);
                Volatile.Write(ref appendCompleted, 1);
            });

            await appendStarted.Task.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
            await Task.Delay(100).ConfigureAwait(false);

            await Assert.That(Volatile.Read(ref appendCompleted)).IsEqualTo(0);
        }, CancellationToken.None);

        await appendTask!.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);
        await Assert.That(Volatile.Read(ref appendCompleted)).IsEqualTo(1);
        await Assert.That(wal.HasPendingEntries).IsTrue();
    }

    [Test]
    public async Task Synchronize_WithLegacyPageManagerCallbacks_ShouldAllowSameThreadWalReentry()
    {
        using var disk = new DiskStream(_dbFile);
        using var pageManager = new PageManager(disk, PageSize);
        using var wal = new WriteAheadLog(_dbFile, PageSize, enabled: true);

        pageManager.RegisterWAL(
            (page, beforeImage) => wal.AppendPage(page, beforeImage),
            lsn => wal.FlushToLSN(lsn),
            () => wal.RequiresBeforeImage);

        var dirtyPage = pageManager.NewPage(PageType.Data);
        dirtyPage.WriteData(0, new byte[] { 1 });

        var flushTask = Task.Run(() => wal.Synchronize(() => pageManager.FlushDirtyPages()));
        await flushTask.WaitAsync(TimeSpan.FromSeconds(5)).ConfigureAwait(false);

        await Assert.That(wal.HasPendingEntries).IsFalse();
    }

    private static string GetWalFile(string dbFile)
    {
        var directory = Path.GetDirectoryName(dbFile) ?? string.Empty;
        var name = Path.GetFileNameWithoutExtension(dbFile);
        var ext = Path.GetExtension(dbFile).TrimStart('.');
        return Path.Combine(directory, $"{name}-wal.{ext}");
    }
}
