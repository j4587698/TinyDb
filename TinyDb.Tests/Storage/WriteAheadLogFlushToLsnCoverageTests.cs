using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Storage;
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

        var mutex = GetPrivateField<SemaphoreSlim>(wal, "_mutex");
        mutex.Wait();

        var released = false;
        try
        {
            var flushTask = wal.FlushToLSNAsync(1);

            await Task.Delay(25);
            SetPrivateField(wal, "_flushedLSN", 9999L);

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

    private static string GetWalFile(string dbFile)
    {
        var directory = Path.GetDirectoryName(dbFile) ?? string.Empty;
        var name = Path.GetFileNameWithoutExtension(dbFile);
        var ext = Path.GetExtension(dbFile).TrimStart('.');
        return Path.Combine(directory, $"{name}-wal.{ext}");
    }

    private static T GetPrivateField<T>(object instance, string fieldName)
    {
        var field = instance.GetType().GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
        if (field == null) throw new InvalidOperationException($"Field '{fieldName}' not found.");
        return (T)field.GetValue(instance)!;
    }

    private static void SetPrivateField<T>(object instance, string fieldName, T value)
    {
        var field = instance.GetType().GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
        if (field == null) throw new InvalidOperationException($"Field '{fieldName}' not found.");
        field.SetValue(instance, value);
    }
}

