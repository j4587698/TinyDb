using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

/// <summary>
/// 回归测试：同一页的磁盘写入必须按快照顺序落盘。
/// 异步写（SavePageAsync / 后台刷盘）先拍快照、再在页锁外写盘；
/// 若不串行化，旧快照可能晚于新内容落盘，把磁盘上的页永久回退到旧版本
/// （曾导致关闭日志时大文档 "chain is incomplete" 与重启后数据损坏）。
/// </summary>
[NotInParallel]
public sealed class PageManagerWriteOrderingTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    [Test]
    public async Task SyncSave_WhileOlderAsyncSnapshotIsInFlight_DiskShouldEndWithNewestContent()
    {
        using var stream = new GatedAsyncWriteDiskStream();
        using var pageManager = new PageManager(stream, 4096);

        var page = pageManager.NewPage(PageType.Data);
        page.WriteData(0, Fill(1));

        stream.HoldNextAsyncWrite();
        var olderAsyncSave = pageManager.SavePageAsync(page);
        await stream.WaitForHeldWrite(Timeout);          // 旧快照已拍下，写盘被挂起

        page.WriteData(0, Fill(2));
        var newerSyncSave = Task.Run(() => pageManager.SavePage(page));
        await Task.WhenAny(newerSyncSave, Task.Delay(200));

        stream.ReleaseHeldWrite();
        await olderAsyncSave.WaitAsync(Timeout);
        await newerSyncSave.WaitAsync(Timeout);

        await Assert.That(Convert.ToHexString(ReadFromDisk(pageManager, page.PageID))).IsEqualTo(Convert.ToHexString(Fill(2)));
    }

    [Test]
    public async Task TwoAsyncSaves_OlderSnapshotMustNotLandLast()
    {
        using var stream = new GatedAsyncWriteDiskStream();
        using var pageManager = new PageManager(stream, 4096);

        var page = pageManager.NewPage(PageType.Data);
        page.WriteData(0, Fill(1));

        stream.HoldNextAsyncWrite();
        var olderAsyncSave = pageManager.SavePageAsync(page);
        await stream.WaitForHeldWrite(Timeout);

        page.WriteData(0, Fill(2));
        var newerAsyncSave = pageManager.SavePageAsync(page);
        await Task.WhenAny(newerAsyncSave, Task.Delay(200));

        stream.ReleaseHeldWrite();
        await olderAsyncSave.WaitAsync(Timeout);
        await newerAsyncSave.WaitAsync(Timeout);

        await Assert.That(Convert.ToHexString(ReadFromDisk(pageManager, page.PageID))).IsEqualTo(Convert.ToHexString(Fill(2)));
    }

    [Test]
    public async Task BackgroundFlush_WithOlderSnapshotInFlight_ShouldNotOverwriteNewerSyncSave()
    {
        using var stream = new GatedAsyncWriteDiskStream();
        using var pageManager = new PageManager(stream, 4096);

        var page = pageManager.NewPage(PageType.Data);
        page.WriteData(0, Fill(1));

        stream.HoldNextAsyncWrite();
        var backgroundFlush = pageManager.FlushDirtyPagesAsync();
        await stream.WaitForHeldWrite(Timeout);

        page.WriteData(0, Fill(2));
        var newerSyncSave = Task.Run(() => pageManager.SavePage(page));
        await Task.WhenAny(newerSyncSave, Task.Delay(200));

        stream.ReleaseHeldWrite();
        await backgroundFlush.WaitAsync(Timeout);
        await newerSyncSave.WaitAsync(Timeout);

        await Assert.That(Convert.ToHexString(ReadFromDisk(pageManager, page.PageID))).IsEqualTo(Convert.ToHexString(Fill(2)));
        await Assert.That(page.IsDirty).IsFalse();
    }

    private static byte[] Fill(byte value) => Enumerable.Repeat(value, 16).ToArray();

    private static byte[] ReadFromDisk(PageManager pageManager, uint pageId)
    {
        var fromDisk = pageManager.GetPage(pageId, useCache: false);
        return fromDisk.ReadData(0, 16);
    }
}
