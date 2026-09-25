using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

/// <summary>
/// 回归测试：关闭时不能在仍有 IO 进行中的情况下释放 WAL 或底层文件。
/// 否则进行中的写入会撞上已释放的流/信号量，
/// 且在异步句柄上会留下指向已释放 overlapped 的完成通知。
/// </summary>
[NotInParallel]
public sealed class ShutdownOrderingTests : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"shutdown_{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        foreach (var path in new[] { _dbPath, Path.ChangeExtension(_dbPath, ".wal") })
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task WalDispose_ShouldWaitForInFlightWriteLockHolder()
    {
        var wal = new WriteAheadLog(_dbPath, 8192, enabled: true);
        wal.AppendPage(new Page(1, 8192, PageType.Data));

        var inCallback = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var synchronize = wal.SynchronizeAsync(async _ =>
        {
            inCallback.TrySetResult();
            await release.Task.ConfigureAwait(false);
        });
        await inCallback.Task.WaitAsync(Timeout);

        var dispose = Task.Run(wal.Dispose);
        var disposedWhileHolderActive = await Task.WhenAny(dispose, Task.Delay(300)) == dispose;

        release.TrySetResult();
        // 修复前：Dispose 立即释放流与信号量，持锁者回调结束后截断日志时抛 ObjectDisposedException。
        await synchronize.WaitAsync(Timeout);
        await dispose.WaitAsync(Timeout);

        await Assert.That(disposedWhileHolderActive).IsFalse();
    }

    [Test]
    public async Task WalDispose_WhenCalledTwice_ShouldBeNoOp()
    {
        var wal = new WriteAheadLog(_dbPath, 8192, enabled: true);
        wal.Dispose();
        await Assert.That(() => wal.Dispose()).ThrowsNothing();
    }

    [Test]
    public async Task StopBackgroundWriteback_ShouldWaitForInFlightWritebackAndBlockNewOnes()
    {
        using var stream = new GatedAsyncWriteDiskStream();
        using var pageManager = new PageManager(stream, 4096, 1);

        var dirty = pageManager.NewPage(PageType.Data);
        dirty.WriteData(0, new byte[] { 1, 2, 3 });

        // 缓存容量为 1 且唯一的页是脏页：再分配一页会触发后台写回，写回被挂起。
        stream.HoldNextAsyncWrite();
        var second = pageManager.NewPage(PageType.Data);
        await stream.WaitForHeldWrite(Timeout);

        var stop = Task.Run(() => pageManager.StopBackgroundWriteback(Timeout));
        var stoppedWhileWritebackActive = await Task.WhenAny(stop, Task.Delay(300)) == stop;

        stream.ReleaseHeldWrite();
        await stop.WaitAsync(Timeout);
        await Assert.That(stoppedWhileWritebackActive).IsFalse();

        // 停止后，缓存压力不应再启动新的后台写回。
        var asyncWritesAfterStop = stream.AsyncWriteCount;
        second.WriteData(0, new byte[] { 4, 5, 6 });
        pageManager.NewPage(PageType.Data);
        await Task.Delay(200);
        await Assert.That(stream.AsyncWriteCount).IsEqualTo(asyncWritesAfterStop);
    }
}
