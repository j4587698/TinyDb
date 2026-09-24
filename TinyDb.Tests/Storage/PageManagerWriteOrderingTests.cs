using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

/// <summary>
/// 回归测试：同一页的磁盘写入必须按快照顺序落盘�?/// 异步写（SavePageAsync / 后台刷盘）先拍快照、再在页锁外写盘�?/// 若不串行化，旧快照可能晚于新内容落盘，把磁盘上的页永久回退到旧版本
/// （曾导致关闭日志时大文档 "chain is incomplete" 与重启后数据损坏）�?/// </summary>
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

    /// <summary>
    /// 内存磁盘流：可以把“下一次异步写”挂起，直到测试放行�?    /// 挂起前先复制调用方传入的字节，模拟快照已拍下、写盘尚未完成的状态�?    /// </summary>
    private sealed class GatedAsyncWriteDiskStream : IDiskStream
    {
        private readonly object _gate = new();
        private readonly MemoryStream _stream = new();
        private int _holdNextAsyncWrite;
        private TaskCompletionSource _held = NewTcs();
        private TaskCompletionSource _release = NewTcs();

        public string FilePath => "gated-memory";
        public long Size { get { lock (_gate) return _stream.Length; } }
        public bool IsReadable => true;
        public bool IsWritable => true;

        public void HoldNextAsyncWrite()
        {
            _held = NewTcs();
            _release = NewTcs();
            Volatile.Write(ref _holdNextAsyncWrite, 1);
        }

        public Task WaitForHeldWrite(TimeSpan timeout) => _held.Task.WaitAsync(timeout);

        public void ReleaseHeldWrite() => _release.TrySetResult();

        public byte[] ReadPage(long pageOffset, int pageSize)
        {
            var buffer = new byte[pageSize];
            lock (_gate)
            {
                if (pageOffset >= _stream.Length) return buffer;
                _stream.Position = pageOffset;
                _stream.ReadExactly(buffer, 0, (int)Math.Min(pageSize, _stream.Length - pageOffset));
                return buffer;
            }
        }

        public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default)
            => Task.FromResult(ReadPage(pageOffset, pageSize));

        public void WritePage(long pageOffset, byte[] pageData)
        {
            lock (_gate)
            {
                if (pageOffset + pageData.Length > _stream.Length) _stream.SetLength(pageOffset + pageData.Length);
                _stream.Position = pageOffset;
                _stream.Write(pageData, 0, pageData.Length);
            }
        }

        public async Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
        {
            var captured = (byte[])pageData.Clone();
            if (Interlocked.Exchange(ref _holdNextAsyncWrite, 0) == 1)
            {
                _held.TrySetResult();
                await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            WritePage(pageOffset, captured);
        }

        public void Flush() { }
        public Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public void SetLength(long length) { lock (_gate) _stream.SetLength(length); }

        public DiskStreamStatistics GetStatistics() => new()
        {
            FilePath = FilePath,
            Size = Size,
            Position = 0,
            IsReadable = true,
            IsWritable = true,
            IsSeekable = true
        };

        public void Dispose()
        {
            _release.TrySetResult();
            _stream.Dispose();
        }

        private static TaskCompletionSource NewTcs() => new(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
