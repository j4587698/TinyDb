using System.Collections.Concurrent;
using System.Threading.Tasks;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class PageInternalsTests
{
    [Test]
    public async Task Page_Metadata_Update_Should_Work()
    {
        var page = new Page(1, 4096, PageType.Data);
        var originalTime = page.Header.ModifiedAt;
        
        await Task.Delay(10);
        page.UpdateStats(100, 2);
        
        await Assert.That(page.Header.FreeBytes).IsEqualTo((ushort)100);
        await Assert.That(page.Header.ItemCount).IsEqualTo((ushort)2);
        await Assert.That(page.Header.ModifiedAt).IsGreaterThan(originalTime);
        await Assert.That(page.IsDirty).IsTrue();
    }

    [Test]
    public async Task Page_ClearData_Should_Reset_State()
    {
        var page = new Page(1, 4096, PageType.Data);
        page.WriteData(0, new byte[] { 1, 2, 3 });
        
        page.ClearData();
        
        await Assert.That(page.PageType).IsEqualTo(PageType.Empty);
        await Assert.That(page.Header.ItemCount).IsEqualTo((ushort)0);
        await Assert.That(page.ReadData(0, 3).All(b => b == 0)).IsTrue();
    }

    [Test]
    public async Task Page_Checksum_Verification_Should_Work()
    {
        var page = new Page(1, 4096, PageType.Data);
        page.WriteData(0, new byte[] { 1, 2, 3 });
        page.UpdateChecksum();
        
        await Assert.That(page.VerifyIntegrity()).IsTrue();
    }

    /// <summary>
    /// 脏页跟踪核心保证：MarkClean() 必须在翻转 IsDirty 的同时触发 clean 回调，
    /// 使外部脏页集合的移除与 IsDirty=false 原子发生。若 MarkClean 不调回调（旧的有 bug 实现），
    /// 会出现 IsDirty=false 但集合仍残留条目，破坏 IsDirty⟺在集合中 的不变式。
    /// </summary>
    [Test]
    public async Task MarkClean_Should_Atomically_Invoke_CleanCallback()
    {
        var page = new Page(7, 4096, PageType.Data);
        var dirtySet = new ConcurrentDictionary<uint, byte>();
        page.SetDirtyCallback(p => dirtySet[p.PageID] = 0, p => dirtySet.TryRemove(p.PageID, out _));

        // 写入触发 MarkDirty → dirty 回调登记
        page.WriteData(0, new byte[] { 1, 2, 3 });
        await Assert.That(page.IsDirty).IsTrue();
        await Assert.That(dirtySet.ContainsKey(page.PageID)).IsTrue();

        // MarkClean 必须同时翻转 IsDirty 与移除集合条目
        page.MarkClean();
        await Assert.That(page.IsDirty).IsFalse();
        await Assert.That(dirtySet.ContainsKey(page.PageID)).IsFalse();
    }

    [Test]
    public async Task MarkCleanIfGeneration_Should_Not_Clear_Newer_Dirty_Write()
    {
        var page = new Page(8, 4096, PageType.Data);
        var dirtySet = new ConcurrentDictionary<uint, byte>();
        page.SetDirtyCallback(p => dirtySet[p.PageID] = 0, p => dirtySet.TryRemove(p.PageID, out _));

        page.WriteData(0, new byte[] { 1 });
        _ = page.SnapshotForDiskWrite(out var flushedGeneration);

        page.WriteData(1, new byte[] { 2 });

        var cleaned = page.MarkCleanIfGeneration(flushedGeneration);

        await Assert.That(cleaned).IsFalse();
        await Assert.That(page.IsDirty).IsTrue();
        await Assert.That(dirtySet.ContainsKey(page.PageID)).IsTrue();
    }
}
