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
        
        wal.Truncate();
        await Assert.That(wal.HasPendingEntries).IsFalse();

        wal.AppendPage(page);
        await wal.TruncateAsync();
        await Assert.That(wal.HasPendingEntries).IsFalse();
    }

    [Test]
    public async Task WAL_Should_Coalesce_Deferred_Transaction_Page_Records()
    {
        using var wal = new WriteAheadLog(_testDbPath, 4096, true);
        var page = new Page(1, 4096, PageType.Data);

        using (var tx = wal.BeginTransaction(Guid.NewGuid(), flushOnCommit: false))
        {
            page.WriteData(0, new byte[] { 1 });
            wal.AppendPageDeferred(page, beforeImage: null);

            page.WriteData(1, new byte[] { 2 });
            wal.AppendPageDeferred(page, beforeImage: null);

            tx.Commit();
        }

        var replayedPages = new List<byte[]>();
        await wal.ReplayAsync((_, data) =>
        {
            replayedPages.Add(data);
            return Task.CompletedTask;
        });

        await Assert.That(replayedPages.Count).IsEqualTo(1);
        await Assert.That(replayedPages[0][PageHeader.Size]).IsEqualTo((byte)1);
        await Assert.That(replayedPages[0][PageHeader.Size + 1]).IsEqualTo((byte)2);
    }

    [Test]
    public async Task WAL_Should_Restore_Coalesced_Deferred_Page_When_Transaction_Does_Not_Commit()
    {
        using var wal = new WriteAheadLog(_testDbPath, 4096, true);
        var page = new Page(1, 4096, PageType.Data);
        page.WriteData(0, new byte[] { 1 });
        var beforeImage = page.Snapshot();

        wal.BeforeTransactionCommitForTesting = () => throw new IOException("commit failed");
        using (var tx = wal.BeginTransaction(Guid.NewGuid(), flushOnCommit: false))
        {
            page.WriteData(0, new byte[] { 9 });
            wal.AppendPageDeferred(page, beforeImage);

            await Assert.That(() => tx.Commit()).Throws<IOException>();
        }

        wal.BeforeTransactionCommitForTesting = null;

        var restoredPages = new Dictionary<uint, byte[]>();
        await wal.ReplayAsync(
            (_, _) => Task.CompletedTask,
            (id, data) =>
            {
                restoredPages[id] = data;
                return Task.CompletedTask;
            });

        await Assert.That(restoredPages.Count).IsEqualTo(1);
        await Assert.That(restoredPages[1][PageHeader.Size]).IsEqualTo((byte)1);
    }

    [Test]
    public async Task WAL_TransactionScope_ShouldNotHoldMutexForEntireLifetime()
    {
        using var wal = new WriteAheadLog(_testDbPath, 4096, true);
        using var tx = wal.BeginTransaction(Guid.NewGuid(), flushOnCommit: false);

        var page = new Page(2, 4096, PageType.Data);
        page.WriteData(0, new byte[] { 2 });

        Task appendTask;
        using (ExecutionContext.SuppressFlow())
        {
            appendTask = Task.Run(() => wal.AppendPage(page));
        }

        var completed = await Task.WhenAny(appendTask, Task.Delay(TimeSpan.FromSeconds(1))) == appendTask;
        await Assert.That(completed).IsTrue();
        await appendTask;

        tx.Commit();
    }

    [Test]
    public async Task PageManager_FlushDirtyPages_ShouldSkipUncommittedDeferredWalPage()
    {
        uint pageId;

        using (var stream = new DiskStream(_testDbPath))
        using (var pageManager = new PageManager(stream, 4096))
        using (var wal = new WriteAheadLog(_testDbPath, 4096, true))
        {
            pageManager.RegisterWAL(
                (page, beforeImage) => wal.AppendPage(page, beforeImage),
                lsn => wal.FlushToLSN(lsn),
                () => wal.RequiresBeforeImage);
            pageManager.RegisterDeferredWAL((page, beforeImage) => wal.AppendPageDeferred(page, beforeImage));
            wal.DeferredTransactionPageLogged = pageManager.MarkDeferredWalPageLogged;

            var page = pageManager.NewPage(PageType.Data);
            pageId = page.PageID;
            page.WriteData(0, new byte[] { 1 });
            pageManager.SavePage(page, forceFlush: true);

            using (wal.BeginTransaction(Guid.NewGuid(), flushOnCommit: false))
            {
                var beforeImage = pageManager.CaptureBeforeImageForWal(page);
                page.WriteData(0, new byte[] { 9 });
                pageManager.SavePageDeferred(page, beforeImage);

                pageManager.FlushDirtyPages();
            }
        }

        using var readStream = new DiskStream(_testDbPath);
        using var readManager = new PageManager(readStream, 4096);
        var readPage = readManager.GetPage(pageId, useCache: false);

        await Assert.That(readPage.Buffer[PageHeader.Size]).IsEqualTo((byte)1);
    }
}
