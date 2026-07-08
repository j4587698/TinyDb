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
            replayedPages[id] = data.ToArray();
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
            replayedPages.Add(data.ToArray());
            return Task.CompletedTask;
        });

        await Assert.That(replayedPages.Count).IsEqualTo(1);
        await Assert.That(replayedPages[0][PageHeader.Size]).IsEqualTo((byte)1);
        await Assert.That(replayedPages[0][PageHeader.Size + 1]).IsEqualTo((byte)2);
    }

    [Test]
    public async Task WAL_AsyncTransaction_Should_Coalesce_Deferred_Transaction_Page_Records()
    {
        using var wal = new WriteAheadLog(_testDbPath, 4096, true);
        var page = new Page(1, 4096, PageType.Data);

        var transactionId = Guid.NewGuid();
        await wal.WriteTransactionBeginAsync(transactionId);
        using (var tx = wal.EnterTransactionContext(transactionId, flushOnCommit: false))
        {
            page.WriteData(0, new byte[] { 1 });
            wal.AppendPageDeferred(page, beforeImage: null);

            page.WriteData(1, new byte[] { 2 });
            wal.AppendPageDeferred(page, beforeImage: null);

            await tx.CommitAsync();
        }

        var replayedPages = new List<byte[]>();
        await wal.ReplayAsync((_, data) =>
        {
            replayedPages.Add(data.ToArray());
            return Task.CompletedTask;
        });

        await Assert.That(replayedPages.Count).IsEqualTo(1);
        await Assert.That(replayedPages[0][PageHeader.Size]).IsEqualTo((byte)1);
        await Assert.That(replayedPages[0][PageHeader.Size + 1]).IsEqualTo((byte)2);
    }

    [Test]
    public async Task WAL_TransactionScope_ShouldNotHoldMutexForEntireLifetime()
    {
        using var wal = new WriteAheadLog(_testDbPath, 4096, true);
        using var tx = wal.BeginTransaction(Guid.NewGuid(), flushOnCommit: false);

        var page = new Page(2, 4096, PageType.Data);
        page.WriteData(0, new byte[] { 2 });

        using var appendStarted = new ManualResetEventSlim();
        Task appendTask;
        using (ExecutionContext.SuppressFlow())
        {
            appendTask = Task.Factory.StartNew(
                () =>
                {
                    appendStarted.Set();
                    wal.AppendPage(page);
                },
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }

        await Assert.That(appendStarted.Wait(TimeSpan.FromSeconds(1))).IsTrue();
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
