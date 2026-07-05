using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Storage;
using TinyDb.Tests.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

[NotInParallel]
[SkipInAot]
public sealed class PageManagerCoverageEdgeCasesTests
{
    [Test]
    public async Task RegisterWalAsyncOverload_ShouldCoverAssignmentAndNullGuard()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"pm_regwal_{Guid.NewGuid():N}.db");
        try
        {
            long? observedLsn = null;
            using var stream = new DiskStream(dbPath);
            using var manager = new PageManager(stream, 4096);

            manager.RegisterWAL(lsn =>
            {
                observedLsn = lsn;
                return Task.CompletedTask;
            });

            var callbackField = typeof(PageManager).GetField("_flushLogToLsnAsync", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new MissingFieldException(typeof(PageManager).FullName, "_flushLogToLsnAsync");
            var callback = (Func<long, WriteAheadLog.WriteLockContext?, CancellationToken, Task>)callbackField.GetValue(manager)!;
            await callback(42L, null, CancellationToken.None);
            await Assert.That(observedLsn).IsEqualTo(42L);

            await Assert.That(() => manager.RegisterWAL((Func<long, Task>)null!))
                .Throws<ArgumentNullException>();
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
        }
    }

    [Test]
    public async Task FreePage_Should_Not_Hold_StateLock_While_Waiting_For_Wal()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"pm_freepage_wal_lock_{Guid.NewGuid():N}.db");
        var appendStarted = new ManualResetEventSlim(false);

        try
        {
            using var stream = new DiskStream(dbPath);
            using var manager = new PageManager(stream, 4096);
            using var wal = new WriteAheadLog(dbPath, 4096, enabled: true);

            manager.RegisterWAL(
                page =>
                {
                    appendStarted.Set();
                    wal.AppendPage(page);
                },
                lsn => wal.FlushToLSN(lsn));

            var page = manager.NewPage(PageType.Data);
            manager.SavePage(page);
            appendStarted.Reset();

            var mutex = UnsafeAccessors.WriteAheadLogAccessor.Mutex(wal);
            mutex.Wait();

            Task? freeTask = null;
            bool enteredStateLock = false;

            try
            {
                freeTask = Task.Run(() => manager.FreePage(page.PageID));

                await Assert.That(appendStarted.Wait(TimeSpan.FromSeconds(5))).IsTrue();

                var stateLock = UnsafeAccessors.PageManagerAccessor.StateLock(manager);
                enteredStateLock = Monitor.TryEnter(stateLock);
                if (enteredStateLock)
                {
                    Monitor.Exit(stateLock);
                }
            }
            finally
            {
                mutex.Release();
            }

            await freeTask!.WaitAsync(TimeSpan.FromSeconds(5));
            await Assert.That(enteredStateLock).IsTrue();
        }
        finally
        {
            appendStarted.Dispose();
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
            try { if (File.Exists(Path.ChangeExtension(dbPath, ".wal"))) File.Delete(Path.ChangeExtension(dbPath, ".wal")); } catch { }
        }
    }

    [Test]
    public async Task FreePage_Should_Not_Dispose_Cached_PageReference()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"pm_freepage_lifetime_{Guid.NewGuid():N}.db");

        try
        {
            using var stream = new DiskStream(dbPath);
            using var manager = new PageManager(stream, 4096);

            var page = manager.NewPage(PageType.Data);
            manager.SavePage(page);

            manager.FreePage(page.PageID);

            await Assert.That(() => page.ReadBytes(0, 1)).ThrowsNothing();
            await Assert.That(manager.CachedPages).IsEqualTo(1);
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
        }
    }

    [Test]
    public async Task PrivateLogMethod_ShouldInvokeConfiguredLogger()
    {
        var dbPath = Path.Combine(Path.GetTempPath(), $"pm_log_{Guid.NewGuid():N}.db");
        try
        {
            TinyDbLogLevel? level = null;
            string? message = null;
            Exception? captured = null;

            using var stream = new DiskStream(dbPath);
            using var manager = new PageManager(
                stream,
                4096,
                logger: (l, m, ex) =>
                {
                    level = l;
                    message = m;
                    captured = ex;
                });

            var logMethod = typeof(PageManager).GetMethod("Log", BindingFlags.NonPublic | BindingFlags.Instance)
                ?? throw new MissingMethodException(typeof(PageManager).FullName, "Log");

            var expected = new InvalidOperationException("page-manager-log");
            logMethod.Invoke(manager, new object?[] { TinyDbLogLevel.Warning, "pm-warning", expected });

            await Assert.That(level).IsEqualTo(TinyDbLogLevel.Warning);
            await Assert.That(message).IsEqualTo("pm-warning");
            await Assert.That(object.ReferenceEquals(captured, expected)).IsTrue();
        }
        finally
        {
            try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }
        }
    }

    [Test]
    public async Task GetStatistics_WithPersistedFreePageCount_ShouldNotTraverseFreeList()
    {
        var stream = new ThrowOnReadDiskStream(size: 4096L * 8L);
        using var manager = new PageManager(stream, 4096);

        manager.Initialize(totalPages: 8, firstFreePageID: 2, freePageCount: 3, hasFreePageCount: true);

        var stats = manager.GetStatistics();

        await Assert.That(stats.FreePages).IsEqualTo(3u);
    }

    [Test]
    public async Task Initialize_WithPersistedZeroFreePageCount_ShouldNotScanPages()
    {
        var stream = new ThrowOnReadDiskStream(size: 4096L * 8L);
        using var manager = new PageManager(stream, 4096);

        manager.Initialize(totalPages: 8, firstFreePageID: 0, freePageCount: 0, hasFreePageCount: true);

        var stats = manager.GetStatistics();

        await Assert.That(stats.FreePages).IsEqualTo(0u);
    }

    [Test]
    public async Task Dispose_WhenFlushAndCleanupFail_ShouldThrowAggregate()
    {
        var manager = new PageManager(new ThrowOnFlushAndDisposeDiskStream(), 4096);

        await Assert.That(() => manager.Dispose()).Throws<AggregateException>();
    }

    [Test]
    public async Task GetPageAsync_WhenDiskPageIsZeroFilled_ShouldReturnEmptyPage()
    {
        using var stream = new FakeDiskStreamBase(size: 4096);
        using var manager = new PageManager(stream, 4096);

        var page = await manager.GetPageAsync(1, useCache: false);

        await Assert.That(page.PageID).IsEqualTo(1u);
        await Assert.That(page.Header.PageType).IsEqualTo(PageType.Empty);
    }

    [Test]
    public async Task GetPageAsync_WhenPageHeaderIdMismatch_ShouldWrapWithInvalidDataException()
    {
        var stream = new FakeDiskStreamBase(size: 4096);
        var mismatched = new Page(2, 4096, PageType.Data);
        stream.WritePage(0, mismatched.Buffer);

        using var manager = new PageManager(stream, 4096);
        await Assert.That(async () => { await manager.GetPageAsync(1, useCache: false); }).Throws<InvalidDataException>();
    }

    [Test]
    public async Task GetPage_WhenCacheEntryAppearsDuringDiskRead_ShouldReturnCachedPage()
    {
        using var stream = new BlockingFirstReadDiskStream(size: 4096);
        var diskPage = new Page(1, 4096, PageType.Data);
        diskPage.WriteData(0, new byte[] { 1 });
        diskPage.UpdateChecksum();
        stream.WritePage(0, diskPage.Buffer);

        using var manager = new PageManager(stream, 4096);
        var loadingTask = Task.Run(() => manager.GetPage(1));

        await Assert.That(stream.ReadStarted.Wait(TimeSpan.FromSeconds(5))).IsTrue();

        var cachedPage = new Page(1, 4096, PageType.Data);
        cachedPage.WriteData(0, new byte[] { 9 });

        var addToCache = typeof(PageManager).GetMethod("AddToCache", BindingFlags.NonPublic | BindingFlags.Instance)
            ?? throw new MissingMethodException(typeof(PageManager).FullName, "AddToCache");
        var addedPage = (Page)addToCache.Invoke(manager, new object[] { cachedPage, false })!;

        await Assert.That(object.ReferenceEquals(addedPage, cachedPage)).IsTrue();

        stream.AllowRead.Set();
        var loadedPage = await loadingTask.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(object.ReferenceEquals(loadedPage, cachedPage)).IsTrue();
        await Assert.That(loadedPage.ReadBytes(0, 1)[0]).IsEqualTo((byte)9);
    }

    private class FakeDiskStreamBase : IDiskStream
    {
        private readonly Dictionary<long, byte[]> _pages = new();

        public FakeDiskStreamBase(long size = 0)
        {
            Size = size;
        }

        public string FilePath => "fake://page-manager";
        public long Size { get; protected set; }
        public bool IsReadable => true;
        public bool IsWritable => true;

        public virtual byte[] ReadPage(long pageOffset, int pageSize)
        {
            if (_pages.TryGetValue(pageOffset, out var data))
            {
                return data;
            }

            return new byte[pageSize];
        }

        public virtual void WritePage(long pageOffset, byte[] pageData)
        {
            _pages[pageOffset] = (byte[])pageData.Clone();
            var end = pageOffset + pageData.Length;
            if (end > Size) Size = end;
        }

        public virtual Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(ReadPage(pageOffset, pageSize));
        }

        public virtual Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
        {
            WritePage(pageOffset, pageData);
            return Task.CompletedTask;
        }

        public virtual void Flush()
        {
        }

        public virtual Task FlushAsync(CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public virtual void SetLength(long length)
        {
            Size = length;
        }

        public virtual DiskStreamStatistics GetStatistics()
        {
            return new DiskStreamStatistics
            {
                FilePath = FilePath,
                Size = Size,
                Position = 0,
                IsReadable = IsReadable,
                IsWritable = IsWritable,
                IsSeekable = true
            };
        }

        public virtual void Dispose()
        {
        }
    }

    private sealed class ThrowOnReadDiskStream : FakeDiskStreamBase
    {
        public ThrowOnReadDiskStream(long size) : base(size)
        {
        }

        public override byte[] ReadPage(long pageOffset, int pageSize)
        {
            throw new IOException("Simulated free-list read failure.");
        }
    }

    private sealed class ThrowOnFlushAndDisposeDiskStream : FakeDiskStreamBase
    {
        public override void Flush()
        {
            throw new IOException("Simulated flush failure.");
        }

        public override void Dispose()
        {
            throw new IOException("Simulated dispose failure.");
        }
    }

    private sealed class BlockingFirstReadDiskStream : FakeDiskStreamBase
    {
        private int _shouldBlock = 1;

        public BlockingFirstReadDiskStream(long size) : base(size)
        {
        }

        public ManualResetEventSlim ReadStarted { get; } = new(false);
        public ManualResetEventSlim AllowRead { get; } = new(false);

        public override byte[] ReadPage(long pageOffset, int pageSize)
        {
            var page = base.ReadPage(pageOffset, pageSize);
            if (Interlocked.Exchange(ref _shouldBlock, 0) == 1)
            {
                ReadStarted.Set();
                if (!AllowRead.Wait(TimeSpan.FromSeconds(5)))
                {
                    throw new TimeoutException("Timed out while waiting to release blocked page read.");
                }
            }

            return page;
        }

        public override void Dispose()
        {
            ReadStarted.Dispose();
            AllowRead.Dispose();
            base.Dispose();
        }
    }
}
