using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

public sealed class PageManagerEvictionConcurrencyTests
{
    [Test]
    public async Task Eviction_ShouldReleaseStripeLockWhileFlushingDirtyPage()
    {
        using var stream = new BlockingWriteDiskStream();
        using var pageManager = new PageManager(stream, 4096, 1);

        var page = pageManager.NewPage(PageType.Data);
        page.SetContent(new byte[] { 1, 2, 3 });
        pageManager.SavePage(page);
        page.UpdateStats(page.Header.FreeBytes, page.Header.ItemCount);

        stream.BlockWrites();
        var evictionTask = Task.Run(() => pageManager.NewPage(PageType.Data));

        try
        {
            await Assert.That(stream.WaitForWriteStarted(TimeSpan.FromSeconds(2))).IsTrue();

            var pinnedTask = Task.Run(() => pageManager.GetPagePinned(page.PageID));
            var completed = await Task.WhenAny(pinnedTask, Task.Delay(TimeSpan.FromSeconds(2)));

            await Assert.That(ReferenceEquals(completed, pinnedTask)).IsTrue();

            var pinnedPage = await pinnedTask;
            pinnedPage.Unpin();
        }
        finally
        {
            stream.AllowWrites();
        }

        var newPage = await evictionTask.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(newPage.PageID).IsNotEqualTo(page.PageID);
        await Assert.That(pageManager.CachedPages).IsLessThanOrEqualTo(pageManager.MaxCacheSize + 4096);
    }

    private sealed class BlockingWriteDiskStream : IDiskStream
    {
        private readonly object _gate = new();
        private readonly MemoryStream _stream = new();
        private readonly ManualResetEventSlim _writeStarted = new();
        private readonly ManualResetEventSlim _allowWrite = new(true);
        private volatile bool _blockWrites;

        public string FilePath => "blocking-memory";

        public long Size
        {
            get
            {
                lock (_gate)
                {
                    return _stream.Length;
                }
            }
        }

        public bool IsReadable => true;

        public bool IsWritable => true;

        public void BlockWrites()
        {
            _writeStarted.Reset();
            _allowWrite.Reset();
            _blockWrites = true;
        }

        public void AllowWrites()
        {
            _blockWrites = false;
            _allowWrite.Set();
        }

        public bool WaitForWriteStarted(TimeSpan timeout)
        {
            return _writeStarted.Wait(timeout);
        }

        public byte[] ReadPage(long pageOffset, int pageSize)
        {
            var buffer = new byte[pageSize];
            lock (_gate)
            {
                if (pageOffset >= _stream.Length)
                {
                    return buffer;
                }

                _stream.Position = pageOffset;
                _stream.Read(buffer, 0, pageSize);
                return buffer;
            }
        }

        public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(ReadPage(pageOffset, pageSize));
        }

        public void WritePage(long pageOffset, byte[] pageData)
        {
            if (_blockWrites)
            {
                _writeStarted.Set();
                if (!_allowWrite.Wait(TimeSpan.FromSeconds(5)))
                {
                    throw new TimeoutException("Timed out waiting for the test to release the blocked write.");
                }
            }

            lock (_gate)
            {
                if (pageOffset + pageData.Length > _stream.Length)
                {
                    _stream.SetLength(pageOffset + pageData.Length);
                }

                _stream.Position = pageOffset;
                _stream.Write(pageData, 0, pageData.Length);
            }
        }

        public Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            WritePage(pageOffset, pageData);
            return Task.CompletedTask;
        }

        public void Flush()
        {
        }

        public Task FlushAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public void SetLength(long length)
        {
            lock (_gate)
            {
                _stream.SetLength(length);
            }
        }

        public DiskStreamStatistics GetStatistics()
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

        public void Dispose()
        {
            _writeStarted.Dispose();
            _allowWrite.Dispose();
            _stream.Dispose();
        }
    }
}
