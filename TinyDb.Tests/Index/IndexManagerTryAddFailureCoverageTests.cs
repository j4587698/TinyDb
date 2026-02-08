using System;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Index;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public sealed class IndexManagerTryAddFailureCoverageTests
{
    [Test]
    public async Task CreateIndex_WhenTryAddFails_ShouldDisposeAndReturnFalse()
    {
        var tempFilePath = Path.Combine(Path.GetTempPath(), $"idx_mgr_tryadd_{Guid.NewGuid():N}.db");

        using var writeStarted = new ManualResetEventSlim(false);
        using var allowWrite = new ManualResetEventSlim(false);

        using var pm = new PageManager(new BlockingDiskStream(new DiskStream(tempFilePath), writeStarted, allowWrite),
            pageSize: 4096,
            maxCacheSize: 64);

        using var manager = new IndexManager("col", pm);

        var indexes = GetIndexes(manager);

        using var dummyIndex = new BTreeIndex("dummy", new[] { "a" }, unique: false, maxKeys: 4);

        var createTask = Task.Run(() => manager.CreateIndex("idx", new[] { "A" }, unique: false));

        try
        {
            await Assert.That(writeStarted.Wait(TimeSpan.FromSeconds(5))).IsTrue();
            await Assert.That(indexes.TryAdd("idx", dummyIndex)).IsTrue();
        }
        finally
        {
            allowWrite.Set();
        }

        var result = await createTask;

        await Assert.That(result).IsFalse();

        indexes.TryRemove("idx", out _);

        try { if (File.Exists(tempFilePath)) File.Delete(tempFilePath); } catch { }
    }

    private static ConcurrentDictionary<string, BTreeIndex> GetIndexes(IndexManager manager)
    {
        var field = typeof(IndexManager).GetField("_indexes", BindingFlags.NonPublic | BindingFlags.Instance);
        if (field == null) throw new InvalidOperationException("IndexManager._indexes not found.");

        return (ConcurrentDictionary<string, BTreeIndex>)field.GetValue(manager)!;
    }

    private sealed class BlockingDiskStream : IDiskStream
    {
        private readonly IDiskStream _inner;
        private readonly ManualResetEventSlim _writeStarted;
        private readonly ManualResetEventSlim _allowWrite;
        private int _blocked;

        public BlockingDiskStream(IDiskStream inner, ManualResetEventSlim writeStarted, ManualResetEventSlim allowWrite)
        {
            _inner = inner ?? throw new ArgumentNullException(nameof(inner));
            _writeStarted = writeStarted ?? throw new ArgumentNullException(nameof(writeStarted));
            _allowWrite = allowWrite ?? throw new ArgumentNullException(nameof(allowWrite));
        }

        public string FilePath => _inner.FilePath;
        public long Size => _inner.Size;
        public bool IsReadable => _inner.IsReadable;
        public bool IsWritable => _inner.IsWritable;

        public byte[] ReadPage(long pageOffset, int pageSize) => _inner.ReadPage(pageOffset, pageSize);

        public void WritePage(long pageOffset, byte[] pageData)
        {
            if (Interlocked.CompareExchange(ref _blocked, 1, 0) == 0)
            {
                _writeStarted.Set();
                _allowWrite.Wait(TimeSpan.FromSeconds(5));
            }

            _inner.WritePage(pageOffset, pageData);
        }

        public Task<byte[]> ReadPageAsync(long pageOffset, int pageSize, CancellationToken cancellationToken = default) =>
            _inner.ReadPageAsync(pageOffset, pageSize, cancellationToken);

        public async Task WritePageAsync(long pageOffset, byte[] pageData, CancellationToken cancellationToken = default)
        {
            if (Interlocked.CompareExchange(ref _blocked, 1, 0) == 0)
            {
                _writeStarted.Set();
                _allowWrite.Wait(TimeSpan.FromSeconds(5));
            }

            await _inner.WritePageAsync(pageOffset, pageData, cancellationToken);
        }

        public void Flush() => _inner.Flush();

        public Task FlushAsync(CancellationToken cancellationToken = default) => _inner.FlushAsync(cancellationToken);

        public void SetLength(long length) => _inner.SetLength(length);

        public DiskStreamStatistics GetStatistics() => _inner.GetStatistics();

        public void Dispose() => _inner.Dispose();
    }
}

