using TinyDb.Attributes;
using TinyDb.Core;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Tests.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Regression;

[NotInParallel]
public sealed class DeepAuditRegressionTests : IDisposable
{
    private readonly string _directory;

    public DeepAuditRegressionTests()
    {
        _directory = Path.Combine(Path.GetTempPath(), "TinyDbDeepAuditFixes", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_directory);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_directory))
            {
                Directory.Delete(_directory, recursive: true);
            }
        }
        catch
        {
        }
    }

    [Test]
    public async Task BsonReader_NonSeekStream_ShouldRejectEarlyContainerEnd()
    {
        byte[] malformedDocument = [12, 0, 0, 0, 0];

        using var inner = new MemoryStream(malformedDocument);
        using var stream = new NonSeekStream(inner);
        using var reader = new BsonReader(stream);

        await Assert.That(() => reader.ReadDocument()).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task WalTransactionContext_ShouldNotLeakIntoDelayedExecutionContext()
    {
        var path = Path.Combine(_directory, "wal-context.db");
        using var wal = new WriteAheadLog(path, pageSize: 4096, enabled: true);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Task appendTask;
        using (var tx = wal.BeginTransaction(Guid.NewGuid(), flushOnCommit: false))
        {
            appendTask = Task.Run(async () =>
            {
                await gate.Task.ConfigureAwait(false);

                var page = new Page(7, 4096, PageType.Data);
                page.WriteData(0, [7]);
                wal.AppendPageDeferred(page, beforeImage: null);
            });

            tx.Commit();
        }

        gate.SetResult();
        await appendTask.WaitAsync(TimeSpan.FromSeconds(5));

        var replayedPageIds = new List<uint>();
        await wal.ReplayAsync((pageId, _) =>
        {
            replayedPageIds.Add(pageId);
            return Task.CompletedTask;
        });

        await Assert.That(replayedPageIds.Contains(7)).IsTrue();
    }

    [Test]
    public async Task PageManager_DisposeFlushFailure_ShouldDisposeAndRejectRetry()
    {
        var disk = new MockDiskStream();
        var pageManager = new PageManager(disk, pageSize: 4096, maxCacheSize: 8);
        var page = pageManager.NewPage(PageType.Data);
        page.WriteData(0, [42]);

        disk.ShouldThrowOnWrite = true;
        await Assert.That(() => pageManager.Dispose()).Throws<AggregateException>();

        await Assert.That(page.IsDirty).IsTrue();

        disk.ShouldThrowOnWrite = false;
        await Assert.That(() => pageManager.FlushDirtyPages()).Throws<ObjectDisposedException>();
        await Assert.That(() => disk.ReadPage(0, 4096)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task DeleteAll_InTransaction_ShouldDeleteVisibleBaseAndPendingDocuments()
    {
        var path = Path.Combine(_directory, "delete-all-transaction.db");
        using var engine = new TinyDbEngine(path);
        var collection = engine.GetCollection<AuditDeleteDocument>();

        collection.Insert(new AuditDeleteDocument { Id = 1, Name = "base-deleted" });
        collection.Insert(new AuditDeleteDocument { Id = 2, Name = "base-visible" });

        using (var transaction = engine.BeginTransaction())
        {
            collection.Insert(new AuditDeleteDocument { Id = 3, Name = "pending-visible" });
            collection.Delete(1);

            var deleted = collection.DeleteAll();

            await Assert.That(deleted).IsEqualTo(2);
            transaction.Commit();
        }

        await Assert.That(collection.FindAll().Count()).IsEqualTo(0);
    }

    private sealed class NonSeekStream : Stream
    {
        private readonly Stream _inner;

        public NonSeekStream(Stream inner)
        {
            _inner = inner;
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => false;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override void Flush() => _inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override int Read(Span<byte> buffer) => _inner.Read(buffer);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);
    }
}

[Entity("AuditDeleteDocuments")]
public sealed class AuditDeleteDocument
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
