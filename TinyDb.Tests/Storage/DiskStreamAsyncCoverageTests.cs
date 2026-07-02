using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

[NotInParallel]
public sealed class DiskStreamAsyncCoverageTests
{
    [Test]
    public async Task WritePageAsync_ShouldWriteBytes()
    {
        var path = Path.Combine(Path.GetTempPath(), $"diskstream_async_{Guid.NewGuid():N}.db");

        try
        {
            using var stream = new DiskStream(path, FileAccess.ReadWrite, FileShare.ReadWrite);

            var data = new byte[] { 1, 2, 3, 4 };
            await stream.WritePageAsync(0, data);
            await stream.FlushAsync();

            stream.Seek(0, SeekOrigin.Begin);
            var read = new byte[data.Length];
            var bytesRead = stream.Read(read, 0, read.Length);

            await Assert.That(bytesRead).IsEqualTo(data.Length);
            await Assert.That(read.SequenceEqual(data)).IsTrue();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task PageReadWriteAsync_ShouldNotMovePosition()
    {
        var path = Path.Combine(Path.GetTempPath(), $"diskstream_async_position_{Guid.NewGuid():N}.db");

        try
        {
            using var stream = new DiskStream(path, FileAccess.ReadWrite, FileShare.ReadWrite);
            stream.SetLength(32);
            stream.Seek(9, SeekOrigin.Begin);

            var data = new byte[] { 5, 6, 7, 8 };
            await stream.WritePageAsync(12, data);
            var read = await stream.ReadPageAsync(12, data.Length);

            await Assert.That(stream.Position).IsEqualTo(9);
            await Assert.That(read.SequenceEqual(data)).IsTrue();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task PageReadWriteAsync_ShouldUseFileSemaphore()
    {
        var path = Path.Combine(Path.GetTempPath(), $"diskstream_async_lock_{Guid.NewGuid():N}.db");

        try
        {
            using var stream = new DiskStream(path, FileAccess.ReadWrite, FileShare.ReadWrite);
            var semaphoreField = typeof(DiskStream).GetField("_semaphore", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
            var semaphore = (SemaphoreSlim)semaphoreField!.GetValue(stream)!;
            var data = new byte[] { 1, 2, 3, 4 };

            await semaphore.WaitAsync();
            Task writeTask = Task.CompletedTask;
            try
            {
                writeTask = stream.WritePageAsync(0, data);
                await Task.Delay(50);
                await Assert.That(writeTask.IsCompleted).IsFalse();
            }
            finally
            {
                semaphore.Release();
            }

            await writeTask;

            await semaphore.WaitAsync();
            Task<byte[]> readTask = Task.FromResult(Array.Empty<byte>());
            try
            {
                readTask = stream.ReadPageAsync(0, data.Length);
                await Task.Delay(50);
                await Assert.That(readTask.IsCompleted).IsFalse();
            }
            finally
            {
                semaphore.Release();
            }

            var read = await readTask;
            await Assert.That(read.SequenceEqual(data)).IsTrue();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }
}
