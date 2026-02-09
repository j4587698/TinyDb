using System;
using System.IO;
using System.Linq;
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
}
