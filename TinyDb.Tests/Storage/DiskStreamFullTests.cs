using System;
using System.IO;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Storage;

public class DiskStreamFullTests
{
    [Test]
    public async Task DiskStream_LockUnlock_ShouldWork()
    {
        var file = Path.GetTempFileName();
        try 
        {
            using var ds = new DiskStream(file, FileAccess.ReadWrite, FileShare.ReadWrite);
            
            var handle = ds.LockRegion(0, 100);
            await Assert.That(handle).IsNotNull();
            
            ds.UnlockRegion(handle);
            
            await Assert.That(() => ds.UnlockRegion(new object())).Throws<ArgumentException>();
        }
        finally
        {
            if (File.Exists(file)) File.Delete(file);
        }
    }

    [Test]
    public async Task DiskStream_SetLength_ShouldWork()
    {
        var file = Path.GetTempFileName();
        try 
        {
            using var ds = new DiskStream(file);
            ds.SetLength(1024);
            await Assert.That(ds.Size).IsEqualTo(1024L);
            
            ds.Write(new byte[10], 0, 10);
            await Assert.That(ds.Position).IsEqualTo(10L);
        }
        finally
        {
            if (File.Exists(file)) File.Delete(file);
        }
    }

    [Test]
    public async Task DiskStream_Statistics_ShouldWork()
    {
        var file = Path.GetTempFileName();
        try 
        {
            using var ds = new DiskStream(file);
            var stats = ds.GetStatistics();
            await Assert.That(stats.FilePath).IsEqualTo(file);
            await Assert.That(stats.ToString()).Contains(file);
        }
        finally
        {
            if (File.Exists(file)) File.Delete(file);
        }
    }
}
