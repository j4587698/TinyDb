using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class DiskStreamTests : IDisposable
{
    private readonly string _testDbPath;

    public DiskStreamTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"ds_test_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Properties_Should_Reflect_FileStream()
    {
        using var ds = new DiskStream(_testDbPath);
        await Assert.That(ds.IsReadable).IsTrue();
        await Assert.That(ds.IsWritable).IsTrue();
        await Assert.That(ds.IsSeekable).IsTrue();
        await Assert.That(ds.FilePath).IsEqualTo(_testDbPath);
        await Assert.That(ds.ToString()).Contains(_testDbPath);
    }

    [Test]
    public async Task SetLength_Should_Change_File_Size()
    {
        using var ds = new DiskStream(_testDbPath);
        ds.SetLength(1000);
        await Assert.That(ds.Size).IsEqualTo(1000);
        
        var info = new FileInfo(_testDbPath);
        await Assert.That(info.Length).IsEqualTo(1000);
    }

    [Test]
    public async Task Seek_Should_Move_Position()
    {
        using var ds = new DiskStream(_testDbPath);
        ds.SetLength(100);
        ds.Seek(50, SeekOrigin.Begin);
        await Assert.That(ds.Position).IsEqualTo(50);
    }

    [Test]
    public async Task LockRegion_Should_Not_Throw()
    {
        using var ds = new DiskStream(_testDbPath);
        var handle = ds.LockRegion(0, 10);
        await Assert.That(handle).IsNotNull();
        ds.UnlockRegion(handle);
    }
    
    [Test]
    public async Task ReadWrite_Should_Work()
    {
        using var ds = new DiskStream(_testDbPath);
        var data = new byte[] { 1, 2, 3 };
        ds.Write(data, 0, 3);
        
        ds.Seek(0, SeekOrigin.Begin);
        var buffer = new byte[3];
        var read = ds.Read(buffer, 0, 3);
        
        await Assert.That(read).IsEqualTo(3);
        await Assert.That(buffer).IsEquivalentTo(data);
    }

    [Test]
    public async Task GetStatistics_Should_Return_Values()
    {
        using var ds = new DiskStream(_testDbPath);
        var stats = ds.GetStatistics();
        await Assert.That(stats.FilePath).IsEqualTo(_testDbPath);
        await Assert.That(stats.ToString()).IsNotNull();
    }
}