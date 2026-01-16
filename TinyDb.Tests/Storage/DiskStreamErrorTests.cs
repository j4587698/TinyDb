using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class DiskStreamErrorTests : IDisposable
{
    private readonly string _testDbPath;

    public DiskStreamErrorTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"disk_err_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task Constructor_Should_Throw_If_File_Locked()
    {
        // Lock file exclusively
        using var fs = new FileStream(_testDbPath, FileMode.Create, FileAccess.Write, FileShare.None);
        
        await Assert.That(() => new DiskStream(_testDbPath))
            .Throws<IOException>();
    }
}
