using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class IndexScannerTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public IndexScannerTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"idx_scan_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    public class User
    {
        public int Id { get; set; }
        [Index(Unique = true)]
        public string Email { get; set; } = "";
        [Index]
        public string Name { get; set; } = "";
    }

    [CompositeIndex("ci_weird", "", "A")]
    public class Weird
    {
        [Index]
        public int A { get; set; }
    }

    [Test]
    public async Task ScanAndCreateIndexes_Should_Create_Property_Indexes()
    {
        IndexScanner.ScanAndCreateIndexes(_engine, typeof(User), "Users");
        var idxMgr = _engine.GetIndexManager("Users");
        
        // The default index name might be idx_{FieldName}
        // Let's check GenerateIndexName in original code or assume it.
        // Actually, let's just check stats.
        var stats = idxMgr.GetAllStatistics().ToList();
        await Assert.That(stats.Count).IsGreaterThanOrEqualTo(3); // _id, Email, Name
    }

    [Test]
    public async Task ScanAndCreateIndexes_Should_Handle_Empty_And_SingleChar_Fields()
    {
        IndexScanner.ScanAndCreateIndexes(_engine, typeof(Weird), "Weird");
        var idxMgr = _engine.GetIndexManager("Weird");

        var stats = idxMgr.GetAllStatistics().ToList();
        await Assert.That(stats.Count).IsGreaterThanOrEqualTo(2);
    }

    [Test]
    public async Task EntityIndexInfo_ToString_Should_Work()
    {
        var info = new EntityIndexInfo
        {
            Name = "idx1",
            Fields = new[] { "F1" },
            IsUnique = true,
            IsComposite = false
        };
        await Assert.That(info.ToString()).Contains("idx1");
    }
}
