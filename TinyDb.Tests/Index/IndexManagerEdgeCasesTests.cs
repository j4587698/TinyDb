using TinyDb.Core;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class IndexManagerEdgeCasesTests : IDisposable
{
    private readonly string _testDbPath;
    private readonly TinyDbEngine _engine;

    public IndexManagerEdgeCasesTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"idx_edge_{Guid.NewGuid():N}.db");
        _engine = new TinyDbEngine(_testDbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task CreateIndex_With_Invalid_Arguments_Should_Throw()
    {
        var idxMgr = _engine.GetIndexManager("col1");
        
        await Assert.That(() => idxMgr.CreateIndex(null!, new[] { "field" })).Throws<ArgumentException>();
        await Assert.That(() => idxMgr.CreateIndex("", new[] { "field" })).Throws<ArgumentException>();
        
        await Assert.That(() => idxMgr.CreateIndex("idx1", null!)).Throws<ArgumentException>();
        await Assert.That(() => idxMgr.CreateIndex("idx1", Array.Empty<string>())).Throws<ArgumentException>();
    }

    [Test]
    public async Task DropIndex_With_Invalid_Arguments_Should_Throw()
    {
        var idxMgr = _engine.GetIndexManager("col1");
        await Assert.That(() => idxMgr.DropIndex(null!)).Throws<ArgumentException>();
        await Assert.That(() => idxMgr.DropIndex("")).Throws<ArgumentException>();
    }

    [Test]
    public async Task GetIndex_With_Invalid_Arguments_Should_Throw()
    {
        var idxMgr = _engine.GetIndexManager("col1");
        await Assert.That(() => idxMgr.GetIndex(null!)).Throws<ArgumentException>();
        await Assert.That(() => idxMgr.GetIndex("")).Throws<ArgumentException>();
    }

    [Test]
    public async Task IndexExists_With_Invalid_Arguments_Should_Throw()
    {
        var idxMgr = _engine.GetIndexManager("col1");
        await Assert.That(() => idxMgr.IndexExists(null!)).Throws<ArgumentException>();
        await Assert.That(() => idxMgr.IndexExists("")).Throws<ArgumentException>();
    }

    [Test]
    public async Task GetBestIndex_With_Empty_Fields_Should_Return_Null()
    {
        var idxMgr = _engine.GetIndexManager("col1");
        idxMgr.CreateIndex("idx1", new[] { "a" });
        
        await Assert.That(idxMgr.GetBestIndex(null!)).IsNull();
        await Assert.That(idxMgr.GetBestIndex(Array.Empty<string>())).IsNull();
    }
}
