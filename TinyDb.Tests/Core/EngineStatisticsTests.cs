using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class EngineStatisticsTests : IDisposable
{
    private readonly string _testDbPath;

    public EngineStatisticsTests()
    {
        _testDbPath = Path.Combine(Path.GetTempPath(), $"stats_{Guid.NewGuid():N}.db");
    }

    public void Dispose()
    {
        try { if (File.Exists(_testDbPath)) File.Delete(_testDbPath); } catch { }
    }

    [Test]
    public async Task GetStatistics_Should_Report_Correct_Values()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        var stats = engine.GetStatistics();
        
        await Assert.That(stats.DatabaseName).IsEqualTo("TinyDb");
        await Assert.That(stats.UsedPages).IsGreaterThan(0u);
        await Assert.That(stats.CollectionCount).IsEqualTo(0);
        
        // Add collection
        engine.GetCollectionWithName<object>("Test");
        var stats2 = engine.GetStatistics();
        await Assert.That(stats2.CollectionCount).IsEqualTo(1);
        
        await Assert.That(stats.ToString()).IsNotNull();
    }

    [Test]
    public async Task TransactionManager_Statistics_Should_Work()
    {
        using var engine = new TinyDbEngine(_testDbPath);
        using var trans = engine.BeginTransaction();
        
        var stats = engine.GetTransactionStatistics();
        await Assert.That(stats.ActiveTransactionCount).IsEqualTo(1);
        await Assert.That(stats.ToString()).IsNotNull();
    }
}
