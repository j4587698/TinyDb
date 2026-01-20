using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class StatisticsCoverageTests
{
    [Test]
    public async Task DatabaseStatistics_ToString_ShouldReturnCorrectFormat()
    {
        var stats = new DatabaseStatistics
        {
            DatabaseName = "TestDB",
            UsedPages = 10,
            TotalPages = 100,
            CollectionCount = 5,
            FileSize = 1024 * 1024,
            CacheHitRatio = 0.855
        };

        var str = stats.ToString();
        
        await Assert.That(str).Contains("Database[TestDB]");
        await Assert.That(str).Contains("10/100 pages");
        await Assert.That(str).Contains("5 collections");
        await Assert.That(str).Contains("1,048,576 bytes"); // 1,048,576
        await Assert.That(str).Contains("HitRatio=85.5%");
    }
}