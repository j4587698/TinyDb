using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Storage;

public class LargeDocumentStatisticsTests
{
    [Test]
    public async Task LargeDocumentStatistics_ToString_ShouldWork()
    {
        var stats = new LargeDocumentStatistics
        {
            IndexPageId = 10,
            TotalLength = 50000,
            PageCount = 7
        };
        
        var str = stats.ToString();
        // Actual format: LargeDoc[Index=10, Size=50,000 bytes, Pages=7]
        await Assert.That(str).Contains("LargeDoc[Index=10");
        await Assert.That(str).Contains("Size=50,000 bytes");
        await Assert.That(str).Contains("Pages=7]");
    }
}