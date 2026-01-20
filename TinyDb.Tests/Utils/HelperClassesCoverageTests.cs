using TinyDb.Index;
using TinyDb.Storage;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Utils;

public class HelperClassesCoverageTests
{
    [Test]
    public async Task SplitResult_Should_Store_Values()
    {
        var key = new IndexKey(new BsonValue[] { 1 });
        var result = new SplitResult
        {
            NewNode = null!,
            PromotedKey = key
        };
        
        await Assert.That(result.NewNode).IsNull();
        await Assert.That(result.PromotedKey).IsEqualTo(key);
    }

    [Test]
    public async Task IndexStatistics_ToString_ShouldWork()
    {
        var stats = new IndexStatistics
        {
            Name = "idx_test",
            Type = IndexType.BTree,
            Fields = new[] { "a" },
            IsUnique = true,
            NodeCount = 5,
            EntryCount = 50,
            TreeHeight = 2
        };
        
        var str = stats.ToString();
        await Assert.That(str).Contains("Index[idx_test]");
        await Assert.That(str).Contains("BTree");
        await Assert.That(str).Contains("1 fields");
        await Assert.That(str).Contains("50 entries");
    }

    [Test]
    public async Task LargeDocumentStatistics_Should_Store_Values()
    {
        var stats = new LargeDocumentStatistics
        {
            IndexPageId = 1,
            TotalLength = 1000,
            PageCount = 2,
            FirstDataPageId = 5
        };
        
        await Assert.That(stats.IndexPageId).IsEqualTo(1u);
        await Assert.That(stats.TotalLength).IsEqualTo(1000);
        await Assert.That(stats.PageCount).IsEqualTo(2);
        await Assert.That(stats.FirstDataPageId).IsEqualTo(5u);
    }
}
