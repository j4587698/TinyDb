using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

public class IndexKeyDefaultCompareBranchCoverageTests
{
    [Test]
    public async Task CompareTo_WithBsonDocumentValues_ShouldHitDefaultComparison()
    {
        var d1 = new BsonDocument().Set("a", 1);
        var d2 = new BsonDocument().Set("a", 2);

        var k1 = new IndexKey(d1);
        var k2 = new IndexKey(d2);

        await Assert.That(k1.CompareTo(k2)).IsNotEqualTo(0);
    }
}

