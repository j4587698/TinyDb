using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public sealed class BsonTimestampHashCodeCoverageTests
{
    [Test]
    public async Task GetHashCode_ShouldReturnValueHashCode()
    {
        var ts = new BsonTimestamp(123, 456);
        await Assert.That(ts.GetHashCode()).IsEqualTo(ts.Value.GetHashCode());
    }
}

