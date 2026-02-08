using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class ObjectIdBranchCoverageTests2
{
    [Test]
    public async Task CompareTo_WhenOtherBytesNull_ShouldReturnOne()
    {
        var oid = ObjectId.NewObjectId();
        await Assert.That(oid.CompareTo(default)).IsEqualTo(1);
    }
}
