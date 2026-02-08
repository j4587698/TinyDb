using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Serialization;

public class BsonMapperConvertToBsonValueBranchCoverageTests
{
    [Test]
    public async Task ConvertToBsonValue_NullAndNonNull_ShouldCoverBranches()
    {
        var n = BsonMapper.ConvertToBsonValue(null);
        await Assert.That(n).IsEqualTo(BsonNull.Value);

        var v = BsonMapper.ConvertToBsonValue(123);
        await Assert.That(v).IsTypeOf<BsonInt32>();
    }
}

