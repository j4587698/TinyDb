using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Bson;

public class BsonBooleanAndValueBranchCoverageTests
{
    [Test]
    public async Task BsonBoolean_ShouldCoverBranches_ForEqualsToStringAndNumericConversions()
    {
        var t = new BsonBoolean(true);
        var f = new BsonBoolean(false);

        await Assert.That(t.ToString()).IsEqualTo("true");
        await Assert.That(f.ToString()).IsEqualTo("false");

        await Assert.That(t.ToDecimal(null)).IsEqualTo(1m);
        await Assert.That(f.ToDecimal(null)).IsEqualTo(0m);

        await Assert.That(t.ToDouble(null)).IsEqualTo(1.0);
        await Assert.That(f.ToDouble(null)).IsEqualTo(0.0);

        await Assert.That(t.ToSingle(null)).IsEqualTo(1.0f);
        await Assert.That(f.ToSingle(null)).IsEqualTo(0.0f);

        await Assert.That(t.Equals(new BsonBoolean(true))).IsTrue();
        await Assert.That(t.Equals(new BsonBoolean(false))).IsFalse();
        await Assert.That(t.Equals(new BsonInt32(1))).IsFalse();
    }

    [Test]
    public async Task BsonValue_As_WithNullRawValue_ShouldThrowInvalidCast()
    {
        await Assert.That(() => BsonNull.Value.As<int>())
            .Throws<InvalidCastException>();
    }
}

