using System;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Query;

public class QueryOptimizerBranchCoverageTests2
{
    private sealed class NullToString
    {
        public override string? ToString() => null;
    }

    [Test]
    public async Task Ctor_NullEngine_ShouldThrow()
    {
        await Assert.That(() => new QueryOptimizer(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ConvertToBsonValue_WhenToStringReturnsNull_ShouldFallbackToEmptyString()
    {
        var method = typeof(QueryOptimizer).GetMethod("ConvertToBsonValue", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var bson = (BsonValue)method!.Invoke(null, new object[] { new NullToString() })!;
        await Assert.That(bson).IsTypeOf<BsonString>();
        await Assert.That(((BsonString)bson).Value).IsEqualTo(string.Empty);
    }

    [Test]
    public async Task ConvertToBsonValue_WhenValueIsBsonValue_ShouldReturnSameInstance()
    {
        var method = typeof(QueryOptimizer).GetMethod("ConvertToBsonValue", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var input = new BsonInt32(123);
        var result = (BsonValue)method!.Invoke(null, new object[] { input })!;

        await Assert.That(ReferenceEquals(result, input)).IsTrue();
    }
}
