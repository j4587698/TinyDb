using System;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class QueryableObjectComparerCoverageTests
{
    [Test]
    public async Task ObjectComparer_CompareAndToDouble_ShouldCoverBranches()
    {
        var queryPipelineType = typeof(ExpressionEvaluator).Assembly.GetType("TinyDb.Query.QueryPipeline");
        await Assert.That(queryPipelineType).IsNotNull();

        var comparerType = queryPipelineType!.GetNestedType("ObjectComparer", BindingFlags.NonPublic);
        await Assert.That(comparerType).IsNotNull();

        var comparer = Activator.CreateInstance(comparerType!, nonPublic: true)!;
        var compare = comparerType!.GetMethod("Compare", BindingFlags.Public | BindingFlags.Instance);
        await Assert.That(compare).IsNotNull();

        var bsonCompare = (int)compare!.Invoke(comparer, new object?[] { new BsonInt32(1), new BsonInt32(2) })!;
        await Assert.That(bsonCompare).IsNegative();

        var bsonDoubleCompare = (int)compare.Invoke(comparer, new object?[] { new BsonDouble(1.0d), 2.0d })!;
        await Assert.That(bsonDoubleCompare).IsNegative();

        var bsonInt32Compare = (int)compare.Invoke(comparer, new object?[] { new BsonInt32(2), 1 })!;
        await Assert.That(bsonInt32Compare).IsPositive();

        var bsonInt64Compare = (int)compare.Invoke(comparer, new object?[] { new BsonInt64(2), 3L })!;
        await Assert.That(bsonInt64Compare).IsNegative();

        var decimalCompare = (int)compare.Invoke(comparer, new object?[] { 1.5m, 2 })!;
        await Assert.That(decimalCompare).IsNegative();

        var fallbackNumericCompare = (int)compare.Invoke(comparer, new object?[] { (short)1, (byte)2 })!;
        await Assert.That(fallbackNumericCompare).IsNegative();

        var floatCompare = (int)compare.Invoke(comparer, new object?[] { 1.0f, 2.0f })!;
        await Assert.That(floatCompare).IsNegative();

        var guidCompare = (int)compare.Invoke(comparer, new object?[] { Guid.Empty, Guid.NewGuid() })!;
        await Assert.That(guidCompare).IsNotEqualTo(0);

        var guidVsStringCompare = (int)compare.Invoke(comparer, new object?[] { Guid.Empty, "x" })!;
        await Assert.That(guidVsStringCompare).IsNotEqualTo(0);

        var toDouble = comparerType.GetMethod("ToDouble", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(toDouble).IsNotNull();

        var convertFail = (double)toDouble!.Invoke(null, new object?[] { new object() })!;
        await Assert.That(convertFail).IsEqualTo(0.0d);
    }
}
