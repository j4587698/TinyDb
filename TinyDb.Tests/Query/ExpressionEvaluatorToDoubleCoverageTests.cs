using System;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Query;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Query;

public sealed class ExpressionEvaluatorToDoubleCoverageTests
{
    [Test]
    public async Task ToDouble_ShouldCoverBranches()
    {
        var method = typeof(ExpressionEvaluator).GetMethod("ToDouble", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var nullValue = (double)method!.Invoke(null, new object?[] { null })!;
        await Assert.That(nullValue).IsEqualTo(0.0d);

        var decimalValue = (double)method.Invoke(null, new object?[] { 12.5m })!;
        await Assert.That(decimalValue).IsEqualTo(12.5d);

        var bsonDouble = (double)method.Invoke(null, new object?[] { new BsonDouble(1.25d) })!;
        await Assert.That(bsonDouble).IsEqualTo(1.25d);

        var bsonInt32 = (double)method.Invoke(null, new object?[] { new BsonInt32(3) })!;
        await Assert.That(bsonInt32).IsEqualTo(3.0d);

        var bsonInt64 = (double)method.Invoke(null, new object?[] { new BsonInt64(4) })!;
        await Assert.That(bsonInt64).IsEqualTo(4.0d);

        var fallback = (double)method.Invoke(null, new object?[] { new object() })!;
        await Assert.That(fallback).IsEqualTo(0.0d);
    }
}

