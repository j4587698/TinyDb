using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public sealed class BsonConversionMissingLinesCoverageTests
{
    private enum IntEnum
    {
        A = 1
    }

    [Test]
    public async Task FromBsonValue_WhenTargetIsBsonValueButTypeMismatches_ShouldThrow()
    {
        await Assert.That(() => BsonConversion.FromBsonValue(new BsonString("x"), typeof(BsonInt32)))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task FromBsonValue_WhenTargetIsNonGenericList_ShouldThrow()
    {
        var array = new BsonArray(new BsonValue[] { 1 });
        await Assert.That(() => BsonConversion.FromBsonValue(array, typeof(ArrayList)))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task FromBsonValue_ListString_ShouldWork()
    {
        var array = new BsonArray(new BsonValue[] { new BsonString("a"), new BsonString("b") });
        var list = (List<string>)BsonConversion.FromBsonValue(array, typeof(List<string>))!;

        await Assert.That(list.Count).IsEqualTo(2);
        await Assert.That(list[0]).IsEqualTo("a");
    }

    [Test]
    public async Task FromBsonValue_ListDouble_ShouldWork()
    {
        var array = new BsonArray(new BsonValue[] { 1, new BsonDouble(2.5) });
        var list = (List<double>)BsonConversion.FromBsonValue(array, typeof(List<double>))!;

        await Assert.That(list.Count).IsEqualTo(2);
        await Assert.That(list[0]).IsEqualTo(1d);
        await Assert.That(list[1]).IsEqualTo(2.5d);
    }

    [Test]
    public async Task FromBsonValue_DictionaryUnsupportedValueType_ShouldThrow()
    {
        var doc = new BsonDocument().Set("a", 1);
        await Assert.That(() => BsonConversion.FromBsonValue(doc, typeof(Dictionary<string, double>)))
            .Throws<NotSupportedException>();
    }

    [Test]
    public async Task FromBsonValue_EnumNumericParseFailure_ShouldWrapAsInvalidOperationException()
    {
        await Assert.That(() => BsonConversion.FromBsonValue(new BsonString("not-a-number"), typeof(IntEnum)))
            .Throws<InvalidOperationException>();
    }

    [Test]
    public async Task IsComplexObjectType_WhenTypeIsBsonValue_ShouldReturnFalse()
    {
        var method = typeof(BsonConversion).GetMethod("IsComplexObjectType", BindingFlags.NonPublic | BindingFlags.Static);
        if (method == null) throw new InvalidOperationException("IsComplexObjectType not found");

        var result = (bool)method.Invoke(null, new object[] { typeof(BsonValue) })!;
        await Assert.That(result).IsFalse();
    }
}
