using System;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public sealed class IndexKeyAdditionalCoverageTests
{
    [Test]
    public async Task CompareTo_NumericCrossType_ShouldCoverBranches()
    {
        var equalDifferentTypes = new IndexKey(new BsonInt32(1)).CompareTo(new IndexKey(new BsonInt64(1)));
        await Assert.That(equalDifferentTypes).IsNegative();

        var differentValues = new IndexKey(new BsonInt32(1)).CompareTo(new IndexKey(new BsonInt64(2)));
        await Assert.That(differentValues).IsNegative();

        var overflowInDecimalConversion = new IndexKey(new BsonDouble(double.MaxValue)).CompareTo(new IndexKey(new BsonInt32(0)));
        await Assert.That(overflowInDecimalConversion).IsPositive();
    }

    [Test]
    public async Task GetTypeOrder_AllCases_ShouldCoverSwitchArms()
    {
        var method = typeof(IndexKey).GetMethod("GetTypeOrder", BindingFlags.NonPublic | BindingFlags.Static);
        await Assert.That(method).IsNotNull();

        var types = new[]
        {
            BsonType.MinKey,
            BsonType.Null,
            BsonType.Boolean,
            BsonType.Int32,
            BsonType.Int64,
            BsonType.Double,
            BsonType.Decimal128,
            BsonType.String,
            BsonType.ObjectId,
            BsonType.DateTime,
            BsonType.Binary,
            BsonType.Array,
            BsonType.Document,
            BsonType.RegularExpression,
            BsonType.JavaScript,
            BsonType.JavaScriptWithScope,
            BsonType.Timestamp,
            BsonType.Symbol,
            BsonType.Undefined,
            BsonType.MaxKey,
            (BsonType)0xEE
        };

        foreach (var t in types)
        {
            var order = (int)method!.Invoke(null, new object[] { t })!;
            await Assert.That(order).IsGreaterThanOrEqualTo(0);
        }
    }

    [Test]
    public async Task CompareTo_BooleanAndDecimal128_ShouldCoverTypeSpecificBranches()
    {
        var boolCompare = new IndexKey(new BsonBoolean(false)).CompareTo(new IndexKey(new BsonBoolean(true)));
        await Assert.That(boolCompare).IsNegative();

        var decCompare = new IndexKey(new BsonDecimal128(1.5m)).CompareTo(new IndexKey(new BsonDecimal128(2.5m)));
        await Assert.That(decCompare).IsNegative();
    }

    [Test]
    public async Task EqualsObject_WhenDifferentType_ShouldReturnFalse()
    {
        var key = new IndexKey(new BsonInt32(1));
        await Assert.That(key.Equals(new object())).IsFalse();
    }

    [Test]
    public async Task ToStringAndHashCode_WhenContainsNull_ShouldCoverNullBranches()
    {
        var key = new IndexKey(new BsonValue[] { null!, new BsonInt32(1) });

        _ = key.GetHashCode();
        await Assert.That(key.ToString()).Contains("null");
    }

    [Test]
    public async Task CompareTo_SameTypeSwitch_ShouldCoverAllArms()
    {
        var stringCmp = new IndexKey(new BsonString("a")).CompareTo(new IndexKey(new BsonString("b")));
        await Assert.That(stringCmp).IsNegative();

        _ = new IndexKey(new BsonInt32(1)).CompareTo(new IndexKey(new BsonInt32(2)));
        _ = new IndexKey(new BsonInt64(1)).CompareTo(new IndexKey(new BsonInt64(2)));
        _ = new IndexKey(new BsonDouble(1.0)).CompareTo(new IndexKey(new BsonDouble(2.0)));
        _ = new IndexKey(new BsonBoolean(false)).CompareTo(new IndexKey(new BsonBoolean(true)));
        _ = new IndexKey(new BsonDateTime(DateTime.UnixEpoch)).CompareTo(new IndexKey(new BsonDateTime(DateTime.UnixEpoch.AddSeconds(1))));
        _ = new IndexKey(new BsonObjectId(ObjectId.NewObjectId())).CompareTo(new IndexKey(new BsonObjectId(ObjectId.NewObjectId())));
        _ = new IndexKey(new BsonBinary(new byte[] { 1 })).CompareTo(new IndexKey(new BsonBinary(new byte[] { 2 })));
        _ = new IndexKey(new BsonDecimal128(1m)).CompareTo(new IndexKey(new BsonDecimal128(2m)));
        _ = new IndexKey(new BsonTimestamp(1, 1)).CompareTo(new IndexKey(new BsonTimestamp(2, 1)));
        _ = new IndexKey(BsonMinKey.Value).CompareTo(new IndexKey(BsonMinKey.Value));
        _ = new IndexKey(BsonMaxKey.Value).CompareTo(new IndexKey(BsonMaxKey.Value));
    }
}
