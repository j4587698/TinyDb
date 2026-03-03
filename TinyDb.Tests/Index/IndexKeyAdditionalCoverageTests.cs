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

    [Test]
    public async Task CompareTo_Numeric_WhenToDecimalThrowsInvalidCastOrFormat_ShouldFallbackToTypeOrder()
    {
        var invalidCastLeft = new IndexKey(new ThrowingNumericBsonValue(new InvalidCastException("bad cast")));
        var formatRight = new IndexKey(new ThrowingNumericBsonValue(new FormatException("bad format")));
        var reference = new IndexKey(new BsonInt32(1));

        var invalidCastComparison = invalidCastLeft.CompareTo(reference);
        var formatComparison = formatRight.CompareTo(reference);

        await Assert.That(invalidCastComparison).IsGreaterThan(0);
        await Assert.That(formatComparison).IsGreaterThan(0);
    }

    private sealed class ThrowingNumericBsonValue : BsonValue
    {
        private readonly Exception _exception;

        public ThrowingNumericBsonValue(Exception exception)
        {
            _exception = exception;
        }

        public override BsonType BsonType => BsonType.Double;
        public override object? RawValue => 0d;
        public override int CompareTo(BsonValue? other) => 0;
        public override bool Equals(BsonValue? other) => ReferenceEquals(this, other);
        public override int GetHashCode() => 0;
        public override TypeCode GetTypeCode() => TypeCode.Double;

        public override bool ToBoolean(IFormatProvider? provider) => throw new InvalidCastException();
        public override byte ToByte(IFormatProvider? provider) => throw new InvalidCastException();
        public override char ToChar(IFormatProvider? provider) => throw new InvalidCastException();
        public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();

        public override decimal ToDecimal(IFormatProvider? provider) => throw _exception;
        public override double ToDouble(IFormatProvider? provider) => throw new InvalidCastException();
        public override short ToInt16(IFormatProvider? provider) => throw new InvalidCastException();
        public override int ToInt32(IFormatProvider? provider) => throw new InvalidCastException();
        public override long ToInt64(IFormatProvider? provider) => throw new InvalidCastException();
        public override sbyte ToSByte(IFormatProvider? provider) => throw new InvalidCastException();
        public override float ToSingle(IFormatProvider? provider) => throw new InvalidCastException();
        public override string ToString(IFormatProvider? provider) => string.Empty;
        public override object ToType(Type conversionType, IFormatProvider? provider) => throw new InvalidCastException();
        public override ushort ToUInt16(IFormatProvider? provider) => throw new InvalidCastException();
        public override uint ToUInt32(IFormatProvider? provider) => throw new InvalidCastException();
        public override ulong ToUInt64(IFormatProvider? provider) => throw new InvalidCastException();
    }
}
