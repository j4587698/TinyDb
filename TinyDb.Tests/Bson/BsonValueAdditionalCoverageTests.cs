using System;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public sealed class BsonValueAdditionalCoverageTests
{
    [Test]
    public async Task ImplicitStringConversion_ShouldHandleNullAndNonNull()
    {
        BsonValue nullValue = (string?)null;
        BsonValue strValue = "abc";

        await Assert.That(nullValue).IsTypeOf<BsonNull>();
        await Assert.That(strValue).IsTypeOf<BsonString>();
    }

    [Test]
    public async Task ToStringOverride_ShouldReturnEmptyString_WhenRawValueIsNull()
    {
        await Assert.That(new NullRawBsonValue().ToString()).IsEqualTo(string.Empty);
        await Assert.That(new BsonString("x").ToString()).IsEqualTo("x");
    }

    [Test]
    public async Task Equals_ObjectNull_ShouldReturnFalse()
    {
        await Assert.That(new BsonInt32(1).Equals((object?)null)).IsFalse();
    }

    [Test]
    public async Task As_DecimalRawValue_ShouldConvertToDecimal128()
    {
        BsonValue value = new DecimalRawBsonValue(12.34m);

        var d128 = value.As<Decimal128>();

        await Assert.That(d128.ToDecimal()).IsEqualTo(12.34m);
    }

    private sealed class DecimalRawBsonValue : BsonValue
    {
        private readonly decimal _value;

        public DecimalRawBsonValue(decimal value)
        {
            _value = value;
        }

        public override BsonType BsonType => BsonType.Decimal128;
        public override object? RawValue => _value;

        public override int CompareTo(BsonValue? other)
        {
            if (other == null) return 1;
            return Convert.ToDecimal(other.RawValue).CompareTo(_value);
        }

        public override bool Equals(BsonValue? other)
        {
            if (other == null) return false;
            return Equals(other.RawValue);
        }

        public override int GetHashCode() => _value.GetHashCode();

        public override TypeCode GetTypeCode() => TypeCode.Decimal;
        public override bool ToBoolean(IFormatProvider? provider) => _value != 0m;
        public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(_value, provider);
        public override char ToChar(IFormatProvider? provider) => Convert.ToChar(_value, provider);
        public override DateTime ToDateTime(IFormatProvider? provider) => Convert.ToDateTime(_value, provider);
        public override decimal ToDecimal(IFormatProvider? provider) => _value;
        public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(_value, provider);
        public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(_value, provider);
        public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(_value, provider);
        public override long ToInt64(IFormatProvider? provider) => Convert.ToInt64(_value, provider);
        public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(_value, provider);
        public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(_value, provider);
        public override string ToString(IFormatProvider? provider) => _value.ToString(provider);
        public override object ToType(Type conversionType, IFormatProvider? provider) => Convert.ChangeType(_value, conversionType, provider)!;
        public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(_value, provider);
        public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(_value, provider);
        public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(_value, provider);
    }

    private sealed class NullRawBsonValue : BsonValue
    {
        public override BsonType BsonType => BsonType.Null;
        public override object? RawValue => null;

        public override int CompareTo(BsonValue? other) => other == null ? 0 : -1;
        public override bool Equals(BsonValue? other) => other is { BsonType: BsonType.Null };
        public override int GetHashCode() => 0;

        public override TypeCode GetTypeCode() => TypeCode.Empty;
        public override bool ToBoolean(IFormatProvider? provider) => false;
        public override byte ToByte(IFormatProvider? provider) => 0;
        public override char ToChar(IFormatProvider? provider) => '\0';
        public override DateTime ToDateTime(IFormatProvider? provider) => default;
        public override decimal ToDecimal(IFormatProvider? provider) => 0m;
        public override double ToDouble(IFormatProvider? provider) => 0d;
        public override short ToInt16(IFormatProvider? provider) => 0;
        public override int ToInt32(IFormatProvider? provider) => 0;
        public override long ToInt64(IFormatProvider? provider) => 0L;
        public override sbyte ToSByte(IFormatProvider? provider) => 0;
        public override float ToSingle(IFormatProvider? provider) => 0f;
        public override string ToString(IFormatProvider? provider) => "null";
        public override object ToType(Type conversionType, IFormatProvider? provider) => throw new InvalidCastException();
        public override ushort ToUInt16(IFormatProvider? provider) => 0;
        public override uint ToUInt32(IFormatProvider? provider) => 0u;
        public override ulong ToUInt64(IFormatProvider? provider) => 0ul;
    }
}
