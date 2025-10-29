using System.Globalization;

namespace TinyDb.Bson;

/// <summary>
/// BSON 128位十进制数值
/// </summary>
public sealed class BsonDecimal128 : BsonValue
{
    public override BsonType BsonType => BsonType.Decimal128;
    public override object? RawValue { get; }

    public decimal Value { get; }

    public BsonDecimal128(decimal value)
    {
        Value = value;
        RawValue = value;
    }

    public static implicit operator BsonDecimal128(decimal value) => new(value);
    public static implicit operator decimal(BsonDecimal128 bsonDecimal) => bsonDecimal.Value;

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonDecimal128 otherDecimal) return Value.CompareTo(otherDecimal.Value);
        if (other.IsNumeric) return Value.CompareTo(other.ToDecimal(CultureInfo.InvariantCulture));
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonDecimal128 otherDecimal && Value == otherDecimal.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);

    // IConvertible 接口的实现
    public override TypeCode GetTypeCode() => TypeCode.Decimal;
    public override bool ToBoolean(IFormatProvider? provider) => Value != 0;
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    public override decimal ToDecimal(IFormatProvider? provider) => Value;
    public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(Value, provider);
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(Value, provider);
    public override long ToInt64(IFormatProvider? provider) => Convert.ToInt64(Value, provider);
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Value, provider);
    public override string ToString(IFormatProvider? provider) => Value.ToString(provider);
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(decimal)) return Value;
        if (conversionType == typeof(BsonDecimal128)) return this;
        if (conversionType == typeof(object)) return this;
        if (conversionType == typeof(string)) return ToString(provider);
        return Convert.ChangeType(Value, conversionType, provider);
    }
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}