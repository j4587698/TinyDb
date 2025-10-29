using System.Globalization;

namespace TinyDb.Bson;

/// <summary>
/// BSON 64位整数值
/// </summary>
public sealed class BsonInt64 : BsonValue
{
    public override BsonType BsonType => BsonType.Int64;
    public override object? RawValue { get; }

    public long Value { get; }

    public BsonInt64(long value)
    {
        Value = value;
        RawValue = value;
    }

    public static implicit operator BsonInt64(long value) => new(value);
    public static implicit operator long(BsonInt64 bsonInt) => bsonInt.Value;

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonInt64 otherLong) return Value.CompareTo(otherLong.Value);
        if (other.IsNumeric) return Convert.ToDouble(Value).CompareTo(other.ToDouble(CultureInfo.InvariantCulture));
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonInt64 otherLong && Value == otherLong.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);

    // IConvertible 接口的实现
    public override TypeCode GetTypeCode() => TypeCode.Int64;
    public override bool ToBoolean(IFormatProvider? provider) => Value != 0;
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    public override decimal ToDecimal(IFormatProvider? provider) => Value;
    public override double ToDouble(IFormatProvider? provider) => Value;
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(Value, provider);
    public override long ToInt64(IFormatProvider? provider) => Value;
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Value, provider);
    public override string ToString(IFormatProvider? provider) => Value.ToString(provider);
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(long)) return Value;
        if (conversionType == typeof(BsonInt64)) return this;
        if (conversionType == typeof(object)) return this;
        if (conversionType == typeof(string)) return ToString(provider);
        return Convert.ChangeType(Value, conversionType, provider);
    }
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}