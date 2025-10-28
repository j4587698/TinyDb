using System.Globalization;

namespace SimpleDb.Bson;

/// <summary>
/// BSON 布尔值
/// </summary>
public sealed class BsonBoolean : BsonValue
{
    public override BsonType BsonType => BsonType.Boolean;
    public override object? RawValue { get; }

    public bool Value { get; }

    public BsonBoolean(bool value)
    {
        Value = value;
        RawValue = value;
    }

    public static implicit operator BsonBoolean(bool value) => new(value);
    public static implicit operator bool(BsonBoolean bsonBoolean) => bsonBoolean.Value;

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonBoolean otherBool) return Value.CompareTo(otherBool.Value);
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonBoolean otherBool && Value == otherBool.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value ? "true" : "false";

    // IConvertible 接口的实现
    public override TypeCode GetTypeCode() => TypeCode.Boolean;
    public override bool ToBoolean(IFormatProvider? provider) => Value;
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    public override decimal ToDecimal(IFormatProvider? provider) => Value ? 1m : 0m;
    public override double ToDouble(IFormatProvider? provider) => Value ? 1.0 : 0.0;
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Value ? 1 : 0;
    public override long ToInt64(IFormatProvider? provider) => Value ? 1L : 0L;
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Value ? 1.0f : 0.0f;
    public override string ToString(IFormatProvider? provider) => ToString();
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(bool)) return Value;
        if (conversionType == typeof(BsonBoolean)) return this;
        if (conversionType == typeof(object)) return this;
        if (conversionType == typeof(string)) return ToString();
        return Convert.ChangeType(Value, conversionType, provider);
    }
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}