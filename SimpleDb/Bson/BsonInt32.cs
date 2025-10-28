using System.Globalization;

namespace SimpleDb.Bson;

/// <summary>
/// BSON 32位整数值
/// </summary>
public sealed class BsonInt32 : BsonValue
{
    public override BsonType BsonType => BsonType.Int32;
    public override object? RawValue { get; }

    public int Value { get; }

    public BsonInt32(int value)
    {
        Value = value;
        RawValue = value;
    }

    public static implicit operator BsonInt32(int value) => new(value);
    public static implicit operator int(BsonInt32 bsonInt) => bsonInt.Value;

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonInt32 otherInt) return Value.CompareTo(otherInt.Value);
        if (other.IsNumeric) return Convert.ToDouble(Value).CompareTo(other.ToDouble(CultureInfo.InvariantCulture));
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonInt32 otherInt && Value == otherInt.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);

    // IConvertible 接口的实现
    public override TypeCode GetTypeCode() => TypeCode.Int32;
    public override bool ToBoolean(IFormatProvider? provider) => Value != 0;
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    public override decimal ToDecimal(IFormatProvider? provider) => Value;
    public override double ToDouble(IFormatProvider? provider) => Value;
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Value;
    public override long ToInt64(IFormatProvider? provider) => Value;
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Value;
    public override string ToString(IFormatProvider? provider) => Value.ToString(provider);
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(int)) return Value;
        if (conversionType == typeof(BsonInt32)) return this;
        if (conversionType == typeof(object)) return this;
        if (conversionType == typeof(string)) return ToString(provider);
        return Convert.ChangeType(Value, conversionType, provider);
    }
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}