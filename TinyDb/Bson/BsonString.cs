using System.Globalization;

namespace TinyDb.Bson;

/// <summary>
/// BSON 字符串值
/// </summary>
public sealed class BsonString : BsonValue
{
    public override BsonType BsonType => BsonType.String;
    public override object? RawValue { get; }

    public string Value { get; }

    public BsonString(string value)
    {
        Value = value ?? string.Empty;
        RawValue = Value;
    }

    public static implicit operator BsonString(string value) => new(value);
    public static implicit operator string(BsonString bsonString) => bsonString.Value;

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonString otherString) return string.Compare(Value, otherString.Value, false, CultureInfo.InvariantCulture);
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonString otherString && Value == otherString.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value;

    // IConvertible 接口的实现
    public override TypeCode GetTypeCode() => TypeCode.String;
    public override bool ToBoolean(IFormatProvider? provider) => Convert.ToBoolean(Value, provider);
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => Convert.ToDateTime(Value, provider);
    public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(Value, provider);
    public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(Value, provider);
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(Value, provider);
    public override long ToInt64(IFormatProvider? provider) => Convert.ToInt64(Value, provider);
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Value, provider);
    public override string ToString(IFormatProvider? provider) => Value;
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(string)) return Value;
        if (conversionType == typeof(BsonString)) return this;
        if (conversionType == typeof(object)) return this;
        return Convert.ChangeType(Value, conversionType, provider);
    }
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}