using System.Globalization;

namespace TinyDb.Bson;

/// <summary>
/// BSON ObjectId值
/// </summary>
public sealed class BsonObjectId : BsonValue
{
    public override BsonType BsonType => BsonType.ObjectId;
    public override object? RawValue { get; }

    public ObjectId Value { get; }

    public BsonObjectId(ObjectId value)
    {
        Value = value;
        RawValue = value;
    }

    public static implicit operator BsonObjectId(ObjectId value) => new(value);
    public static implicit operator ObjectId(BsonObjectId bsonObjectId) => bsonObjectId.Value;

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonObjectId otherObjectId) return Value.CompareTo(otherObjectId.Value);
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonObjectId otherObjectId && Value == otherObjectId.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString();

    // IConvertible 接口的实现
    public override TypeCode GetTypeCode() => TypeCode.Object;
    public override bool ToBoolean(IFormatProvider? provider) => Convert.ToBoolean(Value.ToString(), provider);
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value.ToString(), provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value.ToString(), provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(Value.ToString(), provider);
    public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(Value.ToString(), provider);
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value.ToString(), provider);
    public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(Value.ToString(), provider);
    public override long ToInt64(IFormatProvider? provider) => Convert.ToInt64(Value.ToString(), provider);
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value.ToString(), provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Value.ToString(), provider);
    public override string ToString(IFormatProvider? provider) => ToString();
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(ObjectId)) return Value;
        if (conversionType == typeof(BsonObjectId)) return this;
        if (conversionType == typeof(object)) return this;
        if (conversionType == typeof(string)) return ToString();
        return Convert.ChangeType(Value.ToString(), conversionType, provider);
    }
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value.ToString(), provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value.ToString(), provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value.ToString(), provider);
}