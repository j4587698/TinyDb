using System.Globalization;

namespace TinyDb.Bson;

/// <summary>
/// BSON ObjectId值
/// </summary>
public sealed class BsonObjectId : BsonValue
{
    public override BsonType BsonType => BsonType.ObjectId;
    public override object? RawValue => Value;

    public ObjectId Value { get; }

    public BsonObjectId(ObjectId value)
    {
        Value = value;
    }

    public static implicit operator BsonObjectId(ObjectId value) => new(value);
    public static implicit operator ObjectId(BsonObjectId bsonObjectId) => bsonObjectId.Value;

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonObjectId otherObjectId) return Value.CompareTo(otherObjectId.Value);
        return BsonValueComparer.Compare(this, other);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonObjectId otherObjectId && Value == otherObjectId.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString();

    // IConvertible 接口的实现
    public override TypeCode GetTypeCode() => TypeCode.Object;
    public override bool ToBoolean(IFormatProvider? provider) => throw ConversionNotSupported(nameof(Boolean));
    public override byte ToByte(IFormatProvider? provider) => throw ConversionNotSupported(nameof(Byte));
    public override char ToChar(IFormatProvider? provider) => throw ConversionNotSupported(nameof(Char));
    public override DateTime ToDateTime(IFormatProvider? provider) => throw ConversionNotSupported(nameof(DateTime));
    public override decimal ToDecimal(IFormatProvider? provider) => throw ConversionNotSupported(nameof(Decimal));
    public override double ToDouble(IFormatProvider? provider) => throw ConversionNotSupported(nameof(Double));
    public override short ToInt16(IFormatProvider? provider) => throw ConversionNotSupported(nameof(Int16));
    public override int ToInt32(IFormatProvider? provider) => throw ConversionNotSupported(nameof(Int32));
    public override long ToInt64(IFormatProvider? provider) => throw ConversionNotSupported(nameof(Int64));
    public override sbyte ToSByte(IFormatProvider? provider) => throw ConversionNotSupported(nameof(SByte));
    public override float ToSingle(IFormatProvider? provider) => throw ConversionNotSupported(nameof(Single));
    public override string ToString(IFormatProvider? provider) => ToString();
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(ObjectId)) return Value;
        if (conversionType == typeof(BsonObjectId)) return this;
        if (conversionType == typeof(object)) return this;
        if (conversionType == typeof(string)) return ToString();
        throw ConversionNotSupported(conversionType.Name);
    }
    public override ushort ToUInt16(IFormatProvider? provider) => throw ConversionNotSupported(nameof(UInt16));
    public override uint ToUInt32(IFormatProvider? provider) => throw ConversionNotSupported(nameof(UInt32));
    public override ulong ToUInt64(IFormatProvider? provider) => throw ConversionNotSupported(nameof(UInt64));

    private static InvalidCastException ConversionNotSupported(string targetType)
    {
        return new InvalidCastException($"BsonObjectId does not support conversion to {targetType}.");
    }
}
