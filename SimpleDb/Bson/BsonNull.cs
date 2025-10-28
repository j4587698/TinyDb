using System.Globalization;

namespace SimpleDb.Bson;

/// <summary>
/// BSON null值
/// </summary>
public sealed class BsonNull : BsonValue
{
    public override BsonType BsonType => BsonType.Null;
    public override object? RawValue => null;
    public override bool IsNull => true;

    public static readonly BsonNull Value = new();

    private BsonNull() { }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 0;
        if (other.IsNull) return 0;
        return -1;
    }

    public override bool Equals(BsonValue? other)
    {
        return other is null || other.IsNull;
    }

    public override int GetHashCode() => 0;

    public override string ToString() => "null";

    // IConvertible 接口的实现
    public override TypeCode GetTypeCode() => TypeCode.Empty;
    public override bool ToBoolean(IFormatProvider? provider) => false;
    public override byte ToByte(IFormatProvider? provider) => 0;
    public override char ToChar(IFormatProvider? provider) => '\0';
    public override DateTime ToDateTime(IFormatProvider? provider) => default;
    public override decimal ToDecimal(IFormatProvider? provider) => 0m;
    public override double ToDouble(IFormatProvider? provider) => 0.0;
    public override short ToInt16(IFormatProvider? provider) => 0;
    public override int ToInt32(IFormatProvider? provider) => 0;
    public override long ToInt64(IFormatProvider? provider) => 0;
    public override sbyte ToSByte(IFormatProvider? provider) => 0;
    public override float ToSingle(IFormatProvider? provider) => 0.0f;
    public override string ToString(IFormatProvider? provider) => "null";
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(object)) return null!;
        if (conversionType == typeof(BsonNull)) return this;
        if (!conversionType.IsValueType) return null!;
        return Activator.CreateInstance(conversionType)!;
    }
    public override ushort ToUInt16(IFormatProvider? provider) => 0;
    public override uint ToUInt32(IFormatProvider? provider) => 0;
    public override ulong ToUInt64(IFormatProvider? provider) => 0;
}