using System.Globalization;

namespace TinyDb.Bson;

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
        if (Nullable.GetUnderlyingType(conversionType) != null) return null!;
        if (!conversionType.IsValueType) return null!;

        if (conversionType.IsEnum)
        {
            return Enum.ToObject(conversionType, 0)!;
        }

        if (conversionType == typeof(Guid)) return Guid.Empty;
        if (conversionType == typeof(ObjectId)) return ObjectId.Empty;
        if (conversionType == typeof(DateTime)) return default(DateTime);
        if (conversionType == typeof(DateTimeOffset)) return default(DateTimeOffset);
        if (conversionType == typeof(TimeSpan)) return default(TimeSpan);

        return Type.GetTypeCode(conversionType) switch
        {
            TypeCode.Boolean => false,
            TypeCode.Byte => (byte)0,
            TypeCode.SByte => (sbyte)0,
            TypeCode.Int16 => (short)0,
            TypeCode.UInt16 => (ushort)0,
            TypeCode.Int32 => 0,
            TypeCode.UInt32 => 0u,
            TypeCode.Int64 => 0L,
            TypeCode.UInt64 => 0UL,
            TypeCode.Single => 0f,
            TypeCode.Double => 0d,
            TypeCode.Decimal => 0m,
            TypeCode.Char => '\0',
            _ => throw new NotSupportedException($"不支持将BsonNull转换为 {conversionType}.")
        };
    }
    public override ushort ToUInt16(IFormatProvider? provider) => 0;
    public override uint ToUInt32(IFormatProvider? provider) => 0;
    public override ulong ToUInt64(IFormatProvider? provider) => 0;
}
