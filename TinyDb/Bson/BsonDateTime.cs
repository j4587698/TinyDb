using System.Globalization;

namespace TinyDb.Bson;

/// <summary>
/// BSON DateTime值
/// </summary>
public sealed class BsonDateTime : BsonValue
{
    private const long LegacyMinMilliseconds = -62135596800000L;
    private const long LegacyMaxMilliseconds = 253402300799999L;
    private const ulong TicksMask = 0x3FFFFFFFFFFFFFFFUL;
    private static readonly DateTime UnixEpochUtc = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    public override BsonType BsonType => BsonType.DateTime;
    public override object? RawValue { get; }

    public DateTime Value { get; }

    public BsonDateTime(DateTime value)
    {
        Value = value;
        RawValue = value;
    }

    public static long EncodeStoredValue(DateTime value)
    {
        var kindCode = value.Kind switch
        {
            DateTimeKind.Utc => 1UL,
            DateTimeKind.Local => 2UL,
            _ => 3UL
        };

        return unchecked((long)((kindCode << 62) | (ulong)value.Ticks));
    }

    public static DateTime DecodeStoredValue(long storedValue)
    {
        if (storedValue >= LegacyMinMilliseconds && storedValue <= LegacyMaxMilliseconds)
        {
            return UnixEpochUtc.AddMilliseconds(storedValue);
        }

        var raw = unchecked((ulong)storedValue);
        var ticks = checked((long)(raw & TicksMask));
        var kind = (raw >> 62) switch
        {
            1UL => DateTimeKind.Utc,
            2UL => DateTimeKind.Local,
            _ => DateTimeKind.Unspecified
        };

        return new DateTime(ticks, kind);
    }

    public static long GetComparableTicks(DateTime value)
    {
        var normalized = value.Kind switch
        {
            DateTimeKind.Utc => value,
            DateTimeKind.Local => value.ToUniversalTime(),
            _ => DateTime.SpecifyKind(value, DateTimeKind.Utc)
        };

        return normalized.Ticks;
    }

    public static implicit operator BsonDateTime(DateTime value) => new(value);
    public static implicit operator DateTime(BsonDateTime bsonDateTime) => bsonDateTime.Value;

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonDateTime otherDateTime) return Value.CompareTo(otherDateTime.Value);
        return BsonValueComparer.Compare(this, other);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonDateTime otherDateTime && Value == otherDateTime.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);

    // IConvertible 接口的实现
    public override TypeCode GetTypeCode() => TypeCode.DateTime;
    public override bool ToBoolean(IFormatProvider? provider) => Convert.ToBoolean(Value, provider);
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => Value;
    public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(Value, provider);
    public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(Value, provider);
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(Value, provider);
    public override long ToInt64(IFormatProvider? provider) => Value.Ticks;
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Value, provider);
    public override string ToString(IFormatProvider? provider) => ToString();
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(DateTime)) return Value;
        if (conversionType == typeof(BsonDateTime)) return this;
        if (conversionType == typeof(object)) return this;
        if (conversionType == typeof(string)) return ToString();
        return Convert.ChangeType(Value, conversionType, provider);
    }
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}
