using System;

namespace TinyDb.Bson;

/// <summary>
/// 表示 BSON Decimal128 类型
/// </summary>
public sealed class BsonDecimal128 : BsonValue, IComparable<BsonDecimal128>, IEquatable<BsonDecimal128>
{
    /// <summary>
    /// Decimal128 值
    /// </summary>
    public Decimal128 Value { get; }

    public override BsonType BsonType => BsonType.Decimal128;

    public override object RawValue => Value;

    public BsonDecimal128(Decimal128 value)
    {
        Value = value;
    }

    public BsonDecimal128(decimal value)
    {
        Value = new Decimal128(value);
    }

    // Numeric conversions
    public override bool ToBoolean(IFormatProvider? provider) => Value.Equals(Decimal128.Zero) == false;
    public override double ToDouble(IFormatProvider? provider) => (double)Value.ToDecimal();
    public override decimal ToDecimal(IFormatProvider? provider) => Value.ToDecimal();
    public override int ToInt32(IFormatProvider? provider) => (int)Value.ToDecimal();
    public override long ToInt64(IFormatProvider? provider) => (long)Value.ToDecimal();
    
    public override short ToInt16(IFormatProvider? provider) => (short)Value.ToDecimal();
    public override ushort ToUInt16(IFormatProvider? provider) => (ushort)Value.ToDecimal();
    public override uint ToUInt32(IFormatProvider? provider) => (uint)Value.ToDecimal();
    public override ulong ToUInt64(IFormatProvider? provider) => (ulong)Value.ToDecimal();
    public override byte ToByte(IFormatProvider? provider) => (byte)Value.ToDecimal();
    public override sbyte ToSByte(IFormatProvider? provider) => (sbyte)Value.ToDecimal();
    public override float ToSingle(IFormatProvider? provider) => (float)Value.ToDecimal();

    // Non-numeric conversions
    public override char ToChar(IFormatProvider? provider) => throw new InvalidCastException();
    public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    
    public override string ToString(IFormatProvider? provider) => Value.ToString();

    // IConvertible support
    public override TypeCode GetTypeCode() => TypeCode.Decimal;
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(decimal)) return ToDecimal(provider);
        if (conversionType == typeof(Decimal128)) return Value;
        // Default BsonValue implementation might handle others via IConvertible if we call base, 
        // but BsonValue is abstract and these are abstract methods.
        // Simplified implementation:
        return Convert.ChangeType(Value.ToDecimal(), conversionType, provider);
    }

    // Comparison and Equality
    public int CompareTo(BsonDecimal128? other)
    {
        if (other == null) return 1;
        return Value.CompareTo(other.Value);
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other == null) return 1;
        if (other is BsonDecimal128 otherDec) return CompareTo(otherDec);
        
        try 
        {
            if (other is BsonInt32 i32) return Value.ToDecimal().CompareTo((decimal)i32.Value);
            if (other is BsonInt64 i64) return Value.ToDecimal().CompareTo((decimal)i64.Value);
            if (other is BsonDouble dbl) return Value.ToDecimal().CompareTo((decimal)dbl.Value);
        }
        catch { }
        
        return BsonType.CompareTo(other.BsonType);
    }

    public bool Equals(BsonDecimal128? other)
    {
        if (other == null) return false;
        return Value.Equals(other.Value);
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as BsonDecimal128);
    }

    public override bool Equals(BsonValue? other)
    {
        return Equals(other as BsonDecimal128);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(BsonType, Value);
    }

    public static implicit operator BsonDecimal128(decimal value) => new BsonDecimal128(value);
}