using System;

namespace TinyDb.Bson;

/// <summary>
/// 表示 BSON Symbol 类型（已在 BSON 规范中弃用，但为了兼容性仍需支持）
/// </summary>
public sealed class BsonSymbol : BsonValue, IComparable<BsonSymbol>, IEquatable<BsonSymbol>
{
    /// <summary>
    /// 符号名称
    /// </summary>
    public string Name { get; }

    public override BsonType BsonType => BsonType.Symbol;

    public override object RawValue => Name;

    public BsonSymbol(string name)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
    }

    public override bool ToBoolean(IFormatProvider? provider) => !string.IsNullOrEmpty(Name);
    public override double ToDouble(IFormatProvider? provider) => throw new InvalidCastException();
    public override decimal ToDecimal(IFormatProvider? provider) => throw new InvalidCastException();
    public override int ToInt32(IFormatProvider? provider) => throw new InvalidCastException();
    public override long ToInt64(IFormatProvider? provider) => throw new InvalidCastException();
    public override short ToInt16(IFormatProvider? provider) => throw new InvalidCastException();
    public override ushort ToUInt16(IFormatProvider? provider) => throw new InvalidCastException();
    public override uint ToUInt32(IFormatProvider? provider) => throw new InvalidCastException();
    public override ulong ToUInt64(IFormatProvider? provider) => throw new InvalidCastException();
    public override byte ToByte(IFormatProvider? provider) => throw new InvalidCastException();
    public override sbyte ToSByte(IFormatProvider? provider) => throw new InvalidCastException();
    public override float ToSingle(IFormatProvider? provider) => throw new InvalidCastException();
    public override char ToChar(IFormatProvider? provider) => throw new InvalidCastException();
    public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();

    public override string ToString(IFormatProvider? provider) => Name;

    public override TypeCode GetTypeCode() => TypeCode.String;
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(string)) return Name;
        throw new InvalidCastException();
    }

    public int CompareTo(BsonSymbol? other)
    {
        if (other == null) return 1;
        return string.Compare(Name, other.Name, StringComparison.Ordinal);
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other == null) return 1;
        if (other is BsonSymbol otherSymbol) return CompareTo(otherSymbol);
        return BsonType.CompareTo(other.BsonType);
    }

    public bool Equals(BsonSymbol? other)
    {
        if (other == null) return false;
        return Name == other.Name;
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as BsonSymbol);
    }

    public override bool Equals(BsonValue? other)
    {
        return Equals(other as BsonSymbol);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(BsonType, Name);
    }
}