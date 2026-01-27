using System;
using System.Globalization;

namespace TinyDb.Bson;

/// <summary>
/// 表示 BSON JavaScript 类型
/// </summary>
public sealed class BsonJavaScript : BsonValue, IComparable<BsonJavaScript>, IEquatable<BsonJavaScript>
{
    /// <summary>
    /// JavaScript 代码
    /// </summary>
    public string Code { get; }

    public override BsonType BsonType => BsonType.JavaScript;

    public override object RawValue => Code;

    public BsonJavaScript(string code)
    {
        Code = code ?? throw new ArgumentNullException(nameof(code));
    }

    public override bool ToBoolean(IFormatProvider? provider) => !string.IsNullOrEmpty(Code);
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

    public override string ToString(IFormatProvider? provider) => Code;

    public override TypeCode GetTypeCode() => TypeCode.String;
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(string)) return Code;
        throw new InvalidCastException();
    }

    public int CompareTo(BsonJavaScript? other)
    {
        if (other == null) return 1;
        return string.Compare(Code, other.Code, StringComparison.Ordinal);
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other == null) return 1;
        if (other is BsonJavaScript otherJs) return CompareTo(otherJs);
        return BsonType.CompareTo(other.BsonType);
    }

    public bool Equals(BsonJavaScript? other)
    {
        if (other == null) return false;
        return Code == other.Code;
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as BsonJavaScript);
    }

    public override bool Equals(BsonValue? other)
    {
        return Equals(other as BsonJavaScript);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(BsonType, Code);
    }
}