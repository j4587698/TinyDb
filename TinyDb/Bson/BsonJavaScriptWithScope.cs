using System;

namespace TinyDb.Bson;

/// <summary>
/// 表示带有作用域的 BSON JavaScript 类型
/// </summary>
public sealed class BsonJavaScriptWithScope : BsonValue, IComparable<BsonJavaScriptWithScope>, IEquatable<BsonJavaScriptWithScope>
{
    /// <summary>
    /// JavaScript 代码
    /// </summary>
    public string Code { get; }

    /// <summary>
    /// 作用域文档
    /// </summary>
    public BsonDocument Scope { get; }

    public override BsonType BsonType => BsonType.JavaScriptWithScope;

    public override object RawValue => Code;

    public BsonJavaScriptWithScope(string code, BsonDocument scope)
    {
        Code = code ?? throw new ArgumentNullException(nameof(code));
        Scope = scope ?? throw new ArgumentNullException(nameof(scope));
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

    public override TypeCode GetTypeCode() => TypeCode.Object;
    public override object ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(string)) return Code;
        throw new InvalidCastException();
    }

    public int CompareTo(BsonJavaScriptWithScope? other)
    {
        if (other == null) return 1;
        int result = string.Compare(Code, other.Code, StringComparison.Ordinal);
        if (result != 0) return result;
        return Scope.CompareTo(other.Scope);
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other == null) return 1;
        if (other is BsonJavaScriptWithScope otherJs) return CompareTo(otherJs);
        return BsonType.CompareTo(other.BsonType);
    }

    public bool Equals(BsonJavaScriptWithScope? other)
    {
        if (other == null) return false;
        return Code == other.Code && Scope.Equals(other.Scope);
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as BsonJavaScriptWithScope);
    }

    public override bool Equals(BsonValue? other)
    {
        return Equals(other as BsonJavaScriptWithScope);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(BsonType, Code, Scope);
    }
}