using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace SimpleDb.Bson;

/// <summary>
/// BSON 值的基类，表示所有 BSON 类型的统一接口
/// </summary>
public abstract class BsonValue : IComparable<BsonValue>, IEquatable<BsonValue>, IConvertible
{
    /// <summary>
    /// 获取 BSON 值的类型
    /// </summary>
    public abstract BsonType BsonType { get; }

    /// <summary>
    /// 获取值是否为 null
    /// </summary>
    public virtual bool IsNull => BsonType == BsonType.Null;

    /// <summary>
    /// 获取是否为文档类型
    /// </summary>
    public virtual bool IsDocument => BsonType == BsonType.Document;

    /// <summary>
    /// 获取是否为数组类型
    /// </summary>
    public virtual bool IsArray => BsonType == BsonType.Array;

    /// <summary>
    /// 获取是否为数值类型
    /// </summary>
    public virtual bool IsNumeric => BsonType is BsonType.Double or BsonType.Int32 or BsonType.Int64 or BsonType.Decimal128;

    /// <summary>
    /// 获取是否为字符串类型
    /// </summary>
    public virtual bool IsString => BsonType == BsonType.String;

    /// <summary>
    /// 获取是否为布尔类型
    /// </summary>
    public virtual bool IsBoolean => BsonType == BsonType.Boolean;

    /// <summary>
    /// 获取是否为 ObjectId 类型
    /// </summary>
    public virtual bool IsObjectId => BsonType == BsonType.ObjectId;

    /// <summary>
    /// 获取是否为 DateTime 类型
    /// </summary>
    public virtual bool IsDateTime => BsonType == BsonType.DateTime;

    /// <summary>
    /// 获取原始值
    /// </summary>
    public abstract object? RawValue { get; }

    /// <summary>
    /// 隐式转换：从 string 到 BsonValue
    /// </summary>
    public static implicit operator BsonValue(string? value) => new BsonString(value);

    /// <summary>
    /// 隐式转换：从 int 到 BsonValue
    /// </summary>
    public static implicit operator BsonValue(int value) => new BsonInt32(value);

    /// <summary>
    /// 隐式转换：从 long 到 BsonValue
    /// </summary>
    public static implicit operator BsonValue(long value) => new BsonInt64(value);

    /// <summary>
    /// 隐式转换：从 double 到 BsonValue
    /// </summary>
    public static implicit operator BsonValue(double value) => new BsonDouble(value);

    /// <summary>
    /// 隐式转换：从 bool 到 BsonValue
    /// </summary>
    public static implicit operator BsonValue(bool value) => new BsonBoolean(value);

    /// <summary>
    /// 隐式转换：从 DateTime 到 BsonValue
    /// </summary>
    public static implicit operator BsonValue(DateTime value) => new BsonDateTime(value);

    /// <summary>
    /// 隐式转换：从 ObjectId 到 BsonValue
    /// </summary>
    public static implicit operator BsonValue(ObjectId value) => new BsonObjectId(value);

    
    /// <summary>
    /// 创建 null 值
    /// </summary>
    public static BsonValue Null => BsonNull.Value;

    /// <summary>
    /// 比较两个 BsonValue
    /// </summary>
    public abstract int CompareTo(BsonValue? other);

    /// <summary>
    /// 检查相等性
    /// </summary>
    public abstract bool Equals(BsonValue? other);

    /// <summary>
    /// 获取哈希码
    /// </summary>
    public override abstract int GetHashCode();

    /// <summary>
    /// 转换为字符串表示
    /// </summary>
    public override string ToString() => RawValue?.ToString() ?? string.Empty;

    /// <summary>
    /// 重写 Equals 方法
    /// </summary>
    public override bool Equals(object? obj)
    {
        if (ReferenceEquals(this, obj)) return true;
        if (obj is null) return false;
        if (obj is BsonValue bsonValue) return Equals(bsonValue);
        return false;
    }

    /// <summary>
    /// 相等操作符
    /// </summary>
    public static bool operator ==(BsonValue? left, BsonValue? right)
    {
        if (ReferenceEquals(left, right)) return true;
        if (left is null || right is null) return false;
        return left.Equals(right);
    }

    /// <summary>
    /// 不等操作符
    /// </summary>
    public static bool operator !=(BsonValue? left, BsonValue? right)
    {
        return !(left == right);
    }

    /// <summary>
    /// 类型转换方法
    /// </summary>
    public virtual T As<T>()
    {
        if (RawValue is T value) return value;
        throw new InvalidCastException($"Cannot convert {BsonType} to {typeof(T).Name}");
    }

    /// <summary>
    /// 尝试类型转换
    /// </summary>
    public virtual bool TryAs<T>([NotNullWhen(true)] out T? value)
    {
        value = default;
        if (RawValue is T result)
        {
            value = result;
            return true;
        }
        return false;
    }

    // IConvertible 实现
    public abstract TypeCode GetTypeCode();

    public abstract bool ToBoolean(IFormatProvider? provider);

    public abstract byte ToByte(IFormatProvider? provider);

    public abstract char ToChar(IFormatProvider? provider);

    public abstract DateTime ToDateTime(IFormatProvider? provider);

    public abstract decimal ToDecimal(IFormatProvider? provider);

    public abstract double ToDouble(IFormatProvider? provider);

    public abstract short ToInt16(IFormatProvider? provider);

    public abstract int ToInt32(IFormatProvider? provider);

    public abstract long ToInt64(IFormatProvider? provider);

    public abstract sbyte ToSByte(IFormatProvider? provider);

    public abstract float ToSingle(IFormatProvider? provider);

    public abstract string ToString(IFormatProvider? provider);

    public abstract object ToType(Type conversionType, IFormatProvider? provider);

    public abstract ushort ToUInt16(IFormatProvider? provider);

    public abstract uint ToUInt32(IFormatProvider? provider);

    public abstract ulong ToUInt64(IFormatProvider? provider);
}

/// <summary>
/// BSON 字符串值
/// </summary>
public sealed class BsonString : BsonValue
{
    public override BsonType BsonType => BsonType.String;
    public override object? RawValue { get; }

    public string Value { get; }

    public BsonString(string? value)
    {
        Value = value ?? string.Empty;
        RawValue = value;
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonString otherString) return string.Compare(Value, otherString.Value, StringComparison.Ordinal);
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonString otherString && Value == otherString.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value;

    public static implicit operator string(BsonString value) => value.Value;

    // IConvertible 实现
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
    public override object ToType(Type conversionType, IFormatProvider? provider) => Convert.ChangeType(Value, conversionType, provider);
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}

/// <summary>
/// BSON 32位整数值
/// </summary>
public sealed class BsonInt32 : BsonValue
{
    public override BsonType BsonType => BsonType.Int32;
    public override object? RawValue { get; }

    public int Value { get; }

    public BsonInt32(int value)
    {
        Value = value;
        RawValue = value;
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonInt32 otherInt) return Value.CompareTo(otherInt.Value);
        if (other.IsNumeric) return Convert.ToDouble(Value).CompareTo(other.ToDouble(CultureInfo.InvariantCulture));
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonInt32 otherInt && Value == otherInt.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);

    public static implicit operator int(BsonInt32 value) => value.Value;

    // IConvertible 实现
    public override TypeCode GetTypeCode() => TypeCode.Int32;

    public override bool ToBoolean(IFormatProvider? provider) => Convert.ToBoolean(Value, provider);
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => Convert.ToDateTime(Value, provider);
    public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(Value, provider);
    public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(Value, provider);
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Value;
    public override long ToInt64(IFormatProvider? provider) => Convert.ToInt64(Value, provider);
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Value, provider);
    public override string ToString(IFormatProvider? provider) => Value.ToString(provider);
    public override object ToType(Type conversionType, IFormatProvider? provider) => Convert.ChangeType(Value, conversionType, provider);
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}

/// <summary>
/// BSON 64位整数值
/// </summary>
public sealed class BsonInt64 : BsonValue
{
    public override BsonType BsonType => BsonType.Int64;
    public override object? RawValue { get; }

    public long Value { get; }

    public BsonInt64(long value)
    {
        Value = value;
        RawValue = value;
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonInt64 otherLong) return Value.CompareTo(otherLong.Value);
        if (other.IsNumeric) return Convert.ToDouble(Value).CompareTo(other.ToDouble(CultureInfo.InvariantCulture));
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonInt64 otherLong && Value == otherLong.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);

    public static implicit operator long(BsonInt64 value) => value.Value;

    // IConvertible 实现
    public override TypeCode GetTypeCode() => TypeCode.Int64;

    public override bool ToBoolean(IFormatProvider? provider) => Convert.ToBoolean(Value, provider);
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => Convert.ToDateTime(Value, provider);
    public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(Value, provider);
    public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(Value, provider);
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(Value, provider);
    public override long ToInt64(IFormatProvider? provider) => Value;
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Value, provider);
    public override string ToString(IFormatProvider? provider) => Value.ToString(provider);
    public override object ToType(Type conversionType, IFormatProvider? provider) => Convert.ChangeType(Value, conversionType, provider);
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}

/// <summary>
/// BSON 双精度浮点数值
/// </summary>
public sealed class BsonDouble : BsonValue
{
    public override BsonType BsonType => BsonType.Double;
    public override object? RawValue { get; }

    public double Value { get; }

    public BsonDouble(double value)
    {
        Value = value;
        RawValue = value;
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonDouble otherDouble) return Value.CompareTo(otherDouble.Value);
        if (other.IsNumeric) return Value.CompareTo(other.ToDouble(CultureInfo.InvariantCulture));
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonDouble otherDouble && Value.Equals(otherDouble.Value);
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);

    public static implicit operator double(BsonDouble value) => value.Value;

    // IConvertible 实现
    public override TypeCode GetTypeCode() => TypeCode.Double;

    public override bool ToBoolean(IFormatProvider? provider) => Convert.ToBoolean(Value, provider);
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => Convert.ToDateTime(Value, provider);
    public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(Value, provider);
    public override double ToDouble(IFormatProvider? provider) => Value;
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(Value, provider);
    public override long ToInt64(IFormatProvider? provider) => Convert.ToInt64(Value, provider);
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Value, provider);
    public override string ToString(IFormatProvider? provider) => Value.ToString(provider);
    public override object ToType(Type conversionType, IFormatProvider? provider) => Convert.ChangeType(Value, conversionType, provider);
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}

/// <summary>
/// BSON 布尔值
/// </summary>
public sealed class BsonBoolean : BsonValue
{
    public override BsonType BsonType => BsonType.Boolean;
    public override object? RawValue { get; }

    public bool Value { get; }

    public BsonBoolean(bool value)
    {
        Value = value;
        RawValue = value;
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonBoolean otherBool) return Value.CompareTo(otherBool.Value);
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonBoolean otherBool && Value == otherBool.Value;
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString(CultureInfo.InvariantCulture);

    public static implicit operator bool(BsonBoolean value) => value.Value;

    // IConvertible 实现
    public override TypeCode GetTypeCode() => TypeCode.Boolean;

    public override bool ToBoolean(IFormatProvider? provider) => Value;
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
    public override string ToString(IFormatProvider? provider) => Value.ToString(provider);
    public override object ToType(Type conversionType, IFormatProvider? provider) => Convert.ChangeType(Value, conversionType, provider);
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}

/// <summary>
/// BSON ObjectId 值
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

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonObjectId otherObjectId) return Value.CompareTo(otherObjectId.Value);
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonObjectId otherObjectId && Value.Equals(otherObjectId.Value);
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString();

    public static implicit operator ObjectId(BsonObjectId value) => value.Value;

    // IConvertible 实现
    public override TypeCode GetTypeCode() => TypeCode.Object;

    public override bool ToBoolean(IFormatProvider? provider) => Convert.ToBoolean(Value, provider);
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => Value.Timestamp;
    public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(Value, provider);
    public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(Value, provider);
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(Value, provider);
    public override long ToInt64(IFormatProvider? provider) => Convert.ToInt64(Value, provider);
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Value, provider);
    public override string ToString(IFormatProvider? provider) => Value.ToString();
    public override object ToType(Type conversionType, IFormatProvider? provider) => Convert.ChangeType(Value, conversionType, provider);
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}

/// <summary>
/// BSON DateTime 值
/// </summary>
public sealed class BsonDateTime : BsonValue
{
    public override BsonType BsonType => BsonType.DateTime;
    public override object? RawValue { get; }

    public DateTime Value { get; }

    public BsonDateTime(DateTime value)
    {
        Value = value;
        RawValue = value;
    }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonDateTime otherDateTime) return Value.CompareTo(otherDateTime.Value);
        return BsonType.CompareTo(other.BsonType);
    }

    public override bool Equals(BsonValue? other)
    {
        return other is BsonDateTime otherDateTime && Value.Equals(otherDateTime.Value);
    }

    public override int GetHashCode() => Value.GetHashCode();

    public override string ToString() => Value.ToString("yyyy-MM-ddTHH:mm:ss.fffZ", CultureInfo.InvariantCulture);

    public static implicit operator DateTime(BsonDateTime value) => value.Value;

    // IConvertible 实现
    public override TypeCode GetTypeCode() => TypeCode.DateTime;

    public override bool ToBoolean(IFormatProvider? provider) => Convert.ToBoolean(Value, provider);
    public override byte ToByte(IFormatProvider? provider) => Convert.ToByte(Value, provider);
    public override char ToChar(IFormatProvider? provider) => Convert.ToChar(Value, provider);
    public override DateTime ToDateTime(IFormatProvider? provider) => Value;
    public override decimal ToDecimal(IFormatProvider? provider) => Convert.ToDecimal(Value, provider);
    public override double ToDouble(IFormatProvider? provider) => Convert.ToDouble(Value, provider);
    public override short ToInt16(IFormatProvider? provider) => Convert.ToInt16(Value, provider);
    public override int ToInt32(IFormatProvider? provider) => Convert.ToInt32(Value, provider);
    public override long ToInt64(IFormatProvider? provider) => Convert.ToInt64(Value, provider);
    public override sbyte ToSByte(IFormatProvider? provider) => Convert.ToSByte(Value, provider);
    public override float ToSingle(IFormatProvider? provider) => Convert.ToSingle(Value, provider);
    public override string ToString(IFormatProvider? provider) => Value.ToString(provider);
    public override object ToType(Type conversionType, IFormatProvider? provider) => Convert.ChangeType(Value, conversionType, provider);
    public override ushort ToUInt16(IFormatProvider? provider) => Convert.ToUInt16(Value, provider);
    public override uint ToUInt32(IFormatProvider? provider) => Convert.ToUInt32(Value, provider);
    public override ulong ToUInt64(IFormatProvider? provider) => Convert.ToUInt64(Value, provider);
}

/// <summary>
/// BSON null 值
/// </summary>
public sealed class BsonNull : BsonValue
{
    public static readonly BsonNull Value = new();

    public override BsonType BsonType => BsonType.Null;
    public override object? RawValue => null;
    public override bool IsNull => true;

    private BsonNull() { }

    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 0;
        return other.IsNull ? 0 : -1;
    }

    public override bool Equals(BsonValue? other)
    {
        return other is null || other.IsNull;
    }

    public override int GetHashCode() => 0;

    public override string ToString() => "null";

    // IConvertible 实现
    public override TypeCode GetTypeCode() => TypeCode.Empty;

    public override bool ToBoolean(IFormatProvider? provider) => false;
    public override byte ToByte(IFormatProvider? provider) => 0;
    public override char ToChar(IFormatProvider? provider) => '\0';
    public override DateTime ToDateTime(IFormatProvider? provider) => DateTime.MinValue;
    public override decimal ToDecimal(IFormatProvider? provider) => 0;
    public override double ToDouble(IFormatProvider? provider) => 0;
    public override short ToInt16(IFormatProvider? provider) => 0;
    public override int ToInt32(IFormatProvider? provider) => 0;
    public override long ToInt64(IFormatProvider? provider) => 0;
    public override sbyte ToSByte(IFormatProvider? provider) => 0;
    public override float ToSingle(IFormatProvider? provider) => 0;
    public override string ToString(IFormatProvider? provider) => string.Empty;
    public override object ToType(Type conversionType, IFormatProvider? provider) => null!;
    public override ushort ToUInt16(IFormatProvider? provider) => 0;
    public override uint ToUInt32(IFormatProvider? provider) => 0;
    public override ulong ToUInt64(IFormatProvider? provider) => 0;
}