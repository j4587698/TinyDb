using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace TinyDb.Bson;

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
    public static implicit operator BsonValue(string? value) => value is null ? BsonNull.Value : new BsonString(value);

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
    /// 隐式转换：从 decimal 到 BsonValue
    /// </summary>
    public static implicit operator BsonValue(decimal value) => new BsonDecimal128(value);


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
        
        if (typeof(T) == typeof(decimal) && RawValue is Decimal128 d128)
            return (T)(object)d128.ToDecimal();
            
        if (typeof(T) == typeof(Decimal128) && RawValue is decimal d)
            return (T)(object)new Decimal128(d);

        throw new InvalidCastException($"Cannot convert {BsonType} ({RawValue?.GetType().Name}) to {typeof(T).Name}");
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
