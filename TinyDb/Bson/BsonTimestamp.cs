using System.Diagnostics.CodeAnalysis;

namespace TinyDb.Bson;

/// <summary>
/// BSON 时间戳类型（MongoDB 内部使用）
/// </summary>
public sealed class BsonTimestamp : BsonValue
{
    /// <summary>
    /// 时间戳值
    /// </summary>
    public long Value { get; }

    /// <summary>
    /// 增量计数器
    /// </summary>
    public int Increment => (int)(Value & 0xFFFFFFFF);

    /// <summary>
    /// Unix 时间戳（秒）
    /// </summary>
    public int Timestamp => (int)((Value >> 32) & 0xFFFFFFFF);

    /// <summary>
    /// BSON 类型
    /// </summary>
    public override BsonType BsonType => BsonType.Timestamp;

    /// <summary>
    /// 原始值
    /// </summary>
    public override object? RawValue => Value;

    /// <summary>
    /// 初始化 BsonTimestamp
    /// </summary>
    /// <param name="timestamp">Unix 时间戳（秒）</param>
    /// <param name="increment">增量计数器</param>
    public BsonTimestamp(int timestamp, int increment)
    {
        if (increment < 0)
            throw new ArgumentOutOfRangeException(nameof(increment), "Increment must be non-negative");

        Value = ((long)timestamp << 32) | (uint)increment;
    }

    /// <summary>
    /// 从完整值初始化 BsonTimestamp
    /// </summary>
    /// <param name="value">完整的时间戳值</param>
    public BsonTimestamp(long value)
    {
        Value = value;
    }

    /// <summary>
    /// 创建当前时间的时间戳
    /// </summary>
    /// <param name="increment">增量计数器</param>
    /// <returns>BsonTimestamp</returns>
    public static BsonTimestamp CreateCurrent(int increment = 0)
    {
        var unixTime = (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        return new BsonTimestamp(unixTime, increment);
    }

    /// <summary>
    /// 隐式转换从 long
    /// </summary>
    public static implicit operator BsonTimestamp(long value) => new(value);

    /// <summary>
    /// 隐式转换到 long
    /// </summary>
    public static implicit operator long(BsonTimestamp timestamp) => timestamp.Value;

    /// <summary>
    /// 比较
    /// </summary>
    public override bool Equals(BsonValue? other)
    {
        return other is BsonTimestamp otherTimestamp && Value == otherTimestamp.Value;
    }

    /// <summary>
    /// 获取哈希码
    /// </summary>
    public override int GetHashCode()
    {
        return Value.GetHashCode();
    }

    /// <summary>
    /// 转换为字符串
    /// </summary>
    public override string ToString()
    {
        return $"Timestamp({Value}, Time: {Timestamp}, Inc: {Increment})";
    }

    /// <summary>
    /// 比较
    /// </summary>
    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonTimestamp otherTimestamp) return Value.CompareTo(otherTimestamp.Value);
        return BsonType.CompareTo(other.BsonType);
    }

    /// <summary>
    /// 转换为 DateTime
    /// </summary>
    /// <returns>DateTime</returns>
    public DateTime ToDateTime()
    {
        return DateTimeOffset.FromUnixTimeSeconds(Timestamp).DateTime;
    }

    // IConvertible 接口的简化实现
    public override TypeCode GetTypeCode() => TypeCode.Object;
    public override bool ToBoolean(IFormatProvider? provider) => throw new InvalidCastException();
    public override byte ToByte(IFormatProvider? provider) => throw new InvalidCastException();
    public override char ToChar(IFormatProvider? provider) => throw new InvalidCastException();
    public override DateTime ToDateTime(IFormatProvider? provider) => ToDateTime();
    public override decimal ToDecimal(IFormatProvider? provider) => Value;
    public override double ToDouble(IFormatProvider? provider) => Value;
    public override short ToInt16(IFormatProvider? provider) => (short)Value;
    public override int ToInt32(IFormatProvider? provider) => (int)Value;
    public override long ToInt64(IFormatProvider? provider) => Value;
    public override sbyte ToSByte(IFormatProvider? provider) => (sbyte)Value;
    public override float ToSingle(IFormatProvider? provider) => Value;
    public override string ToString(IFormatProvider? provider) => ToString();
    public override object ToType(Type conversionType, IFormatProvider? provider) => throw new InvalidCastException();
    public override ushort ToUInt16(IFormatProvider? provider) => (ushort)Value;
    public override uint ToUInt32(IFormatProvider? provider) => (uint)Value;
    public override ulong ToUInt64(IFormatProvider? provider) => (ulong)Value;
}
