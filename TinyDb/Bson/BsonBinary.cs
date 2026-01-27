using System.Diagnostics.CodeAnalysis;

namespace TinyDb.Bson;

/// <summary>
/// BSON 二进制数据类型
/// </summary>
public sealed class BsonBinary : BsonValue
{
    /// <summary>
    /// 二进制数据子类型
    /// </summary>
    public enum BinarySubType : byte
    {
        /// <summary>
        /// 通用二进制数据
        /// </summary>
        Generic = 0x00,

        /// <summary>
        /// 函数
        /// </summary>
        Function = 0x01,

        /// <summary>
        /// 旧版二进制数据（已废弃）
        /// </summary>
        BinaryOld = 0x02,

        /// <summary>
        /// UUID
        /// </summary>
        UuidLegacy = 0x03,

        /// <summary>
        /// UUID (RFC 4122)
        /// </summary>
        Uuid = 0x04,

        /// <summary>
        /// MD5 哈希
        /// </summary>
        Md5 = 0x05,

        /// <summary>
        /// 用户自定义
        /// </summary>
        UserDefined = 0x80
    }

    /// <summary>
    /// 二进制数据
    /// </summary>
    public byte[] Bytes { get; }

    /// <summary>
    /// 子类型
    /// </summary>
    public BinarySubType SubType { get; }

    /// <summary>
    /// 值（兼容性属性）
    /// </summary>
    public byte[] Value => Bytes;

    /// <summary>
    /// BSON 类型
    /// </summary>
    public override BsonType BsonType => BsonType.Binary;

    /// <summary>
    /// 原始值
    /// </summary>
    public override object? RawValue => Bytes;

    /// <summary>
    /// 初始化 BsonBinary
    /// </summary>
    /// <param name="bytes">二进制数据</param>
    /// <param name="subType">子类型</param>
    public BsonBinary(byte[] bytes, BinarySubType subType = BinarySubType.Generic)
    {
        Bytes = bytes ?? throw new ArgumentNullException(nameof(bytes));
        SubType = subType;
    }

    /// <summary>
    /// 初始化 BsonBinary 从 Guid
    /// </summary>
    /// <param name="guid">Guid值</param>
    /// <param name="subType">子类型</param>
    public BsonBinary(Guid guid, BinarySubType subType = BinarySubType.Uuid)
    {
        Bytes = guid.ToByteArray();
        SubType = subType;
    }

    /// <summary>
    /// 隐式转换从 byte[]
    /// </summary>
    public static implicit operator BsonBinary(byte[] bytes) => new(bytes);

    /// <summary>
    /// 隐式转换到 byte[]
    /// </summary>
    public static implicit operator byte[](BsonBinary binary) => binary.Bytes;

    /// <summary>
    /// 比较
    /// </summary>
    public override bool Equals(BsonValue? other)
    {
        if (other is not BsonBinary otherBinary) return false;
        if (SubType != otherBinary.SubType) return false;

        return Bytes.SequenceEqual(otherBinary.Bytes);
    }

    /// <summary>
    /// 获取哈希码
    /// </summary>
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(SubType);
        hash.Add(Bytes.Length);
        
        // Add content hash (optimization: maybe only sample if large?)
        // For correctness/coverage now, we iterate.
        foreach(var b in Bytes)
        {
            hash.Add(b);
        }
        
        return hash.ToHashCode();
    }

    /// <summary>
    /// 转换为字符串
    /// </summary>
    public override string ToString()
    {
        return $"Binary({SubType}, {Bytes.Length} bytes)";
    }

    /// <summary>
    /// 比较
    /// </summary>
    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonBinary otherBinary)
        {
            // 1. Length
            var lenDiff = Bytes.Length.CompareTo(otherBinary.Bytes.Length);
            if (lenDiff != 0) return lenDiff;

            // 2. SubType
            var subTypeDiff = SubType.CompareTo(otherBinary.SubType);
            if (subTypeDiff != 0) return subTypeDiff;

            // 3. Bytes
            for (int i = 0; i < Bytes.Length; i++)
            {
                var byteDiff = Bytes[i].CompareTo(otherBinary.Bytes[i]);
                if (byteDiff != 0) return byteDiff;
            }
            return 0;
        }
        return BsonType.CompareTo(other.BsonType);
    }

    // IConvertible 接口的简化实现
    public override TypeCode GetTypeCode() => TypeCode.Object;
    public override bool ToBoolean(IFormatProvider? provider) => throw new InvalidCastException();
    public override byte ToByte(IFormatProvider? provider) => throw new InvalidCastException();
    public override char ToChar(IFormatProvider? provider) => throw new InvalidCastException();
    public override DateTime ToDateTime(IFormatProvider? provider) => throw new InvalidCastException();
    public override decimal ToDecimal(IFormatProvider? provider) => throw new InvalidCastException();
    public override double ToDouble(IFormatProvider? provider) => throw new InvalidCastException();
    public override short ToInt16(IFormatProvider? provider) => throw new InvalidCastException();
    public override int ToInt32(IFormatProvider? provider) => throw new InvalidCastException();
    public override long ToInt64(IFormatProvider? provider) => throw new InvalidCastException();
    public override sbyte ToSByte(IFormatProvider? provider) => throw new InvalidCastException();
    public override float ToSingle(IFormatProvider? provider) => throw new InvalidCastException();
    public override string ToString(IFormatProvider? provider) => ToString();
    public override object ToType(Type conversionType, IFormatProvider? provider) => throw new InvalidCastException();
    public override ushort ToUInt16(IFormatProvider? provider) => throw new InvalidCastException();
    public override uint ToUInt32(IFormatProvider? provider) => throw new InvalidCastException();
    public override ulong ToUInt64(IFormatProvider? provider) => throw new InvalidCastException();
}