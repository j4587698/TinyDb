using System.Globalization;

namespace TinyDb.Bson;

/// <summary>
/// BsonValue 扩展方法，提供便捷的类型转换功能
/// </summary>
public static class BsonValueExtensions
{
    /// <summary>
    /// 转换为 32 位整数
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>整数值</returns>
    public static int ToInt32(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToInt32(provider);
    }

    /// <summary>
    /// 转换为 64 位整数
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>整数值</returns>
    public static long ToInt64(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToInt64(provider);
    }

    /// <summary>
    /// 转换为布尔值
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>布尔值</returns>
    public static bool ToBoolean(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToBoolean(provider);
    }

    /// <summary>
    /// 转换为双精度浮点数
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>浮点数值</returns>
    public static double ToDouble(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToDouble(provider);
    }

    /// <summary>
    /// 转换为字符串
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>字符串值</returns>
    public static string ToString(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToString(provider);
    }

    /// <summary>
    /// 转换为 DateTime
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>DateTime 值</returns>
    public static DateTime ToDateTime(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToDateTime(provider);
    }

    /// <summary>
    /// 转换为十进制数
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>十进制数值</returns>
    public static decimal ToDecimal(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToDecimal(provider);
    }

    /// <summary>
    /// 转换为单精度浮点数
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>单精度浮点数值</returns>
    public static float ToSingle(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToSingle(provider);
    }

    /// <summary>
    /// 转换为 16 位整数
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>16 位整数值</returns>
    public static short ToInt16(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToInt16(provider);
    }

    /// <summary>
    /// 转换为无符号 16 位整数
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>无符号 16 位整数值</returns>
    public static ushort ToUInt16(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToUInt16(provider);
    }

    /// <summary>
    /// 转换为无符号 32 位整数
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>无符号 32 位整数值</returns>
    public static uint ToUInt32(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToUInt32(provider);
    }

    /// <summary>
    /// 转换为无符号 64 位整数
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>无符号 64 位整数值</returns>
    public static ulong ToUInt64(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToUInt64(provider);
    }

    /// <summary>
    /// 转换为字节
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>字节值</returns>
    public static byte ToByte(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToByte(provider);
    }

    /// <summary>
    /// 转换为有符号字节
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>有符号字节值</returns>
    public static sbyte ToSByte(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToSByte(provider);
    }

    /// <summary>
    /// 转换为字符
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="provider">格式化提供程序</param>
    /// <returns>字符值</returns>
    public static char ToChar(this BsonValue value, IFormatProvider? provider = null)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        return value.ToChar(provider);
    }
}