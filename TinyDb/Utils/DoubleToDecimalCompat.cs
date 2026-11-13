using System.Globalization;

namespace TinyDb.Utils;

/// <summary>
/// .NET 11 起 <c>double</c>/<c>float</c> 到 <c>decimal</c> 的转换改为"精确转换"，
/// 例如 <c>Convert.ToDecimal(3.14)</c> 从 <c>3.14</c> 变成 <c>3.1400000000000001243449787580</c>。
/// TinyDb 需要保持"写入什么就读出什么"的语义，否则用 <c>decimal</c> 查询 <c>double</c>
/// 字段做相等比较将无法命中，因此在新目标框架下显式恢复 .NET 10 及以前的行为
/// （double 保留 15 位有效数字，float 保留 7 位有效数字）。
/// </summary>
internal static class DoubleToDecimalCompat
{
    /// <summary>
    /// 等价于 .NET 10 及以前的 <see cref="Convert.ToDecimal(double, IFormatProvider?)"/>。
    /// </summary>
    public static decimal FromDouble(double value)
    {
#if NET11_0_OR_GREATER
        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            throw new OverflowException("Value was either too large or too small for a Decimal.");
        }

        return decimal.Parse(
            value.ToString("G15", CultureInfo.InvariantCulture),
            NumberStyles.Float,
            CultureInfo.InvariantCulture);
#else
        return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
#endif
    }

    /// <summary>
    /// 等价于 .NET 10 及以前的 <see cref="Convert.ToDecimal(float, IFormatProvider?)"/>。
    /// </summary>
    public static decimal FromSingle(float value)
    {
#if NET11_0_OR_GREATER
        if (float.IsNaN(value) || float.IsInfinity(value))
        {
            throw new OverflowException("Value was either too large or too small for a Decimal.");
        }

        return decimal.Parse(
            value.ToString("G7", CultureInfo.InvariantCulture),
            NumberStyles.Float,
            CultureInfo.InvariantCulture);
#else
        return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
#endif
    }
}
