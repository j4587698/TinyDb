using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;

namespace SimpleDb.Bson;

/// <summary>
/// BSON 正则表达式类型
/// </summary>
public sealed class BsonRegularExpression : BsonValue
{
    /// <summary>
    /// 正则表达式模式
    /// </summary>
    public string Pattern { get; }

    /// <summary>
    /// 正则表达式选项
    /// </summary>
    public string Options { get; }

    /// <summary>
    /// BSON 类型
    /// </summary>
    public override BsonType BsonType => BsonType.RegularExpression;

    /// <summary>
    /// 原始值
    /// </summary>
    public override object? RawValue => Pattern;

    /// <summary>
    /// 初始化 BsonRegularExpression
    /// </summary>
    /// <param name="pattern">正则表达式模式</param>
    /// <param name="options">正则表达式选项</param>
    public BsonRegularExpression(string pattern, string options = "")
    {
        Pattern = pattern ?? throw new ArgumentNullException(nameof(pattern));
        Options = options ?? "";
    }

    /// <summary>
    /// 从 .NET 正则表达式创建
    /// </summary>
    /// <param name="regex">.NET 正则表达式</param>
    /// <returns>BSON 正则表达式</returns>
    public static BsonRegularExpression FromRegex(Regex regex)
    {
        if (regex == null) throw new ArgumentNullException(nameof(regex));

        var options = ConvertOptions(regex.Options);
        return new BsonRegularExpression(regex.ToString(), options);
    }

    /// <summary>
    /// 转换为 .NET 正则表达式
    /// </summary>
    /// <returns>.NET 正则表达式</returns>
    public Regex ToRegex()
    {
        var options = ParseOptions(Options);
        return new Regex(Pattern, options);
    }

    /// <summary>
    /// 隐式转换从 string
    /// </summary>
    public static implicit operator BsonRegularExpression(string pattern) => new(pattern);

    /// <summary>
    /// 隐式转换从 Regex
    /// </summary>
    public static implicit operator BsonRegularExpression(Regex regex) => FromRegex(regex);

    /// <summary>
    /// 比较
    /// </summary>
    public override bool Equals(BsonValue? other)
    {
        if (other is not BsonRegularExpression otherRegex) return false;
        return Pattern == otherRegex.Pattern && Options == otherRegex.Options;
    }

    /// <summary>
    /// 获取哈希码
    /// </summary>
    public override int GetHashCode()
    {
        return HashCode.Combine(Pattern, Options);
    }

    /// <summary>
    /// 转换为字符串
    /// </summary>
    public override string ToString()
    {
        return $"/{Pattern}/{Options}";
    }

    /// <summary>
    /// 比较
    /// </summary>
    public override int CompareTo(BsonValue? other)
    {
        if (other is null) return 1;
        if (other is BsonRegularExpression otherRegex) return Pattern.CompareTo(otherRegex.Pattern);
        return BsonType.CompareTo(other.BsonType);
    }

    /// <summary>
    /// 转换 .NET 选项为 BSON 选项
    /// </summary>
    private static string ConvertOptions(RegexOptions options)
    {
        var result = new System.Text.StringBuilder();

        if (options.HasFlag(RegexOptions.IgnoreCase))
            result.Append('i');
        if (options.HasFlag(RegexOptions.Multiline))
            result.Append('m');
        if (options.HasFlag(RegexOptions.Singleline))
            result.Append('s');
        if (options.HasFlag(RegexOptions.IgnorePatternWhitespace))
            result.Append('x');

        return result.ToString();
    }

    /// <summary>
    /// 转换 BSON 选项为 .NET 选项
    /// </summary>
    private static RegexOptions ParseOptions(string options)
    {
        var result = RegexOptions.None;

        foreach (var option in options.ToLowerInvariant())
        {
            switch (option)
            {
                case 'i':
                    result |= RegexOptions.IgnoreCase;
                    break;
                case 'm':
                    result |= RegexOptions.Multiline;
                    break;
                case 's':
                    result |= RegexOptions.Singleline;
                    break;
                case 'x':
                    result |= RegexOptions.IgnorePatternWhitespace;
                    break;
            }
        }

        return result;
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