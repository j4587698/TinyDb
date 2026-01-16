using System;
using System.Collections.Generic;
using System.Globalization;
using Avalonia.Data.Converters;
using TinyDb.UI.Models;

namespace TinyDb.UI.Converters;

/// <summary>
/// 字段类型转换器，用于在TableFieldType枚举和字符串显示之间转换
/// </summary>
public class FieldTypeConverter : IValueConverter
{
    /// <summary>
    /// 字段类型到显示名称的映射
    /// </summary>
    private static readonly Dictionary<TableFieldType, string> FieldTypeToDisplay = new()
    {
        { TableFieldType.String, "字符串 (String)" },
        { TableFieldType.Integer, "整数 (Integer)" },
        { TableFieldType.Long, "长整数 (Long)" },
        { TableFieldType.Double, "浮点数 (Double)" },
        { TableFieldType.Decimal, "小数 (Decimal)" },
        { TableFieldType.Boolean, "布尔值 (Boolean)" },
        { TableFieldType.DateTime, "日期时间 (DateTime)" },
        { TableFieldType.DateTimeOffset, "日期时间 (DateTimeOffset)" },
        { TableFieldType.Guid, "GUID" },
        { TableFieldType.Binary, "二进制 (Binary)" },
        { TableFieldType.Json, "JSON" },
        { TableFieldType.Array, "数组 (Array)" },
        { TableFieldType.Object, "对象 (Object)" },
        { TableFieldType.Reference, "引用 (Reference)" }
    };

    /// <summary>
    /// 显示名称到字段类型的映射
    /// </summary>
    private static readonly Dictionary<string, TableFieldType> DisplayToFieldType = new()
    {
        { "字符串 (String)", TableFieldType.String },
        { "整数 (Integer)", TableFieldType.Integer },
        { "长整数 (Long)", TableFieldType.Long },
        { "浮点数 (Double)", TableFieldType.Double },
        { "小数 (Decimal)", TableFieldType.Decimal },
        { "布尔值 (Boolean)", TableFieldType.Boolean },
        { "日期时间 (DateTime)", TableFieldType.DateTime },
        { "日期时间 (DateTimeOffset)", TableFieldType.DateTimeOffset },
        { "GUID", TableFieldType.Guid },
        { "二进制 (Binary)", TableFieldType.Binary },
        { "JSON", TableFieldType.Json },
        { "数组 (Array)", TableFieldType.Array },
        { "对象 (Object)", TableFieldType.Object },
        { "引用 (Reference)", TableFieldType.Reference }
    };

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is TableFieldType fieldType)
        {
            return FieldTypeToDisplay.TryGetValue(fieldType, out var displayName) ? displayName : value.ToString();
        }
        return value;
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value is string displayName)
        {
            return DisplayToFieldType.TryGetValue(displayName, out var fieldType) ? fieldType : TableFieldType.String;
        }
        return TableFieldType.String;
    }
}