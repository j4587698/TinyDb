using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using TinyDb.UI.Models;
using Avalonia.Data.Converters;

namespace TinyDb.UI.Converters;

/// <summary>
/// 文档字段值转换器，用于动态获取文档字段值
/// </summary>
public class DocumentFieldValueConverter : IMultiValueConverter
{
    public static readonly DocumentFieldValueConverter Instance = new();

    public object? Convert(IList<object?> values, Type targetType, object? parameter, CultureInfo culture)
    {
        if (values.Count < 2)
            return string.Empty;

        var fieldName = values[0] as string;
        var document = values[1] as DocumentItem;

        if (string.IsNullOrEmpty(fieldName) || document == null)
            return string.Empty;

        return document.GetFieldValue(fieldName);
    }

    public object[] ConvertBack(object value, Type[] targetTypes, object parameter, CultureInfo culture)
    {
        throw new NotImplementedException();
    }
}