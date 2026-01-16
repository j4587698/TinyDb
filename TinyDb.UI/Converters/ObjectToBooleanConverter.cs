using System;
using System.Globalization;
using Avalonia.Data.Converters;

namespace TinyDb.UI.Converters;

/// <summary>
/// 对象到布尔值转换器，用于检查对象是否为null或空
/// </summary>
public class ObjectToBooleanConverter : IValueConverter
{
    public static readonly ObjectToBooleanConverter Instance = new();

    public object? Convert(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        if (value == null)
            return false;

        if (value is string str)
            return !string.IsNullOrWhiteSpace(str);

        if (value is System.Collections.ICollection collection)
            return collection.Count > 0;

        return true;
    }

    public object? ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture)
    {
        throw new NotImplementedException();
    }
}