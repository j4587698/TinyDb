using System.Reflection;

namespace TinyDb.Query;

internal static class ExpressionKnownMemberEvaluator
{
    public static object? EvaluateDateTimeProperty(DateTime value, string propertyName)
    {
        if (propertyName == nameof(DateTime.Year)) return value.Year;
        if (propertyName == nameof(DateTime.Month)) return value.Month;
        if (propertyName == nameof(DateTime.Day)) return value.Day;
        if (propertyName == nameof(DateTime.Hour)) return value.Hour;
        if (propertyName == nameof(DateTime.Minute)) return value.Minute;
        if (propertyName == nameof(DateTime.Second)) return value.Second;
        if (propertyName == nameof(DateTime.Date)) return value.Date;
        if (propertyName == nameof(DateTime.DayOfWeek)) return (int)value.DayOfWeek;
        return null;
    }

    public static object? EvaluateProperty(object? container, PropertyInfo property)
    {
        if (container is string s && property.Name == nameof(string.Length))
        {
            return s.Length;
        }

        if (container is DateTime dt)
        {
            return EvaluateDateTimeProperty(dt, property.Name);
        }

        return null;
    }
}
