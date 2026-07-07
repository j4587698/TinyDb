using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Utils;

namespace TinyDb.Query;

public static partial class ExpressionEvaluator
{

    private static object? GetMemberValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(MemberExpression expression, T entity)
        where T : class
    {
        object? target = entity;
        if (expression.Expression != null && expression.Expression.NodeType != ExpressionType.Parameter)
        {
            target = EvaluateValue(expression.Expression, entity);
        }

        if (target == null)
        {
            if (expression.MemberName == "HasValue") return false;
            if (expression.MemberName == "Value") return null;
            return null;
        }

        // BsonDocument stores data as dictionary entries, not C# properties
        // Always use GetMemberValueFromTarget for proper dictionary-style access
        if (target is BsonDocument)
        {
            return GetMemberValueFromTarget(expression.MemberName, target);
        }

        // Optimization: Try to use AOT adapter or EntityMetadata if target is T
        var type = target.GetType();
        if (type == typeof(T))
        {
            if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
            {
                return adapter.GetPropertyValue((T)target, expression.MemberName);
            }
            if (EntityMetadata<T>.TryGetProperty(expression.MemberName, out var property))
            {
                return property.GetValue(target);
            }
        }

        return GetMemberValueFromTarget(expression.MemberName, target);
    }

    private static object? GetMemberValue(MemberExpression expression, object entity)
    {
        object? target = entity;
        if (expression.Expression != null && expression.Expression.NodeType != ExpressionType.Parameter)
        {
            target = EvaluateValue(expression.Expression, entity);
        }

        if (target == null)
        {
            if (expression.MemberName == "HasValue") return false;
            if (expression.MemberName == "Value") return null;
            return null;
        }

        return GetMemberValueFromTarget(expression.MemberName, target);
    }

    private static object? GetMemberValueFromTarget(string memberName, object target)
    {
        // Check BsonDocument first - it implements IEnumerable but needs special handling for property access
        if (target is BsonDocument doc)
        {
            var camelName = CamelCaseNameCache.GetOrAdd(memberName, static name => BsonFieldName.ToCamelCase(name));

            if (doc.TryGetValue(camelName, out var val)) return val.RawValue;
            if (doc.TryGetValue(memberName, out val)) return val.RawValue;
            if (string.Equals(memberName, "Id", StringComparison.OrdinalIgnoreCase) &&
                doc.TryGetValue("_id", out val))
            {
                return val.RawValue;
            }

            return null;
        }

        // Check AotGrouping for Key and Count
        if (target is IGrouping<object, object> grouping)
        {
            if (memberName == "Key") return grouping.Key;
            // Count is handled via method call, but provide fallback
            if (memberName == "Count" && target is QueryPipeline.AotGrouping aotGroup) return aotGroup.Count;
        }

        // Check BsonArray Count before ICollection/IEnumerable Count
        if (target is BsonArray arr && memberName == "Count")
        {
            return arr.Count;
        }

        // Check BsonValue wrapper for arrays Count
        if (target is BsonValue bv && bv.IsArray && memberName == "Count")
        {
            return ((BsonArray)bv.RawValue!).Count;
        }

        // Standard type checks
        if (target is string str)
        {
            if (memberName == "Length") return str.Length;
        }
        else if (target is DateTime dt)
        {
            if (memberName == "Year") return dt.Year;
            if (memberName == "Month") return dt.Month;
            if (memberName == "Day") return dt.Day;
            if (memberName == "Hour") return dt.Hour;
            if (memberName == "Minute") return dt.Minute;
            if (memberName == "Second") return dt.Second;
            if (memberName == "Date") return dt.Date;
            if (memberName == "DayOfWeek") return (int)dt.DayOfWeek;
            return null;
        }
        else if (target is System.Collections.ICollection col)
        {
            if (memberName == "Count") return col.Count;
        }
        else if (target is System.Collections.IEnumerable en)
        {
            if (memberName == "Count")
            {
                int count = 0;
                foreach (var _ in en) count++;
                return count;
            }
        }

        var type = target.GetType();

        if (AotHelperRegistry.TryGetAdapter(type, out var adapter) && adapter is IAotEntityPropertyAccessor accessor)
        {
            return accessor.GetPropertyValueUntyped(target, memberName);
        }

        // Fallback to reflection for other properties
        var prop = GetPropertySafe(type, memberName);
        if (prop != null)
        {
            return prop.GetValue(target);
        }

        if (memberName == "HasValue") return true;
        if (memberName == "Value") return target;

        return null;
    }

    [UnconditionalSuppressMessage("TrimAnalysis", "IL2075", Justification = "Fallback reflection for non-AOT scenarios. AOT apps should use Source Generator.")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2070", Justification = "Fallback reflection for non-AOT scenarios.")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2080", Justification = "Fallback reflection cache for non-AOT scenarios. AOT apps should use Source Generator.")]
    private static PropertyInfo? GetPropertySafe(Type type, string name)
    {
        return PropertyCache.GetOrAdd((type, name), static key => key.Type.GetProperty(key.Name));
    }

}
