using System;
using System.Collections.Generic;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// AOT 回退模式下集合元素/字典值的类型判定。
/// </summary>
internal static class AotFallbackTypes
{
    /// <summary>
    /// 判断类型（含可空值类型、嵌套集合）能否在 AOT 回退模式下逐元素转换。
    /// </summary>
    internal static bool IsSupportedElementType(Type type)
    {
        var targetType = Nullable.GetUnderlyingType(type) ?? type;

        if (targetType.IsArray)
        {
            var elementType = targetType.GetElementType();
            return targetType.GetArrayRank() == 1 &&
                   elementType != null &&
                   IsSupportedElementType(elementType);
        }

        if (targetType.IsGenericType)
        {
            var definition = targetType.GetGenericTypeDefinition();
            if (definition == typeof(Dictionary<,>))
            {
                var arguments = targetType.GetGenericArguments();
                return arguments[0] == typeof(string) && IsSupportedElementType(arguments[1]);
            }

            if (definition == typeof(List<>) || definition == typeof(HashSet<>))
            {
                return IsSupportedElementType(targetType.GetGenericArguments()[0]);
            }

            if (!targetType.IsInterface &&
                !targetType.IsAbstract &&
                targetType.GetGenericArguments().Length == 1 &&
                typeof(System.Collections.IEnumerable).IsAssignableFrom(targetType))
            {
                return IsSupportedElementType(targetType.GetGenericArguments()[0]);
            }
        }

        if (typeof(BsonValue).IsAssignableFrom(targetType))
        {
            return true;
        }

        return targetType.IsEnum ||
               targetType == typeof(string) ||
               targetType == typeof(bool) ||
               targetType == typeof(byte) ||
               targetType == typeof(sbyte) ||
               targetType == typeof(short) ||
               targetType == typeof(ushort) ||
               targetType == typeof(int) ||
               targetType == typeof(uint) ||
               targetType == typeof(long) ||
               targetType == typeof(ulong) ||
               targetType == typeof(float) ||
               targetType == typeof(double) ||
               targetType == typeof(decimal) ||
               targetType == typeof(char) ||
               targetType == typeof(DateTime) ||
               targetType == typeof(DateTimeOffset) ||
               targetType == typeof(TimeSpan) ||
               targetType == typeof(Guid) ||
               targetType == typeof(ObjectId) ||
               targetType == typeof(byte[]);
    }
}
