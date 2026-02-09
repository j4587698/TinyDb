using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Reflection;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.IdGeneration;

/// <summary>
/// ID生成辅助工具
/// </summary>
public static class IdGenerationHelper<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>
    where T : class, new()
{
    /// <summary>
    /// 检查实体是否需要生成ID
    /// </summary>
    /// <param name="entity">实体实例</param>
    /// <returns>是否需要生成ID</returns>
    public static bool ShouldGenerateId(T entity)
    {
        if (entity == null) return false;

        var idProperty = TinyDb.Serialization.EntityMetadata<T>.IdProperty;
        if (idProperty == null) return false;

        // 检查ID是否已有值
        var currentValue = idProperty.GetValue(entity);
        if (!IsEmptyValue(currentValue)) return false;

        // 检查是否有生成策略
        var generationAttribute = idProperty.GetCustomAttribute<IdGenerationAttribute>();
        return generationAttribute != null && generationAttribute.Strategy != IdGenerationStrategy.None;
    }

    /// <summary>
    /// 为实体生成ID
    /// </summary>
    /// <param name="entity">实体实例</param>
    /// <returns>是否成功生成ID</returns>
    public static bool GenerateIdForEntity(T entity)
    {
        if (entity == null) return false;

        var idProperty = TinyDb.Serialization.EntityMetadata<T>.IdProperty;
        if (idProperty == null) return false;

        var generationAttribute = idProperty.GetCustomAttribute<IdGenerationAttribute>();
        if (generationAttribute == null || generationAttribute.Strategy == IdGenerationStrategy.None)
        {
            return false;
        }

        try
        {
            var generator = IdGeneratorFactory.GetGenerator(generationAttribute.Strategy);
            if (!generator.Supports(idProperty.PropertyType))
            {
                return false;
            }

            var newId = generator.GenerateId(typeof(T), idProperty, generationAttribute.SequenceName);
            var convertedValue = ConvertGeneratedId(newId, idProperty.PropertyType);
            idProperty.SetValue(entity, convertedValue);

            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// 检查值是否为空值
    /// </summary>
    /// <param name="value">要检查的值</param>
    /// <returns>是否为空值</returns>
    private static bool IsEmptyValue(object? value)
    {
        return value switch
        {
            null => true,
            ObjectId objectId => objectId == ObjectId.Empty,
            string str => string.IsNullOrWhiteSpace(str),
            Guid guid => guid == Guid.Empty,
            int i => i == 0,
            long l => l == 0,
            _ => false
        };
    }

    /// <summary>
    /// 获取实体的ID生成策略
    /// </summary>
    /// <returns>ID生成策略</returns>
    public static IdGenerationStrategy GetIdGenerationStrategy()
    {
        var idProperty = TinyDb.Serialization.EntityMetadata<T>.IdProperty;
        if (idProperty == null) return IdGenerationStrategy.None;

        var generationAttribute = idProperty.GetCustomAttribute<IdGenerationAttribute>();
        return generationAttribute?.Strategy ?? IdGenerationStrategy.None;
    }

    private static object? ConvertGeneratedId(BsonValue bsonValue, Type targetType)
    {
        if (targetType == null) throw new ArgumentNullException(nameof(targetType));

        var nonNullableType = Nullable.GetUnderlyingType(targetType) ?? targetType;
        var rawValue = bsonValue.RawValue;

        if (rawValue == null)
        {
            return null;
        }

        if (nonNullableType.IsInstanceOfType(rawValue))
        {
            return rawValue;
        }

        if (nonNullableType == typeof(string))
        {
            return rawValue.ToString();
        }

        if (nonNullableType == typeof(Guid))
        {
            return rawValue switch
            {
                byte[] bytes when bytes.Length == 16 => new Guid(bytes),
                string str => Guid.Parse(str),
                _ => Guid.Parse(rawValue.ToString() ?? string.Empty)
            };
        }

        if (nonNullableType == typeof(ObjectId))
        {
            return rawValue switch
            {
                string str => ObjectId.Parse(str),
                _ => ObjectId.Parse(rawValue.ToString() ?? string.Empty)
            };
        }

        if (nonNullableType.IsEnum)
        {
            var underlyingType = Enum.GetUnderlyingType(nonNullableType);
            if (rawValue is string enumText)
            {
                if (TryConvertEnumFromString(nonNullableType, enumText, out var enumValue))
                {
                    return enumValue;
                }

                throw new ArgumentException($"Invalid enum value '{rawValue}' for '{nonNullableType.FullName}'.");
            }

            try
            {
                var underlyingValue = Convert.ChangeType(rawValue, underlyingType, CultureInfo.InvariantCulture);
                return Enum.ToObject(nonNullableType, underlyingValue!);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw new ArgumentException($"Invalid enum value '{rawValue}' for '{nonNullableType.FullName}'.", ex);
            }
        }

        return Convert.ChangeType(rawValue, nonNullableType);
    }

    private static bool TryConvertEnumFromString(Type enumType, string enumText, out object enumValue)
    {
        enumValue = default!;

        if (string.IsNullOrWhiteSpace(enumText))
        {
            return false;
        }

        var underlyingType = Enum.GetUnderlyingType(enumType);
        var parts = enumText.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0)
        {
            return false;
        }

        if (parts.Length == 1)
        {
            if (!TryConvertEnumTokenToUnderlying(enumType, underlyingType, parts[0], out var underlying))
            {
                return false;
            }

            enumValue = Enum.ToObject(enumType, underlying);
            return true;
        }

        ulong combined = 0;
        foreach (var part in parts)
        {
            if (!TryConvertEnumTokenToUInt64(enumType, underlyingType, part, out var tokenValue))
            {
                return false;
            }

            combined |= tokenValue;
        }

        enumValue = Enum.ToObject(enumType, FromUInt64(combined, underlyingType));
        return true;
    }

    private static bool TryConvertEnumTokenToUInt64(Type enumType, Type underlyingType, string token, out ulong value)
    {
        value = 0;

        if (TryConvertEnumTokenToUnderlying(enumType, underlyingType, token, out var underlying))
        {
            value = ToUInt64(underlying, underlyingType);
            return true;
        }

        return false;
    }

    private static bool TryConvertEnumTokenToUnderlying(Type enumType, Type underlyingType, string token, out object underlyingValue)
    {
        underlyingValue = default!;

        try
        {
            underlyingValue = Convert.ChangeType(token, underlyingType, CultureInfo.InvariantCulture)!;
            return true;
        }
        catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
        {
            return TryLookupEnumUnderlyingValueByName(enumType, token, out underlyingValue);
        }
    }

    private static bool TryLookupEnumUnderlyingValueByName(Type enumType, string name, out object underlyingValue)
    {
        underlyingValue = default!;

        var names = Enum.GetNames(enumType);
        var values = Enum.GetValuesAsUnderlyingType(enumType);

        for (var i = 0; i < names.Length; i++)
        {
            if (!string.Equals(names[i], name, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            underlyingValue = values.GetValue(i)!;
            return true;
        }

        return false;
    }

    private static ulong ToUInt64(object underlyingValue, Type underlyingType)
    {
        return Type.GetTypeCode(underlyingType) switch
        {
            TypeCode.Byte => (byte)underlyingValue,
            TypeCode.SByte => unchecked((ulong)(sbyte)underlyingValue),
            TypeCode.Int16 => unchecked((ulong)(short)underlyingValue),
            TypeCode.UInt16 => (ushort)underlyingValue,
            TypeCode.Int32 => unchecked((ulong)(int)underlyingValue),
            TypeCode.UInt32 => (uint)underlyingValue,
            TypeCode.Int64 => unchecked((ulong)(long)underlyingValue),
            TypeCode.UInt64 => (ulong)underlyingValue,
            _ => throw new NotSupportedException($"Unsupported enum underlying type '{underlyingType.FullName}'.")
        };
    }

    private static object FromUInt64(ulong value, Type underlyingType)
    {
        return Type.GetTypeCode(underlyingType) switch
        {
            TypeCode.Byte => (byte)value,
            TypeCode.SByte => unchecked((sbyte)value),
            TypeCode.Int16 => unchecked((short)value),
            TypeCode.UInt16 => (ushort)value,
            TypeCode.Int32 => unchecked((int)value),
            TypeCode.UInt32 => (uint)value,
            TypeCode.Int64 => unchecked((long)value),
            TypeCode.UInt64 => value,
            _ => throw new NotSupportedException($"Unsupported enum underlying type '{underlyingType.FullName}'.")
        };
    }
}
