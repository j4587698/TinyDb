using System;
using System.Collections;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Bson;

namespace TinyDb.Serialization;

public static partial class AotBsonMapper
{
    /// <summary>
    /// 将 BSON 值转换为目标类型。
    /// </summary>
    /// <param name="bsonValue">BSON 值。</param>
    /// <param name="targetType">目标类型。</param>
    /// <returns>转换后的对象。</returns>
    public static object? ConvertValue(BsonValue bsonValue, [DynamicallyAccessedMembers(TypeInspectionRequirements)] Type targetType)
    {
        if (bsonValue == null) throw new ArgumentNullException(nameof(bsonValue));
        if (targetType == null) throw new ArgumentNullException(nameof(targetType));

        return ConvertFromBsonValue(bsonValue, targetType);
    }

    public static TEnum ConvertEnumValue<TEnum>(BsonValue bsonValue)
        where TEnum : struct, Enum
    {
        if (bsonValue == null) throw new ArgumentNullException(nameof(bsonValue));

        if (bsonValue.IsNull)
        {
            return default;
        }

        return bsonValue switch
        {
            BsonString s => Enum.Parse<TEnum>(s.Value, ignoreCase: true),
            BsonInt32 i32 => (TEnum)Enum.ToObject(typeof(TEnum), i32.Value),
            BsonInt64 i64 => (TEnum)Enum.ToObject(typeof(TEnum), i64.Value),
            BsonDouble dbl => (TEnum)Enum.ToObject(typeof(TEnum), Convert.ToInt64(dbl.Value)),
            BsonDecimal128 dec => (TEnum)Enum.ToObject(typeof(TEnum), Convert.ToInt64(dec.Value.ToDecimal())),
            _ => Enum.Parse<TEnum>(bsonValue.ToString(), ignoreCase: true)
        };
    }

    private static BsonValue ConvertToBsonValue(object? value)
    {
        if (value == null)
        {
            return BsonNull.Value;
        }

        if (value is BsonValue bsonValue)
        {
            return bsonValue;
        }

        var runtimeType = value.GetType();

        if (IsDictionaryType(runtimeType))
        {
            return ConvertDictionaryToBsonDocument(value);
        }

        if (IsCollectionType(runtimeType))
        {
            return ConvertCollectionToBsonArray((IEnumerable)value);
        }

        if (IsComplexObjectType(runtimeType))
        {
            if (AotHelperRegistry.TryGetUntypedAdapter(runtimeType, out var adapter))
            {
                return adapter.ToDocumentUntyped(value);
            }

            throw new InvalidOperationException(
                $"Type '{runtimeType.FullName}' must have [Entity] attribute for AOT serialization. " +
                $"Add [Entity] attribute to the type to enable source generator support.");
        }

        return BsonConversion.ToBsonValue(value);
    }

    private static object? ConvertFromBsonValue(BsonValue bsonValue, [DynamicallyAccessedMembers(TypeInspectionRequirements)] Type targetType)
    {
        if (targetType == null) throw new ArgumentNullException(nameof(targetType));

        if (bsonValue == null || bsonValue.IsNull)
        {
            if (!targetType.IsValueType || Nullable.GetUnderlyingType(targetType) != null)
            {
                return null;
            }

            return CreateDefaultValue(targetType);
        }

        var nonNullableType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (nonNullableType.ContainsGenericParameters)
        {
            throw new InvalidOperationException($"Open generic target type '{nonNullableType}' is not supported.");
        }

        // Special handling for object type - unwrap BsonValue to CLR type
        if (nonNullableType == typeof(object))
        {
            return UnwrapBsonValue(bsonValue);
        }

        if (IsDictionaryType(nonNullableType))
        {
            if (bsonValue is BsonDocument doc)
            {
                return ConvertDictionary(nonNullableType, doc);
            }
            throw new NotSupportedException($"无法将 BSON 类型 {bsonValue.BsonType} 反序列化为字典类型 {nonNullableType.FullName}");
        }

        if (IsCollectionType(nonNullableType))
        {
            if (bsonValue is BsonArray array)
            {
                return ConvertCollection(nonNullableType, array);
            }
            throw new NotSupportedException($"无法将 BSON 类型 {bsonValue.BsonType} 反序列化为集合类型 {nonNullableType.FullName}");
        }

        if (IsComplexObjectType(nonNullableType))
        {
            if (bsonValue is BsonDocument nestedDoc)
            {
                if (AotHelperRegistry.TryGetUntypedAdapter(nonNullableType, out var adapter))
                {
                    return adapter.FromDocumentUntyped(nestedDoc);
                }

                throw new InvalidOperationException(
                    $"Type '{nonNullableType.FullName}' must have [Entity] attribute for AOT serialization. " +
                    $"Add [Entity] attribute to the type to enable source generator support.");
            }
        }

        return ConvertPrimitiveValue(bsonValue, nonNullableType);
    }

    private static object CreateDefaultValue(Type targetType)
    {
        if (targetType.IsEnum)
        {
            return Enum.ToObject(targetType, 0);
        }

        return targetType switch
        {
            var t when t == typeof(bool) => false,
            var t when t == typeof(byte) => default(byte),
            var t when t == typeof(sbyte) => default(sbyte),
            var t when t == typeof(short) => default(short),
            var t when t == typeof(ushort) => default(ushort),
            var t when t == typeof(int) => default(int),
            var t when t == typeof(uint) => default(uint),
            var t when t == typeof(long) => default(long),
            var t when t == typeof(ulong) => default(ulong),
            var t when t == typeof(float) => default(float),
            var t when t == typeof(double) => default(double),
            var t when t == typeof(decimal) => default(decimal),
            var t when t == typeof(char) => default(char),
            var t when t == typeof(DateTime) => default(DateTime),
            var t when t == typeof(Guid) => default(Guid),
            var t when t == typeof(ObjectId) => default(ObjectId),
            var t when t == typeof(Decimal128) => default(Decimal128),
            _ => throw new NotSupportedException($"AOT fallback does not support default value creation for type '{targetType.FullName}'.")
        };
    }

    /// <summary>
    /// 解包 BsonValue 为原始 CLR 对象。
    /// </summary>
    /// <param name="value">BsonValue。</param>
    /// <returns>原始对象。</returns>
    private static object? UnwrapBsonValue(BsonValue value)
    {
        return value switch
        {
            BsonNull => null,
            BsonString s => s.Value,
            BsonInt32 i => i.Value,
            BsonInt64 l => l.Value,
            BsonDouble d => d.Value,
            BsonDecimal128 d => d.Value,
            BsonBoolean b => b.Value,
            BsonDateTime dt => dt.Value,
            BsonObjectId o => o.Value,
            BsonBinary b when b.SubType is BsonBinary.BinarySubType.Uuid or BsonBinary.BinarySubType.UuidLegacy => new Guid(b.Bytes),
            BsonBinary b => b.Bytes,
            _ => value
        };
    }

    /// <summary>
    /// 判断是否为复杂对象类型
    /// </summary>
    private static bool IsComplexObjectType(Type type)
    {
        // 排除基本类型、字符串、枚举和集合类型
        if (type.IsPrimitive ||
            type == typeof(string) ||
            type == typeof(byte[]) ||
            type == typeof(DateTime) ||
            type == typeof(Guid) ||
            type == typeof(ObjectId) ||
            type == typeof(decimal) ||
            type.IsEnum)
        {
            return false;
        }

        // 排除集合类型（List<T>, Dictionary<K,V>, 数组等）
        if (IsCollectionType(type) || IsDictionaryType(type))
        {
            return false;
        }

        // 处理复杂对象类型（class 和 struct）
        return type.IsClass || type.IsValueType;
    }

}
