using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// 提供在BSON值与CLR值之间转换的基础设施，避免运行时反射构造动态代码。
/// </summary>
public static class BsonConversion
{
    /// <summary>
    /// 将对象转换为 BsonValue。
    /// </summary>
    /// <param name="value">要转换的对象。</param>
    /// <returns>转换后的 BsonValue。</returns>
    public static BsonValue ToBsonValue(object value)
    {
        return value switch
        {
            null => BsonNull.Value,
            BsonValue bson => bson,
            byte[] bytes => new BsonBinary(bytes),
            ReadOnlyMemory<byte> rom => new BsonBinary(rom.ToArray()),
            Memory<byte> mem => new BsonBinary(mem.ToArray()),
            byte b => new BsonInt32(b),
            sbyte sb => new BsonInt32(sb),
            short s => new BsonInt32(s),
            ushort us => new BsonInt32(us),
            uint ui => new BsonInt64(ui),
            ulong ul => new BsonInt64(unchecked((long)ul)),
            string str => new BsonString(str),
            int i => new BsonInt32(i),
            long l => new BsonInt64(l),
            double d => new BsonDouble(d),
            float f => new BsonDouble(f),
            decimal dec => new BsonDecimal128(dec),
            bool b => new BsonBoolean(b),
            DateTime dt => new BsonDateTime(dt),
            Guid guid => new BsonString(guid.ToString()),
            ObjectId oid => new BsonObjectId(oid),
            Enum enumValue => ConvertEnumToBsonValue(enumValue),
            System.Collections.IDictionary dict => ConvertDictionaryToBsonDocument(dict),
            System.Collections.IEnumerable enumerable => ConvertEnumerableToBsonArray(enumerable),
            _ => ConvertComplexObjectToBsonValue(value)
        };
    }

    /// <summary>
    /// 将枚举转换为BsonValue
    /// </summary>
    private static BsonValue ConvertEnumToBsonValue(Enum enumValue)
    {
        // 将枚举转换为其基础类型（通常是int32）或字符串表示
        var underlyingType = Enum.GetUnderlyingType(enumValue.GetType());
        var convertedValue = Convert.ChangeType(enumValue, underlyingType);
        return ToBsonValue(convertedValue!);
    }

    /// <summary>
    /// 将复杂对象转换为BsonValue（通过AotBsonMapper）
    /// </summary>
    private static BsonValue ConvertComplexObjectToBsonValue(object value)
    {
        var type = value.GetType();

        // 检查是否为复杂对象类型
        if (IsComplexObjectType(type))
        {
            // 首先尝试使用注册的AOT适配器（AOT兼容）
            if (AotHelperRegistry.TryGetUntypedAdapter(type, out var adapter))
            {
                return adapter.ToDocumentUntyped(value);
            }

            // 尝试使用AotBsonMapper序列化
            var doc = AotBsonMapper.ToDocument(value);
            if (doc != null)
            {
                return doc;
            }
        }

        // 回退到字符串表示
        return new BsonString(value.ToString() ?? string.Empty);
    }

    /// <summary>
    /// 将Dictionary转换为BsonDocument
    /// </summary>
    private static BsonDocument ConvertDictionaryToBsonDocument(System.Collections.IDictionary dictionary)
    {
        var document = new BsonDocument();
        foreach (System.Collections.DictionaryEntry entry in dictionary)
        {
            var key = entry.Key.ToString() ?? string.Empty;
            var value = ToBsonValue(entry.Value!);
            document = document.Set(key, value);
        }
        return document;
    }

    /// <summary>
    /// 将Enumerable转换为BsonArray
    /// </summary>
    private static BsonArray ConvertEnumerableToBsonArray(System.Collections.IEnumerable enumerable)
    {
        var list = new List<BsonValue>();
        foreach (var item in enumerable)
        {
            list.Add(ToBsonValue(item!));
        }
        return new BsonArray(list);
    }

    /// <summary>
    /// 将值转换为BsonValue（用于源代码生成器）
    /// </summary>
    public static BsonValue ConvertToBsonValue(object value)
    {
        return ToBsonValue(value);
    }

    /// <summary>
    /// 将 BsonValue 转换为目标类型的对象。
    /// </summary>
    /// <param name="bsonValue">要转换的 BsonValue。</param>
    /// <param name="targetType">目标类型。</param>
    /// <returns>转换后的对象。</returns>
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2062", Justification = "Source generator在AOT模式下会生成确切的目标类型以保留所需成员。")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2070", Justification = "Source generator在AOT模式下会生成确切的目标类型以保留所需成员。")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2067", Justification = "Source generator在AOT模式下会生成确切的目标类型以保留所需成员。")]
    public static object? FromBsonValue(BsonValue bsonValue, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType)
    {
        if (bsonValue == null) throw new ArgumentNullException(nameof(bsonValue));
        if (targetType == null) throw new ArgumentNullException(nameof(targetType));

        if (bsonValue.IsNull) return null;

        targetType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // 处理枚举类型
        if (targetType.IsEnum)
        {
            return ConvertFromBsonValueToEnum(bsonValue, targetType);
        }

        // 处理集合类型
        if (targetType.IsGenericType)
        {
            var genericTypeDefinition = targetType.GetGenericTypeDefinition();

            // 处理 List<T>
            if (genericTypeDefinition == typeof(List<>))
            {
                if (bsonValue is BsonArray array)
                {
                    var elementType = targetType.GetGenericArguments()[0];
                    var list = Activator.CreateInstance(targetType);

                    foreach (var item in array)
                    {
                        var convertedItem = FromBsonValue(item, elementType);
                        targetType.GetMethod("Add")?.Invoke(list, new[] { convertedItem });
                    }

                    return list;
                }
            }

            // 处理 Dictionary<string, T>
            if (genericTypeDefinition == typeof(Dictionary<,>) &&
                targetType.GetGenericArguments()[0] == typeof(string))
            {
                if (bsonValue is BsonDocument document)
                {
                    var valueType = targetType.GetGenericArguments()[1];
                    var dictionary = Activator.CreateInstance(targetType);

                    foreach (var kvp in document)
                    {
                        var convertedValue = FromBsonValue(kvp.Value, valueType);
                        targetType.GetMethod("Add")?.Invoke(dictionary, new[] { kvp.Key, convertedValue });
                    }

                    return dictionary;
                }
            }
        }

        // 处理枚举类型（在switch之前特殊处理，因为枚举也是值类型）
        if (targetType.IsEnum)
        {
            return ConvertFromBsonValueToEnum(bsonValue, targetType);
        }

        // 处理嵌套的复杂类型（BsonDocument/BsonDocumentValue -> 自定义类/结构体）
        // 注意：BsonDocument 嵌套时会被包装为 BsonDocumentValue
        if (IsComplexObjectType(targetType))
        {
            BsonDocument? nestedDoc = null;
            if (bsonValue is BsonDocument doc)
            {
                nestedDoc = doc;
            }
            else if (bsonValue.IsDocument && bsonValue.RawValue is BsonDocument rawDoc)
            {
                nestedDoc = rawDoc;
            }

            if (nestedDoc != null)
            {
                // 首先尝试使用注册的AOT适配器（AOT兼容）
                if (AotHelperRegistry.TryGetUntypedAdapter(targetType, out var adapter))
                {
                    return adapter.FromDocumentUntyped(nestedDoc);
                }

                // 回退到AotBsonMapper.ConvertValue (使用反射)
                return AotBsonMapper.ConvertValue(nestedDoc, targetType);
            }
        }

        return targetType switch
        {
            var t when t == typeof(string) =>
                bsonValue is BsonString bsonString ? bsonString.Value : bsonValue.ToString(),
            var t when t == typeof(int) =>
                bsonValue switch
                {
                    BsonInt32 i32 => i32.Value,
                    BsonInt64 i64 => checked((int)i64.Value),
                    BsonDouble dbl => Convert.ToInt32(dbl.Value),
                    _ => Convert.ToInt32(bsonValue.ToString())
                },
            var t when t == typeof(long) =>
                bsonValue switch
                {
                    BsonInt64 i64 => i64.Value,
                    BsonInt32 i32 => i32.Value,
                    BsonDouble dbl => Convert.ToInt64(dbl.Value),
                    _ => Convert.ToInt64(bsonValue.ToString())
                },
            var t when t == typeof(double) =>
                bsonValue is BsonDouble dbl ? dbl.Value : Convert.ToDouble(bsonValue.ToString()),
            var t when t == typeof(float) =>
                bsonValue is BsonDouble dbl ? (float)dbl.Value : Convert.ToSingle(bsonValue.ToString()),
            var t when t == typeof(decimal) =>
                bsonValue switch
                {
                    BsonDecimal128 dec128 => dec128.Value.ToDecimal(),
                    BsonDouble dbl => Convert.ToDecimal(dbl.Value),
                    BsonInt32 i32 => Convert.ToDecimal(i32.Value),
                    BsonInt64 i64 => Convert.ToDecimal(i64.Value),
                    _ => Convert.ToDecimal(bsonValue.ToString())
                },
            var t when t == typeof(bool) =>
                bsonValue is BsonBoolean bl ? bl.Value : Convert.ToBoolean(bsonValue.ToString()),
            var t when t == typeof(DateTime) =>
                bsonValue is BsonDateTime dt ? dt.Value : Convert.ToDateTime(bsonValue.ToString()),
            var t when t == typeof(Guid) =>
                bsonValue is BsonString bsonString
                    ? Guid.Parse(bsonString.Value)
                    : bsonValue is BsonBinary bin && (bin.Bytes.Length == 16 || bin.SubType == BsonBinary.BinarySubType.Uuid || bin.SubType == BsonBinary.BinarySubType.UuidLegacy)
                        ? new Guid(bin.Bytes)
                        : Guid.Parse(bsonValue.ToString()),
            var t when t == typeof(char) =>
                bsonValue switch
                {
                    BsonInt32 i32 => checked((char)i32.Value),
                    BsonString str when str.Value.Length > 0 => str.Value[0],
                    _ => Convert.ToChar(bsonValue.ToString(), CultureInfo.InvariantCulture)
                },
            var t when t == typeof(ObjectId) =>
                bsonValue is BsonObjectId oid ? oid.Value : ObjectId.Parse(bsonValue.ToString()),
            var t when t == typeof(byte) =>
                bsonValue switch
                {
                    BsonInt32 i32 => checked((byte)i32.Value),
                    BsonInt64 i64 => checked((byte)i64.Value),
                    BsonDouble dbl => checked((byte)dbl.Value),
                    BsonString str => byte.Parse(str.Value, CultureInfo.InvariantCulture),
                    _ => Convert.ToByte(bsonValue.ToString(), CultureInfo.InvariantCulture)
                },
            var t when t == typeof(sbyte) =>
                bsonValue switch
                {
                    BsonInt32 i32 => checked((sbyte)i32.Value),
                    BsonInt64 i64 => checked((sbyte)i64.Value),
                    BsonDouble dbl => checked((sbyte)dbl.Value),
                    BsonString str => sbyte.Parse(str.Value, CultureInfo.InvariantCulture),
                    _ => Convert.ToSByte(bsonValue.ToString(), CultureInfo.InvariantCulture)
                },
            var t when t == typeof(short) =>
                bsonValue switch
                {
                    BsonInt32 i32 => checked((short)i32.Value),
                    BsonInt64 i64 => checked((short)i64.Value),
                    BsonDouble dbl => checked((short)dbl.Value),
                    BsonString str => short.Parse(str.Value, CultureInfo.InvariantCulture),
                    _ => Convert.ToInt16(bsonValue.ToString(), CultureInfo.InvariantCulture)
                },
            var t when t == typeof(ushort) =>
                bsonValue switch
                {
                    BsonInt32 i32 => checked((ushort)i32.Value),
                    BsonInt64 i64 => checked((ushort)i64.Value),
                    BsonDouble dbl => checked((ushort)dbl.Value),
                    BsonString str => ushort.Parse(str.Value, CultureInfo.InvariantCulture),
                    _ => Convert.ToUInt16(bsonValue.ToString(), CultureInfo.InvariantCulture)
                },
            var t when t == typeof(uint) =>
                bsonValue switch
                {
                    BsonInt64 i64 => checked((uint)i64.Value),
                    BsonInt32 i32 => checked((uint)i32.Value),
                    BsonDouble dbl => checked((uint)dbl.Value),
                    BsonString str => uint.Parse(str.Value, CultureInfo.InvariantCulture),
                    _ => Convert.ToUInt32(bsonValue.ToString(), CultureInfo.InvariantCulture)
                },
            var t when t == typeof(ulong) =>
                bsonValue switch
                {
                    BsonInt64 i64 => unchecked((ulong)i64.Value),
                    BsonInt32 i32 => unchecked((ulong)i32.Value),
                    BsonDouble dbl => checked((ulong)dbl.Value),
                    BsonString str => ulong.Parse(str.Value, CultureInfo.InvariantCulture),
                    _ => Convert.ToUInt64(bsonValue.ToString(), CultureInfo.InvariantCulture)
                },
            var t when t == typeof(byte[]) =>
                bsonValue switch
                {
                    BsonBinary bin => bin.Bytes.ToArray(),
                    BsonString str => Convert.FromBase64String(str.Value),
                    _ => throw new InvalidOperationException($"Cannot convert '{bsonValue.GetType().Name}' to byte[].")
                },
            var t when t == typeof(ReadOnlyMemory<byte>) =>
                bsonValue switch
                {
                    BsonBinary bin => new ReadOnlyMemory<byte>(bin.Bytes.ToArray()),
                    BsonString str => new ReadOnlyMemory<byte>(Convert.FromBase64String(str.Value)),
                    _ => throw new InvalidOperationException($"Cannot convert '{bsonValue.GetType().Name}' to ReadOnlyMemory<byte>.")
                },
            var t when t == typeof(Memory<byte>) =>
                bsonValue switch
                {
                    BsonBinary bin => new Memory<byte>(bin.Bytes.ToArray()),
                    BsonString str => new Memory<byte>(Convert.FromBase64String(str.Value)),
                    _ => throw new InvalidOperationException($"Cannot convert '{bsonValue.GetType().Name}' to Memory<byte>.")
                },
            _ => bsonValue.ToString()
        };
    }

    /// <summary>
    /// 将BsonValue转换为枚举
    /// </summary>
    private static object ConvertFromBsonValueToEnum(BsonValue bsonValue, Type enumType)
    {
        try
        {
            var underlyingType = Enum.GetUnderlyingType(enumType);
            object? convertedValue;

            // 先转换为底层类型
            if (underlyingType == typeof(int))
            {
                convertedValue = bsonValue switch
                {
                    BsonInt32 i32 => i32.Value,
                    BsonInt64 i64 => checked((int)i64.Value),
                    BsonDouble dbl => Convert.ToInt32(dbl.Value),
                    BsonString str => int.Parse(str.Value),
                    _ => Convert.ToInt32(bsonValue.ToString())
                };
            }
            else if (underlyingType == typeof(long))
            {
                convertedValue = bsonValue switch
                {
                    BsonInt64 i64 => i64.Value,
                    BsonInt32 i32 => i32.Value,
                    BsonDouble dbl => Convert.ToInt64(dbl.Value),
                    BsonString str => long.Parse(str.Value),
                    _ => Convert.ToInt64(bsonValue.ToString())
                };
            }
            else if (underlyingType == typeof(short))
            {
                convertedValue = bsonValue switch
                {
                    BsonInt32 i32 => (short)i32.Value,
                    BsonInt64 i64 => checked((short)i64.Value),
                    BsonDouble dbl => Convert.ToInt16(dbl.Value),
                    BsonString str => short.Parse(str.Value),
                    _ => Convert.ToInt16(bsonValue.ToString())
                };
            }
            else if (underlyingType == typeof(byte))
            {
                convertedValue = bsonValue switch
                {
                    BsonInt32 i32 => (byte)i32.Value,
                    BsonInt64 i64 => checked((byte)i64.Value),
                    BsonDouble dbl => Convert.ToByte(dbl.Value),
                    BsonString str => byte.Parse(str.Value),
                    _ => Convert.ToByte(bsonValue.ToString())
                };
            }
            else
            {
                // 默认处理为字符串
                convertedValue = bsonValue.ToString();
            }

            // 转换为枚举
            return Enum.ToObject(enumType, convertedValue!);
        }
        catch
        {
            // 如果数字转换失败，尝试字符串转换
            return Enum.Parse(enumType, bsonValue.ToString(), ignoreCase: true);
        }
    }

    /// <summary>
    /// 判断是否为复杂对象类型（类或结构体，排除基本类型、集合等）
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

        // 排除集合类型
        if (typeof(System.Collections.IEnumerable).IsAssignableFrom(type) && type != typeof(string))
        {
            return false;
        }

        // 排除 BsonValue 及其派生类型
        if (typeof(BsonValue).IsAssignableFrom(type))
        {
            return false;
        }

        // 处理复杂对象类型（class 和 struct）
        return type.IsClass || (type.IsValueType && !type.IsEnum && !type.IsPrimitive);
    }
}
