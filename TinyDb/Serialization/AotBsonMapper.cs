using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Globalization;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// AOT友好的BSON映射器，根据源生成器提供的辅助适配器进行序列化与反序列化。
/// </summary>
public static class AotBsonMapper
{
    // 简单的循环引用检测，使用ThreadLocal来跟踪当前正在序列化的对象
    [ThreadStatic]
    private static HashSet<object>? _serializingObjects;

    private const DynamicallyAccessedMemberTypes EntityMemberRequirements =
        DynamicallyAccessedMemberTypes.PublicConstructors |
        DynamicallyAccessedMemberTypes.PublicProperties |
        DynamicallyAccessedMemberTypes.PublicFields |
        DynamicallyAccessedMemberTypes.PublicMethods;

    private const DynamicallyAccessedMemberTypes TypeInspectionRequirements =
        EntityMemberRequirements | DynamicallyAccessedMemberTypes.Interfaces;

    private sealed class ReferenceEqualityComparer : IEqualityComparer<object>
    {
        public static readonly ReferenceEqualityComparer Instance = new();

        public new bool Equals(object? x, object? y) => ReferenceEquals(x, y);

        public int GetHashCode(object obj) => RuntimeHelpers.GetHashCode(obj);
    }

    /// <summary>
    /// 将实体转换为 BSON 文档。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">实体对象。</param>
    /// <returns>BSON 文档。</returns>
    public static BsonDocument ToDocument<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(T entity)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (entity is BsonDocument doc)
        {
            return doc;
        }

        // 循环引用检测
        _serializingObjects ??= new HashSet<object>(ReferenceEqualityComparer.Instance);
        
        if (!typeof(T).IsValueType && _serializingObjects.Contains(entity))
        {
            // 检测到循环引用，返回空文档（或只包含ID的文档）
            if (AotHelperRegistry.TryGetAdapter<T>(out var circularAdapter))
            {
                var id = circularAdapter.GetId(entity);
                if (id != null && !id.IsNull)
                {
                    return new BsonDocument().Set("_id", id);
                }
            }
            return new BsonDocument();
        }

        if (!typeof(T).IsValueType)
        {
            _serializingObjects.Add(entity);
        }

        try
        {
            if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
            {
                return adapter.ToDocument(entity);
            }

            throw new InvalidOperationException(
                $"Type '{typeof(T).FullName}' must have [Entity] attribute for AOT serialization. " +
                $"Add [Entity] attribute to the type to enable source generator support.");
        }
        finally
        {
            if (!typeof(T).IsValueType)
            {
                _serializingObjects.Remove(entity);
            }
        }
    }

    /// <summary>
    /// 将 BSON 文档转换为实体对象。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="document">BSON 文档。</param>
    /// <returns>实体对象。</returns>
    public static T FromDocument<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(BsonDocument document)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        // Special case: if T is BsonDocument, return the document directly
        if (typeof(T) == typeof(BsonDocument))
        {
            return (T)(object)document;
        }

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.FromDocument(document);
        }

        throw new InvalidOperationException(
            $"Type '{typeof(T).FullName}' must have [Entity] attribute for AOT serialization. " +
            $"Add [Entity] attribute to the type to enable source generator support.");
    }

    /// <summary>
    /// 获取实体的 ID 值。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">实体对象。</param>
    /// <returns>BSON 格式的 ID。</returns>
    public static BsonValue GetId<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(T entity)
        where T : class, new()
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // Special case: if entity is BsonDocument, get _id directly
        if (entity is BsonDocument doc)
        {
            return doc.ContainsKey("_id") ? doc["_id"] : BsonNull.Value;
        }

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.GetId(entity);
        }

        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty == null)
        {
            return BsonNull.Value;
        }

        var value = idProperty.GetValue(entity);
        return value != null ? BsonConversion.ToBsonValue(value) : BsonNull.Value;
    }

    /// <summary>
    /// 设置实体的 ID 值。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">实体对象。</param>
    /// <param name="id">新的 ID 值。</param>
    public static void SetId<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(T entity, BsonValue id)
        where T : class, new()
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // Special case: if entity is BsonDocument, set _id directly
        // Note: BsonDocument.Set returns a new instance, so this only works for mutable operations
        // For BsonDocument, the caller should use doc.Set("_id", id) instead
        if (entity is BsonDocument)
        {
            // BsonDocument is immutable in terms of Set returning new instance
            // The typical pattern is: doc = doc.Set("_id", id)
            // We cannot modify the reference here, so we skip this case
            return;
        }

        AotIdAccessor<T>.SetId(entity, id);
    }

    /// <summary>
    /// 获取实体的属性值。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">实体对象。</param>
    /// <param name="propertyName">属性名称。</param>
    /// <returns>属性值。</returns>
    public static object? GetPropertyValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity, string propertyName)
        where T : class, new()
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));
        if (string.IsNullOrWhiteSpace(propertyName)) throw new ArgumentNullException(nameof(propertyName));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.GetPropertyValue(entity, propertyName);
        }

        return EntityMetadata<T>.TryGetProperty(propertyName, out var property)
            ? property.GetValue(entity)
            : null;
    }

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

            return Activator.CreateInstance(targetType);
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
    /// 将字符串转换为驼峰命名法。
    /// </summary>
    /// <param name="name">原始字符串。</param>
    /// <returns>驼峰命名字符串。</returns>
    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();
        return char.ToLowerInvariant(name[0]) + name.Substring(1);
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

    /// <summary>
    /// 判断是否为集合类型
    /// </summary>
    private static bool IsCollectionType(Type type) =>
        type != null &&
        type != typeof(string) &&
        type != typeof(byte[]) &&
        typeof(IEnumerable).IsAssignableFrom(type) &&
        !IsDictionaryType(type);

    /// <summary>
    /// 将集合转换为BsonArray
    /// </summary>
    private static BsonArray ConvertCollectionToBsonArray(object collection)
    {
        if (collection == null) throw new ArgumentNullException(nameof(collection));

        if (collection is not IEnumerable enumerable)
        {
            throw new ArgumentException("集合类型必须实现 IEnumerable 接口。", nameof(collection));
        }

        BsonArray bsonArray = new BsonArray();

        foreach (var item in enumerable)
        {
            if (item == null)
            {
                bsonArray = bsonArray.AddValue(BsonNull.Value);
                continue;
            }

            var itemType = item.GetType();
            var bsonValue = ConvertToBsonValue(item);
            bsonArray = bsonArray.AddValue(bsonValue);
        }

        return bsonArray;
    }

    /// <summary>
    /// 判断是否为Dictionary类型
    /// </summary>
    private static bool IsDictionaryType(Type type) => type != null && typeof(IDictionary).IsAssignableFrom(type);

    /// <summary>
    /// 将Dictionary转换为BsonDocument
    /// </summary>
    private static BsonDocument ConvertDictionaryToBsonDocument(object dictionary)
    {
        if (dictionary == null) throw new ArgumentNullException(nameof(dictionary));

        if (dictionary is not IDictionary rawDictionary)
        {
            throw new ArgumentException($"对象 {dictionary.GetType().FullName} 未实现 IDictionary 接口，无法在AOT回退模式下进行序列化。", nameof(dictionary));
        }

        var bsonDocument = new BsonDocument();

        foreach (DictionaryEntry entry in rawDictionary)
        {
            if (entry.Key is not string key)
            {
                throw new NotSupportedException("AOT 回退仅支持字符串键的字典。");
            }

            var bsonValue = entry.Value != null
                ? ConvertToBsonValue(entry.Value)
                : BsonNull.Value;
            bsonDocument = bsonDocument.Set(key, bsonValue);
        }

        return bsonDocument;
    }

    private static object? ConvertDictionary([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type dictionaryType, BsonDocument document)
    {
        if (document == null) return null;

        if (dictionaryType.IsInterface || dictionaryType.IsAbstract)
        {
            throw new NotSupportedException($"AOT 回退模式不支持接口/抽象字典类型 {dictionaryType.FullName}，请使用具体的 Dictionary<TKey, TValue>。");
        }

        if (!dictionaryType.IsGenericType || dictionaryType.GetGenericArguments().Length != 2)
        {
            throw new NotSupportedException($"字典类型 {dictionaryType.FullName} 不是有效的泛型字典类型。");
        }

        var args = dictionaryType.GetGenericArguments();
        var keyType = args[0];
        var valueType = args[1];

        if (keyType != typeof(string))
        {
            throw new NotSupportedException($"AOT 回退模式仅支持字符串键的字典，但实际键类型为 {keyType.FullName}。");
        }

        var instance = Activator.CreateInstance(dictionaryType)!;

        if (instance is not IDictionary dictionary)
        {
            throw new NotSupportedException($"字典类型 {dictionaryType.FullName} 未实现 IDictionary 接口，无法在 AOT 回退模式下填充数据。");
        }

        if (valueType == typeof(int))
        {
            foreach (var element in document)
            {
                dictionary[element.Key] = ConvertFromBsonValue(element.Value, typeof(int));
            }

            return instance;
        }

        if (valueType == typeof(string))
        {
            foreach (var element in document)
            {
                dictionary[element.Key] = ConvertFromBsonValue(element.Value, typeof(string));
            }

            return instance;
        }

        if (valueType == typeof(object))
        {
            foreach (var element in document)
            {
                dictionary[element.Key] = ConvertFromBsonValue(element.Value, typeof(object));
            }

            return instance;
        }

        throw new NotSupportedException($"AOT 回退模式不支持字典值类型 '{valueType.FullName}'。");
    }

    private static object? TryWrapWithTargetCollection([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type targetCollectionType, object sourceCollection)
    {
        if (targetCollectionType == null) throw new ArgumentNullException(nameof(targetCollectionType));
        if (sourceCollection == null) throw new ArgumentNullException(nameof(sourceCollection));

        var sourceType = sourceCollection.GetType();

        foreach (var ctor in targetCollectionType.GetConstructors(BindingFlags.Public | BindingFlags.Instance))
        {
            var parameters = ctor.GetParameters();
            if (parameters.Length != 1)
            {
                continue;
            }

            if (parameters[0].ParameterType == sourceType)
            {
                return ctor.Invoke(new[] { sourceCollection });
            }
        }

        return null;
    }

    private static object? TryCreateCollectionFromListCtor([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type collectionType, BsonArray array)
    {
        if (collectionType == null) throw new ArgumentNullException(nameof(collectionType));
        if (array == null) return null;

        Type? elementType = null;

        if (collectionType.IsGenericType && collectionType.GetGenericArguments().Length == 1)
        {
            elementType = collectionType.GetGenericArguments()[0];
        }
        else
        {
            var enumerableInterface = collectionType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            if (enumerableInterface != null)
            {
                elementType = enumerableInterface.GetGenericArguments()[0];
            }
        }

        if (elementType == null)
        {
            return null;
        }

        if (elementType == typeof(int))
        {
            var list = new List<int>(array.Count);
            foreach (var bsonValue in array)
            {
                list.Add((int)ConvertFromBsonValue(bsonValue, typeof(int))!);
            }

            return TryWrapWithTargetCollection(collectionType, list);
        }

        if (elementType == typeof(string))
        {
            var list = new List<string?>(array.Count);
            foreach (var bsonValue in array)
            {
                list.Add((string?)ConvertFromBsonValue(bsonValue, typeof(string)));
            }

            return TryWrapWithTargetCollection(collectionType, list);
        }

        if (elementType == typeof(object))
        {
            var list = new List<object?>(array.Count);
            foreach (var bsonValue in array)
            {
                list.Add(ConvertFromBsonValue(bsonValue, typeof(object)));
            }

            return TryWrapWithTargetCollection(collectionType, list);
        }

        return null;
    }

    private static object? ConvertCollection([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type collectionType, BsonArray array)
    {
        if (array == null) return null;

        if (collectionType.IsArray)
        {
            var arrayElementType = collectionType.GetElementType()!;

            if (arrayElementType == typeof(int))
            {
                var values = new int[array.Count];
                for (int i = 0; i < array.Count; i++)
                {
                    values[i] = (int)ConvertFromBsonValue(array[i], typeof(int))!;
                }
                return values;
            }

            if (arrayElementType == typeof(string))
            {
                var values = new string[array.Count];
                for (int i = 0; i < array.Count; i++)
                {
                    values[i] = (string)ConvertFromBsonValue(array[i], typeof(string))!;
                }
                return values;
            }

            throw new NotSupportedException($"AOT fallback does not support array element type '{arrayElementType.FullName}'.");
        }

        if (collectionType.IsInterface || collectionType.IsAbstract)
        {
            throw new NotSupportedException($"AOT 回退模式不支持接口/抽象集合类型 {collectionType.FullName}，请使用具体的 List<T> 或 ArrayList。");
        }

        var wrapped = TryCreateCollectionFromListCtor(collectionType, array);
        if (wrapped != null)
        {
            return wrapped;
        }

        if (collectionType.GetConstructor(Type.EmptyTypes) == null)
        {
            throw new NotSupportedException(
                $"AOT fallback does not support collection type '{collectionType.FullName}' without a public parameterless constructor or List<T> constructor.");
        }

        var instance = Activator.CreateInstance(collectionType)!;

        if (instance is not IList list)
        {
            throw new NotSupportedException($"AOT 回退模式仅支持实现 IList 的集合类型，但 {collectionType.FullName} 不支持。");
        }

        var elementType = collectionType.IsGenericType && collectionType.GetGenericArguments().Length == 1
            ? collectionType.GetGenericArguments()[0]
            : typeof(object);

        if (elementType == typeof(int))
        {
            foreach (var bsonValue in array)
            {
                list.Add(ConvertFromBsonValue(bsonValue, typeof(int)));
            }

            return instance;
        }

        if (elementType == typeof(string))
        {
            foreach (var bsonValue in array)
            {
                list.Add(ConvertFromBsonValue(bsonValue, typeof(string)));
            }

            return instance;
        }

        if (elementType == typeof(object))
        {
            foreach (var bsonValue in array)
            {
                list.Add(ConvertFromBsonValue(bsonValue, typeof(object)));
            }

            return instance;
        }

        throw new NotSupportedException($"AOT 回退模式不支持集合元素类型 '{elementType.FullName}'。");
    }


    private static object ConvertPrimitiveValue(BsonValue bsonValue, Type targetType)
    {
        if (targetType == typeof(BsonValue) || targetType.IsAssignableFrom(bsonValue.GetType()))
        {
            return bsonValue;
        }

        return targetType switch
        {
            var t when t == typeof(string) => bsonValue.ToString(),
            var t when t == typeof(bool) => bsonValue switch
            {
                BsonBoolean b => b.Value,
                BsonString s => bool.Parse(s.Value),
                BsonInt32 i => Convert.ToBoolean(i.Value),
                BsonInt64 l => Convert.ToBoolean(l.Value),
                BsonDouble d => Convert.ToBoolean(d.Value),
                BsonDecimal128 dec => Convert.ToBoolean(dec.Value),
                _ => Convert.ToBoolean(bsonValue.ToString())
            },
            var t when t == typeof(int) => bsonValue switch
            {
                BsonInt32 i32 => i32.Value,
                BsonInt64 i64 => checked((int)i64.Value),
                BsonDouble dbl => Convert.ToInt32(dbl.Value),
                BsonDecimal128 dec => checked((int)dec.Value.ToDecimal()),
                BsonString s => int.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToInt32(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(long) => bsonValue switch
            {
                BsonInt64 i64 => i64.Value,
                BsonInt32 i32 => i32.Value,
                BsonDouble dbl => Convert.ToInt64(dbl.Value),
                BsonDecimal128 dec => checked((long)dec.Value.ToDecimal()),
                BsonString s => long.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToInt64(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(byte) => bsonValue switch
            {
                BsonInt32 i32 => checked((byte)i32.Value),
                BsonInt64 i64 => checked((byte)i64.Value),
                BsonDouble dbl => checked((byte)dbl.Value),
                BsonDecimal128 dec => checked((byte)dec.Value.ToDecimal()),
                BsonString s => byte.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToByte(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(sbyte) => bsonValue switch
            {
                BsonInt32 i32 => checked((sbyte)i32.Value),
                BsonInt64 i64 => checked((sbyte)i64.Value),
                BsonDouble dbl => checked((sbyte)dbl.Value),
                BsonDecimal128 dec => checked((sbyte)dec.Value.ToDecimal()),
                BsonString s => sbyte.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToSByte(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(uint) => bsonValue switch
            {
                BsonInt32 i32 => checked((uint)i32.Value),
                BsonInt64 i64 => checked((uint)i64.Value),
                BsonDouble dbl => checked((uint)dbl.Value),
                BsonDecimal128 dec => checked((uint)dec.Value.ToDecimal()),
                BsonString s => uint.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToUInt32(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(ulong) => bsonValue switch
            {
                BsonInt64 i64 => checked((ulong)i64.Value),
                BsonInt32 i32 => checked((ulong)i32.Value),
                BsonDouble dbl => checked((ulong)dbl.Value),
                BsonDecimal128 dec => checked((ulong)dec.Value.ToDecimal()),
                BsonString s => ulong.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToUInt64(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(ushort) => bsonValue switch
            {
                BsonInt32 i32 => checked((ushort)i32.Value),
                BsonInt64 i64 => checked((ushort)i64.Value),
                BsonDouble dbl => checked((ushort)dbl.Value),
                BsonDecimal128 dec => checked((ushort)dec.Value.ToDecimal()),
                BsonString s => ushort.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToUInt16(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(double) => bsonValue switch
            {
                BsonDouble dbl => dbl.Value,
                BsonInt64 i64 => i64.Value,
                BsonInt32 i32 => i32.Value,
                BsonDecimal128 dec => (double)dec.Value.ToDecimal(),
                BsonString s => double.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToDouble(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(float) =>
                Convert.ToSingle(ConvertPrimitiveValue(bsonValue, typeof(double)), CultureInfo.InvariantCulture),
            var t when t == typeof(decimal) => bsonValue switch
            {
                BsonDecimal128 dec => dec.Value.ToDecimal(),
                BsonDouble dbl => Convert.ToDecimal(dbl.Value, CultureInfo.InvariantCulture),
                BsonInt32 i32 => (decimal)i32.Value,
                BsonInt64 i64 => (decimal)i64.Value,
                BsonString s => decimal.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToDecimal(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(short) => bsonValue switch
            {
                BsonInt32 i32 => checked((short)i32.Value),
                BsonInt64 i64 => checked((short)i64.Value),
                BsonDouble dbl => checked((short)dbl.Value),
                BsonDecimal128 dec => checked((short)dec.Value.ToDecimal()),
                BsonString s => short.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToInt16(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(DateTime) => bsonValue switch
            {
                BsonDateTime date => date.Value,
                BsonString s => DateTime.Parse(s.Value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
                _ => DateTime.Parse(bsonValue.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
            },
            var t when t == typeof(Guid) => bsonValue switch
            {
                BsonBinary binary when binary.SubType is BsonBinary.BinarySubType.Uuid or BsonBinary.BinarySubType.UuidLegacy => new Guid(binary.Bytes),
                BsonString s => Guid.Parse(s.Value),
                _ => Guid.Parse(bsonValue.ToString())
            },
            var t when t == typeof(ObjectId) => bsonValue switch
            {
                BsonObjectId objectId => objectId.Value,
                BsonString s => ObjectId.Parse(s.Value),
                _ => ObjectId.Parse(bsonValue.ToString())
            },
            var t when t.IsEnum => bsonValue switch
            {
                BsonInt32 i32 => Enum.ToObject(t, i32.Value),
                BsonInt64 i64 => Enum.ToObject(t, i64.Value),
                BsonDouble dbl => Enum.ToObject(t, Convert.ToInt64(dbl.Value)),
                BsonDecimal128 dec => Enum.ToObject(t, Convert.ToInt64(dec.Value.ToDecimal())),
                BsonString s when long.TryParse(s.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) =>
                    Enum.ToObject(t, parsed),
                BsonString s =>
                    throw new InvalidOperationException($"Cannot convert '{s.Value}' to enum '{t.FullName}' in AOT mode. Use AotBsonMapper.ConvertEnumValue<TEnum>() for name-based parsing."),
                _ => throw new InvalidOperationException($"Cannot convert {bsonValue.GetType().Name} to enum '{t.FullName}' in AOT mode.")
            },
            var t when t == typeof(byte[]) => bsonValue switch
            {
                BsonBinary binary => binary.Bytes,
                BsonString s => Convert.FromBase64String(s.Value),
                _ => throw new InvalidOperationException($"Cannot convert {bsonValue.GetType().Name} to byte[].")
            },
            _ => throw new NotSupportedException($"Unsupported conversion from BSON type '{bsonValue.GetType().Name}' to '{targetType.FullName}'. 为此类型注册源生成器适配器以获得完整支持。")
        };
    }
}
