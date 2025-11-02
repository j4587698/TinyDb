using System.Collections;
using System.Collections.Concurrent;
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

    private sealed class ReferenceEqualityComparer : IEqualityComparer<object>
    {
        public static readonly ReferenceEqualityComparer Instance = new();

        public new bool Equals(object? x, object? y) => ReferenceEquals(x, y);

        public int GetHashCode(object obj) => RuntimeHelpers.GetHashCode(obj);
    }

    private sealed class ReflectionMetadata
    {
        public ReflectionMetadata(
            PropertyInfo? idProperty,
            PropertyInfo[] properties,
            Dictionary<string, PropertyInfo> propertyMap,
            Dictionary<string, PropertyInfo> camelCasePropertyMap,
            FieldInfo[] fields,
            Dictionary<string, FieldInfo> camelCaseFieldMap)
        {
            IdProperty = idProperty;
            Properties = properties;
            PropertyMap = propertyMap;
            CamelCasePropertyMap = camelCasePropertyMap;
            Fields = fields;
            CamelCaseFieldMap = camelCaseFieldMap;
        }

        public PropertyInfo? IdProperty { get; }

        public IReadOnlyList<PropertyInfo> Properties { get; }

        public IReadOnlyDictionary<string, PropertyInfo> PropertyMap { get; }

        public IReadOnlyDictionary<string, PropertyInfo> CamelCasePropertyMap { get; }

        public IReadOnlyList<FieldInfo> Fields { get; }

        public IReadOnlyDictionary<string, FieldInfo> CamelCaseFieldMap { get; }
    }

    private static readonly ConcurrentDictionary<Type, ReflectionMetadata> MetadataCache = new();

    [UnconditionalSuppressMessage("TrimAnalysis", "IL2111", Justification = "Metadata 缓存的访问由源生成器生成的注册代码触发，生成过程会确保所需成员被保留。")]
    private static ReflectionMetadata GetMetadata([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] Type type)
    {
        return MetadataCache.GetOrAdd(type, BuildMetadata);
    }

    private static ReflectionMetadata BuildMetadata([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] Type type)
    {
        var properties = type
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(property => property.CanRead && property.GetMethod is { IsStatic: false })
            .Where(property => property.GetCustomAttribute<BsonIgnoreAttribute>() == null)
            .ToArray();

        var propertyMap = properties.ToDictionary(property => property.Name, StringComparer.Ordinal);
        var camelCasePropertyMap = properties.ToDictionary(property => ToCamelCase(property.Name), StringComparer.Ordinal);

        var fields = type
            .GetFields(BindingFlags.Instance | BindingFlags.Public)
            .Where(field => !field.IsSpecialName && !field.IsLiteral)
            .ToArray();
        var camelCaseFieldMap = fields.ToDictionary(field => ToCamelCase(field.Name), StringComparer.Ordinal);

        var idProperty = ResolveIdProperty(type, propertyMap);

        return new ReflectionMetadata(idProperty, properties, propertyMap, camelCasePropertyMap, fields, camelCaseFieldMap);
    }

    private static PropertyInfo? ResolveIdProperty(Type type, IReadOnlyDictionary<string, PropertyInfo> propertyMap)
    {
        var entityAttribute = type.GetCustomAttribute<EntityAttribute>();
        if (!string.IsNullOrWhiteSpace(entityAttribute?.IdProperty) &&
            propertyMap.TryGetValue(entityAttribute.IdProperty!, out var specified))
        {
            return specified;
        }

        foreach (var property in propertyMap.Values)
        {
            if (property.GetCustomAttribute<IdAttribute>() != null)
            {
                return property;
            }
        }

        foreach (var candidate in new[] { "Id", "_id", "ID" })
        {
            if (propertyMap.TryGetValue(candidate, out var property))
            {
                return property;
            }
        }

        return null;
    }
    public static BsonDocument ToDocument<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(T entity)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.ToDocument(entity);
        }

        return FallbackToDocument(typeof(T), entity!);
    }

    public static T FromDocument<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(BsonDocument document)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.FromDocument(document);
        }

        return (T)FallbackFromDocument(typeof(T), document);
    }

    public static BsonValue GetId<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(T entity)
        where T : class, new()
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

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

    public static void SetId<[DynamicallyAccessedMembers(EntityMemberRequirements)] T>(T entity, BsonValue id)
        where T : class, new()
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        AotIdAccessor<T>.SetId(entity, id);
    }

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

    public static object? ConvertValue(BsonValue bsonValue, Type targetType)
    {
        if (bsonValue == null) throw new ArgumentNullException(nameof(bsonValue));
        if (targetType == null) throw new ArgumentNullException(nameof(targetType));

        return ConvertFromBsonValue(bsonValue, targetType);
    }

    private static BsonDocument FallbackToDocument([DynamicallyAccessedMembers(EntityMemberRequirements)] Type entityType, object entity)
    {
        if (entityType == null) throw new ArgumentNullException(nameof(entityType));
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        _serializingObjects ??= new HashSet<object>(ReferenceEqualityComparer.Instance);

        if (!entityType.IsValueType && _serializingObjects.Contains(entity))
        {
            var metadata = GetMetadata(entityType);
            var circularDoc = new BsonDocument();
            if (metadata.IdProperty != null)
            {
                var id = metadata.IdProperty.GetValue(entity);
                if (id != null)
                {
                    circularDoc = circularDoc.Set("_id", BsonConversion.ToBsonValue(id));
                }
            }
            return circularDoc;
        }

        if (!entityType.IsValueType)
        {
            _serializingObjects.Add(entity);
        }

        try
        {
            return FallbackToDocumentInternal(entityType, entity);
        }
        finally
        {
            if (!entityType.IsValueType)
            {
                _serializingObjects.Remove(entity);
            }
        }
    }

    private static BsonDocument FallbackToDocumentInternal([DynamicallyAccessedMembers(EntityMemberRequirements)] Type entityType, object entity)
    {
        var metadata = GetMetadata(entityType);
        var document = new BsonDocument();

        if (metadata.IdProperty != null)
        {
            var id = metadata.IdProperty.GetValue(entity);
            if (id != null)
            {
                document = document.Set("_id", BsonConversion.ToBsonValue(id));
            }
        }

        foreach (var property in metadata.Properties)
        {
            if (metadata.IdProperty != null && property.Name == metadata.IdProperty.Name)
            {
                continue;
            }

            var value = property.GetValue(entity);
            var bsonValue = ConvertToBsonValue(value);
            document = document.Set(ToCamelCase(property.Name), bsonValue);
        }

        foreach (var field in metadata.Fields)
        {
            var value = field.GetValue(entity);
            var bsonValue = ConvertToBsonValue(value);
            document = document.Set(ToCamelCase(field.Name), bsonValue);
        }

        return document;
    }

    private static object FallbackFromDocument([DynamicallyAccessedMembers(EntityMemberRequirements)] Type entityType, BsonDocument document)
    {
        if (entityType == null) throw new ArgumentNullException(nameof(entityType));
        if (document == null) throw new ArgumentNullException(nameof(document));

        if (entityType.IsValueType)
        {
            var boxed = Activator.CreateInstance(entityType) ?? throw new InvalidOperationException($"Failed to create instance of {entityType.FullName}");
            return PopulateEntityMembers(entityType, boxed, document, isStruct: true);
        }

        var entity = Activator.CreateInstance(entityType) ?? throw new InvalidOperationException($"Failed to create instance of {entityType.FullName}");
        PopulateEntityMembers(entityType, entity, document, isStruct: false);
        return entity;
    }

    private static object PopulateEntityMembers([DynamicallyAccessedMembers(EntityMemberRequirements)] Type entityType, object target, BsonDocument document, bool isStruct)
    {
        var metadata = GetMetadata(entityType);

        foreach (var (key, bsonValue) in document)
        {
            if (key == "_id" && metadata.IdProperty != null)
            {
                var idValue = ConvertFromBsonValue(bsonValue, metadata.IdProperty.PropertyType);
                metadata.IdProperty.SetValue(target, idValue);
                continue;
            }

            if (metadata.CamelCasePropertyMap.TryGetValue(key, out var property) && property.CanWrite)
            {
                var converted = ConvertFromBsonValue(bsonValue, property.PropertyType);
                property.SetValue(target, converted);
                continue;
            }

            if (metadata.CamelCaseFieldMap.TryGetValue(key, out var field))
            {
                var converted = ConvertFromBsonValue(bsonValue, field.FieldType);
                field.SetValue(target, converted);
            }
        }

        return target;
    }

    [UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "实体类型的具体成员由源生成器生成的注册代码保留。")]
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
            return FallbackToDocument(runtimeType, value);
        }

        return BsonConversion.ToBsonValue(value);
    }

    [UnconditionalSuppressMessage("TrimAnalysis", "IL2067", Justification = "AOT发布时由源生成器生成的实体注册代码会标记必要构造函数。")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2062", Justification = "AOT发布时由源生成器生成的实体注册代码会标记必要类型信息。")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "AOT发布时由源生成器生成的实体注册代码会标记必要类型信息。")]
    private static object? ConvertFromBsonValue(BsonValue bsonValue, Type targetType)
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

        if (IsDictionaryType(nonNullableType))
        {
            throw new NotSupportedException($"AOT 回退模式不支持将 BSON 文档反序列化为 {nonNullableType.FullName}，请为该类型注册源生成器适配器。");
        }

        if (IsCollectionType(nonNullableType))
        {
            throw new NotSupportedException($"AOT 回退模式不支持将 BSON 数组反序列化为 {nonNullableType.FullName}，请为该类型注册源生成器适配器。");
        }

        if (IsComplexObjectType(nonNullableType))
        {
            if (bsonValue is BsonDocument nestedDoc)
            {
                return FallbackFromDocument(nonNullableType, nestedDoc);
            }
        }

        return ConvertPrimitiveValue(bsonValue, nonNullableType);
    }

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
        return type.IsClass || (type.IsValueType && !type.IsEnum && !type.IsPrimitive);
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

    private static object? ConvertPrimitiveValue(BsonValue bsonValue, Type targetType)
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
                _ => Convert.ToBoolean(bsonValue.ToString())
            },
            var t when t == typeof(int) => bsonValue switch
            {
                BsonInt32 i32 => i32.Value,
                BsonInt64 i64 => checked((int)i64.Value),
                BsonDouble dbl => Convert.ToInt32(dbl.Value),
                BsonString s => int.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToInt32(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(long) => bsonValue switch
            {
                BsonInt64 i64 => i64.Value,
                BsonInt32 i32 => i32.Value,
                BsonDouble dbl => Convert.ToInt64(dbl.Value),
                BsonString s => long.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToInt64(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(double) => bsonValue switch
            {
                BsonDouble dbl => dbl.Value,
                BsonInt64 i64 => i64.Value,
                BsonInt32 i32 => i32.Value,
                BsonString s => double.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToDouble(bsonValue.ToString(), CultureInfo.InvariantCulture)
            },
            var t when t == typeof(float) =>
                Convert.ToSingle(ConvertPrimitiveValue(bsonValue, typeof(double)), CultureInfo.InvariantCulture),
            var t when t == typeof(decimal) => bsonValue switch
            {
                BsonDecimal128 dec => dec.Value,
                BsonDouble dbl => Convert.ToDecimal(dbl.Value, CultureInfo.InvariantCulture),
                BsonString s => decimal.Parse(s.Value, CultureInfo.InvariantCulture),
                _ => Convert.ToDecimal(bsonValue.ToString(), CultureInfo.InvariantCulture)
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
