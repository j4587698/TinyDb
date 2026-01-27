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
            .Where(property => property.GetIndexParameters().Length == 0)
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

            // 直接调用 FallbackToDocumentInternal，因为 ToDocument<T> 已经处理了循环引用检测
            return FallbackToDocumentInternal(typeof(T), entity!);
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

        return (T)FallbackFromDocument(typeof(T), document);
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
        // Optimize: Use Dictionary to collect elements first to avoid creating multiple BsonDocument instances via chaining Set()
        var elements = new Dictionary<string, BsonValue>(metadata.Properties.Count + metadata.Fields.Count + 1);

        if (metadata.IdProperty != null)
        {
            var id = metadata.IdProperty.GetValue(entity);
            if (id != null)
            {
                elements["_id"] = BsonConversion.ToBsonValue(id);
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
            elements[ToCamelCase(property.Name)] = bsonValue;
        }

        foreach (var field in metadata.Fields)
        {
            var value = field.GetValue(entity);
            var bsonValue = ConvertToBsonValue(value);
            elements[ToCamelCase(field.Name)] = bsonValue;
        }

        return new BsonDocument(elements);
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
                var idValue = ConvertFromBsonValue(bsonValue, GetPropertyValueType(metadata.IdProperty));
                metadata.IdProperty.SetValue(target, idValue);
                continue;
            }

            if (metadata.CamelCasePropertyMap.TryGetValue(key, out var property) && property.CanWrite)
            {
                var converted = ConvertFromBsonValue(bsonValue, GetPropertyValueType(property));
                property.SetValue(target, converted);
                continue;
            }

            if (metadata.CamelCaseFieldMap.TryGetValue(key, out var field))
            {
                var converted = ConvertFromBsonValue(bsonValue, GetFieldValueType(field));
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
                return FallbackFromDocument(nonNullableType, nestedDoc);
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
        return type.IsClass || (type.IsValueType && !type.IsEnum && !type.IsPrimitive);
    }

    [return: DynamicallyAccessedMembers(TypeInspectionRequirements)]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2073", Justification = "成员类型信息由源生成器注册的 AOT 适配器保留。")]
    private static Type GetPropertyValueType(PropertyInfo property) => property.PropertyType;

    [return: DynamicallyAccessedMembers(TypeInspectionRequirements)]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2073", Justification = "成员类型信息由源生成器注册的 AOT 适配器保留。")]
    private static Type GetFieldValueType(FieldInfo field) => field.FieldType;

    [return: DynamicallyAccessedMembers(TypeInspectionRequirements)]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2073", Justification = "集合元素类型由源生成器注册的 AOT 适配器保留。")]
    private static Type? GetArrayElementType(Type arrayType) => arrayType.GetElementType();

    [return: DynamicallyAccessedMembers(TypeInspectionRequirements)]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2073", Justification = "方法参数类型由源生成器注册的 AOT 适配器保留。")]
    private static Type GetParameterType(ParameterInfo parameter) => parameter.ParameterType;

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

    [UnconditionalSuppressMessage("Aot", "IL3050", Justification = "回退路径需要创建泛型字典以保持兼容性，推荐使用 Source Generator 提供的静态适配器。")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2077", Justification = "Type requirements are preserved by ResolveDictionaryTypes but lost in ValueTuple return.")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2080", Justification = "Type requirements are preserved by ResolveDictionaryTypes but lost in ValueTuple return.")]
    private static object? ConvertDictionary([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type dictionaryType, BsonDocument document)
    {
        if (document == null) return null;

        var (keyType, valueType, concreteType) = ResolveDictionaryTypes(dictionaryType);
        if (keyType != typeof(string))
        {
            throw new NotSupportedException($"AOT 回退模式仅支持字符串键的字典，但实际键类型为 {keyType.FullName}。");
        }

        var dictionaryInstance = Activator.CreateInstance(concreteType)
            ?? throw new InvalidOperationException($"无法创建字典类型 {concreteType.FullName} 的实例。");

        var addMethod = concreteType
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(m => m.Name == "Add" && m.GetParameters().Length == 2);
        if (addMethod == null)
        {
            throw new NotSupportedException($"字典类型 {concreteType.FullName} 缺少可用的 Add 方法，无法在 AOT 回退模式下填充数据。");
        }

        foreach (var element in document)
        {
            var value = ConvertFromBsonValue(element.Value, valueType);
            addMethod.Invoke(dictionaryInstance, new[] { element.Key, value });
        }

        return dictionaryInstance;
    }

    [UnconditionalSuppressMessage("Aot", "IL3050", Justification = "回退路径需要创建泛型集合以保持兼容性，推荐使用 Source Generator 提供的静态适配器。")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2075", Justification = "回退路径通过反射访问集合的 Add 方法，仅在缺失 Source Generator 适配器时启用。")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2077", Justification = "Type requirements are preserved by ResolveCollectionTypes but lost in ValueTuple return.")]
    private static object? ConvertCollection([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type collectionType, BsonArray array)
    {
        if (array == null) return null;

        if (collectionType.IsArray)
        {
            var arrayElementType = GetArrayElementType(collectionType)
                ?? throw new NotSupportedException($"无法确定数组类型 {collectionType.FullName} 的元素类型。");

            var arrayInstance = Array.CreateInstance(arrayElementType, array.Count);
            for (int i = 0; i < array.Count; i++)
            {
                var element = ConvertFromBsonValue(array[i], arrayElementType);
                arrayInstance.SetValue(element, i);
            }
            return arrayInstance;
        }

        var (elementType, concreteType) = ResolveCollectionTypes(collectionType);

        var candidateType = concreteType;
        object? instance = TryCreateInstance(candidateType);
        if (instance == null)
        {
            candidateType = typeof(List<>).MakeGenericType(elementType);
            instance = TryCreateInstance(candidateType);
        }

        if (instance == null)
        {
            throw new InvalidOperationException($"无法创建集合类型 {collectionType.FullName} 的实例。");
        }

        var addMethod = instance.GetType()
            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(m => m.Name == "Add" && m.GetParameters().Length == 1);

        if (addMethod == null)
        {
            throw new NotSupportedException($"集合类型 {instance.GetType().FullName} 缺少可用的 Add 方法，无法在 AOT 回退模式下填充数据。");
        }

        var addParameterType = GetParameterType(addMethod.GetParameters()[0]);

        foreach (var bsonValue in array)
        {
            var element = ConvertFromBsonValue(bsonValue, addParameterType);
            addMethod.Invoke(instance, new[] { element });
        }

        if (collectionType.IsAssignableFrom(instance.GetType()))
        {
        return instance;
    }

        var constructed = TryWrapWithTargetCollection(collectionType, instance);
        if (constructed != null)
        {
            return constructed;
        }

        throw new NotSupportedException($"AOT 回退模式无法将 {instance.GetType().FullName} 转换为 {collectionType.FullName}。请为该集合类型注册源生成器适配器。");
    }

    [UnconditionalSuppressMessage("Aot", "IL3050", Justification = "回退路径需要创建泛型字典以保持兼容性，推荐使用 Source Generator 提供的静态适配器。")]
    private static (Type KeyType, Type ValueType, Type ConcreteType) ResolveDictionaryTypes([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type dictionaryType)
    {
        var sourceType = dictionaryType;
        if (!sourceType.IsGenericType || sourceType.GetGenericArguments().Length != 2)
        {
            sourceType = dictionaryType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>))
                ?? throw new NotSupportedException($"字典类型 {dictionaryType.FullName} 未实现泛型 IDictionary<TKey, TValue> 接口，无法在 AOT 回退模式下使用。");
        }

        var args = sourceType.GetGenericArguments();
        var keyType = args[0];
        var valueType = args[1];

        var concreteType = dictionaryType;
        if (dictionaryType.IsInterface || dictionaryType.IsAbstract)
        {
            concreteType = typeof(Dictionary<,>).MakeGenericType(keyType, valueType);
        }

        return (keyType, valueType, concreteType);
    }

    [UnconditionalSuppressMessage("Aot", "IL3050", Justification = "回退路径需要创建泛型集合以保持兼容性，推荐使用 Source Generator 提供的静态适配器。")]
    private static (Type ElementType, Type ConcreteType) ResolveCollectionTypes([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type collectionType)
    {
        var elementType = ResolveCollectionElementType(collectionType);

        if (collectionType.IsInterface || collectionType.IsAbstract)
        {
            var listType = typeof(List<>).MakeGenericType(elementType);
            return (elementType, listType);
        }

        return (elementType, collectionType);
    }

    private static Type ResolveCollectionElementType([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type collectionType)
    {
        if (collectionType.IsArray)
        {
            return GetArrayElementType(collectionType)
                   ?? throw new NotSupportedException($"无法确定数组类型 {collectionType.FullName} 的元素类型。");
        }

        if (collectionType.IsGenericType && collectionType.GetGenericArguments().Length == 1)
        {
            return collectionType.GetGenericArguments()[0];
        }

        var interfaceType = collectionType.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && (i.GetGenericTypeDefinition() == typeof(IEnumerable<>) ||
                                                     i.GetGenericTypeDefinition() == typeof(ICollection<>) ||
                                                     i.GetGenericTypeDefinition() == typeof(IList<>)));

        return interfaceType?.GetGenericArguments()[0] ?? typeof(object);
    }

    private static object? TryCreateInstance([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type type)
    {
        try
        {
            return Activator.CreateInstance(type);
        }
        catch
        {
            return null;
        }
    }

    private static object? TryWrapWithTargetCollection([DynamicallyAccessedMembers(TypeInspectionRequirements)] Type targetType, object sourceCollection)
    {
        var sourceType = sourceCollection.GetType();

        var matchingCtor = targetType.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(ctor =>
            {
                var parameters = ctor.GetParameters();
                return parameters.Length == 1 && GetParameterType(parameters[0]).IsInstanceOfType(sourceCollection);
            });
        if (matchingCtor != null)
        {
            return matchingCtor.Invoke(new[] { sourceCollection });
        }

        var enumerableCtor = targetType.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .FirstOrDefault(ctor =>
            {
                var parameters = ctor.GetParameters();
                return parameters.Length == 1 && typeof(IEnumerable).IsAssignableFrom(GetParameterType(parameters[0]));
            });

        if (enumerableCtor != null)
        {
            return enumerableCtor.Invoke(new[] { sourceCollection });
        }

        return null;
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
                BsonString s => Enum.Parse(t, s.Value, true),
                BsonInt32 i32 => Enum.ToObject(t, i32.Value),
                BsonInt64 i64 => Enum.ToObject(t, i64.Value),
                _ => Enum.Parse(t, bsonValue.ToString(), true)
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
