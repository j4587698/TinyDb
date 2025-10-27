using System.Collections.Concurrent;
using System.Reflection;
using System.Linq.Expressions;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Diagnostics.CodeAnalysis;
using SimpleDb.Bson;

namespace SimpleDb.Serialization;

/// <summary>
/// BSON 对象映射器，负责 C# 对象和 BSON 文档之间的转换
/// </summary>
public static class BsonMapper
{
    private static readonly ConcurrentDictionary<Type, ObjectSerializer> _serializers = new();
    private static readonly ConcurrentDictionary<Type, object> _idPropertyCache = new();

    /// <summary>
    /// 将 C# 对象转换为 BSON 文档
    /// </summary>
    /// <typeparam name="T">对象类型</typeparam>
    /// <param name="entity">对象实例</param>
    /// <returns>BSON 文档</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)]
    public static BsonDocument ToDocument<T>(T entity)
        where T : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var serializer = GetOrCreateSerializer<T>();
        return serializer.Serialize(entity);
    }

    /// <summary>
    /// 将 BSON 文档转换为 C# 对象
    /// </summary>
    /// <typeparam name="T">对象类型</typeparam>
    /// <param name="document">BSON 文档</param>
    /// <returns>对象实例</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T? ToObject<T>(BsonDocument document)
        where T : class
    {
        if (document == null) return default(T);

        var serializer = GetOrCreateSerializer<T>();
        return (T?)serializer.Deserialize(document);
    }

    /// <summary>
    /// 获取或创建对象序列化器
    /// </summary>
    /// <typeparam name="T">对象类型</typeparam>
    /// <returns>序列化器实例</returns>
    private static ObjectSerializer<T> GetOrCreateSerializer<T>()
        where T : class
    {
        var type = typeof(T);
        return (ObjectSerializer<T>)_serializers.GetOrAdd(type,
            _ => new ObjectSerializer<T>());
    }

    /// <summary>
    /// 获取对象的 ID 属性
    /// </summary>
    /// <typeparam name="T">对象类型</typeparam>
    /// <returns>ID 属性访问器</returns>
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
    public static PropertyAccessor<T>? GetIdProperty<T>()
        where T : class
    {
        var type = typeof(T);
        if (_idPropertyCache.TryGetValue(type, out var cached))
        {
            return (PropertyAccessor<T>?)cached;
        }

        var property = FindIdProperty<T>();
        _idPropertyCache[type] = property!;
        return property;
    }

    /// <summary>
    /// 查找 ID 属性
    /// </summary>
    /// <typeparam name="T">对象类型</typeparam>
    /// <returns>ID 属性访问器</returns>
    private static PropertyAccessor<T>? FindIdProperty<T>()
        where T : class
    {
        var type = typeof(T);
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite);

        // 优先查找名为 "Id" 的属性
        var idProperty = properties.FirstOrDefault(p =>
            string.Equals(p.Name, "Id", StringComparison.OrdinalIgnoreCase));

        if (idProperty != null)
        {
            return new PropertyAccessor<T>(idProperty);
        }

        // 查找带有 BsonIdAttribute 的属性
        idProperty = properties.FirstOrDefault(p =>
            p.GetCustomAttribute<BsonIdAttribute>() != null);

        if (idProperty != null)
        {
            return new PropertyAccessor<T>(idProperty);
        }

        // 查找名为 "_id" 的属性
        idProperty = properties.FirstOrDefault(p =>
            string.Equals(p.Name, "_id", StringComparison.OrdinalIgnoreCase));

        return idProperty != null ? new PropertyAccessor<T>(idProperty) : null;
    }

    /// <summary>
    /// 获取对象的 ID 值
    /// </summary>
    /// <typeparam name="T">对象类型</typeparam>
    /// <param name="entity">对象实例</param>
    /// <returns>ID 值</returns>
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
    public static BsonValue GetId<T>(T entity)
        where T : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var idProperty = GetIdProperty<T>();
        if (idProperty == null)
        {
            throw new InvalidOperationException($"Type {typeof(T).Name} does not have an ID property");
        }

        var idValue = idProperty.GetValue(entity);
        return ConvertToBsonValue(idValue);
    }

    /// <summary>
    /// 设置对象的 ID 值
    /// </summary>
    /// <typeparam name="T">对象类型</typeparam>
    /// <param name="entity">对象实例</param>
    /// <param name="id">ID 值</param>
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)]
    public static void SetId<T>(T entity, BsonValue id)
        where T : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        var idProperty = GetIdProperty<T>();
        if (idProperty == null)
        {
            throw new InvalidOperationException($"Type {typeof(T).Name} does not have an ID property");
        }

        var idValue = ConvertFromBsonValue(id, idProperty.PropertyType);
        idProperty.SetValue(entity, idValue);
    }

    /// <summary>
    /// 转换 C# 值为 BSON 值
    /// </summary>
    /// <param name="value">C# 值</param>
    /// <returns>BSON 值</returns>
    public static BsonValue ConvertToBsonValue(object? value)
    {
        return value switch
        {
            null => BsonNull.Value,
            string str => str,
            int i => i,
            long l => l,
            short s => (int)s,
            ushort us => (int)us,
            uint ui => (long)ui,
            ulong ul => (long)ul,
            float f => (double)f,
            double d => d,
            decimal dec => (double)dec,
            bool b => b,
            DateTime dt => dt,
            DateTimeOffset dto => dto.DateTime,
            Guid guid => guid.ToString(),
            ObjectId oid => oid,
            Enum enumValue => ConvertToBsonValue(Convert.ChangeType(enumValue, Enum.GetUnderlyingType(enumValue.GetType()))),
            BsonValue bsonValue => bsonValue,
            IDictionary<string, object?> dict => BsonDocument.FromDictionary(new Dictionary<string, object?>(dict)),
            IEnumerable<object?> list => BsonArray.FromList(list.ToList()),
            _ => ConvertComplexTypeToBsonValue(value)
        };
    }

    /// <summary>
    /// 转换 BSON 值为指定类型
    /// </summary>
    /// <param name="bsonValue">BSON 值</param>
    /// <param name="targetType">目标类型</param>
    /// <returns>C# 值</returns>
    private static object? ConvertBsonValueToType(BsonValue bsonValue, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] Type targetType)
    {
        return ConvertFromBsonValue(bsonValue, targetType);
    }

    /// <summary>
    /// 转换 BSON 值为 C# 值
    /// </summary>
    /// <param name="bsonValue">BSON 值</param>
    /// <param name="targetType">目标类型</param>
    /// <returns>C# 值</returns>
    public static object? ConvertFromBsonValue(BsonValue bsonValue, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] Type targetType)
    {
        if (bsonValue.IsNull) return GetDefaultValue(targetType);

        return Type.GetTypeCode(targetType) switch
        {
            TypeCode.String => bsonValue.ToString(),
            TypeCode.Int16 => (short)bsonValue.ToInt32(CultureInfo.InvariantCulture),
            TypeCode.Int32 => bsonValue.ToInt32(CultureInfo.InvariantCulture),
            TypeCode.Int64 => bsonValue.ToInt64(CultureInfo.InvariantCulture),
            TypeCode.UInt16 => (ushort)bsonValue.ToInt32(CultureInfo.InvariantCulture),
            TypeCode.UInt32 => (uint)bsonValue.ToInt64(CultureInfo.InvariantCulture),
            TypeCode.UInt64 => (ulong)bsonValue.ToInt64(CultureInfo.InvariantCulture),
            TypeCode.Single => (float)bsonValue.ToDouble(CultureInfo.InvariantCulture),
            TypeCode.Double => bsonValue.ToDouble(CultureInfo.InvariantCulture),
            TypeCode.Decimal => (decimal)bsonValue.ToDouble(CultureInfo.InvariantCulture),
            TypeCode.Boolean => bsonValue.ToBoolean(CultureInfo.InvariantCulture),
            TypeCode.DateTime => bsonValue.ToDateTime(CultureInfo.InvariantCulture),
            TypeCode.Object when targetType == typeof(Guid) => Guid.Parse(bsonValue.ToString()),
            TypeCode.Object when targetType == typeof(ObjectId) => bsonValue.As<ObjectId>(),
            TypeCode.Object when targetType.IsEnum => Enum.Parse(targetType, bsonValue.ToString()),
            TypeCode.Object when targetType.IsClass => ConvertComplexTypeFromBsonValue(bsonValue, targetType),
            _ => Convert.ChangeType(bsonValue.RawValue, targetType, CultureInfo.InvariantCulture)
        };
    }

    /// <summary>
    /// 转换复杂类型为 BSON 值
    /// </summary>
    /// <param name="value">复杂类型值</param>
    /// <returns>BSON 值</returns>
    private static BsonValue ConvertComplexTypeToBsonValue(object value)
    {
        var type = value.GetType();
        var serializer = _serializers.GetOrAdd(type, _ => CreateSerializer(type));
        return serializer.Serialize(value);
    }

    /// <summary>
    /// 从 BSON 值转换复杂类型
    /// </summary>
    /// <param name="bsonValue">BSON 值</param>
    /// <param name="targetType">目标类型</param>
    /// <returns>复杂类型值</returns>
    private static object? ConvertComplexTypeFromBsonValue(BsonValue bsonValue, Type targetType)
    {
        if (bsonValue is BsonDocument doc)
        {
            var serializer = _serializers.GetOrAdd(targetType, _ => CreateSerializer(targetType));
            return serializer.Deserialize(doc);
        }

        throw new NotSupportedException($"Cannot convert BSON type {bsonValue.BsonType} to {targetType.Name}");
    }

    /// <summary>
    /// 创建序列化器
    /// </summary>
    /// <param name="type">类型</param>
    /// <returns>序列化器</returns>
    private static ObjectSerializer CreateSerializer(Type type)
    {
        var serializerType = typeof(ObjectSerializer<>).MakeGenericType(type);
        return (ObjectSerializer)Activator.CreateInstance(serializerType)!;
    }

    /// <summary>
    /// 获取类型的默认值
    /// </summary>
    /// <param name="type">类型</param>
    /// <returns>默认值</returns>
    private static object? GetDefaultValue(Type type)
    {
        return type.IsValueType ? Activator.CreateInstance(type) : null;
    }
}

/// <summary>
/// 对象序列化器基类
/// </summary>
public abstract class ObjectSerializer
{
    public abstract BsonDocument Serialize(object obj);
    public abstract object? Deserialize(BsonDocument document);
}

/// <summary>
/// 对象序列化器
/// </summary>
/// <typeparam name="T">对象类型</typeparam>
public sealed class ObjectSerializer<T> : ObjectSerializer
    where T : class
{
    private readonly Dictionary<string, PropertyAccessor<T>> _properties;
    private readonly PropertyAccessor<T>? _idProperty;

    public ObjectSerializer()
    {
        var type = typeof(T);

        // 调试信息
        Console.WriteLine($"[DEBUG] ObjectSerializer for {type.Name}");

        _properties = type
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)
            .Where(p => !IsIgnoredProperty(p))
            .ToDictionary(
                p => GetPropertyName(p),
                p => new PropertyAccessor<T>(p)
            );

        // 尝试使用反射获取ID属性
        _idProperty = BsonMapper.GetIdProperty<T>();

        // 如果反射失败，尝试智能识别ID属性
        if (_idProperty == null)
        {
            Console.WriteLine($"[DEBUG] ObjectSerializer: Using smart ID property detection");
            Console.WriteLine($"[DEBUG] ObjectSerializer: Properties count: {_properties.Count}");
            foreach (var prop in _properties.Keys)
            {
                Console.WriteLine($"[DEBUG] ObjectSerializer: Found property '{prop}'");
            }

            // 查找标准ID属性名称
            var standardIdNames = new[] { "Id", "_id", "ID" };
            foreach (var idName in standardIdNames)
            {
                if (_properties.TryGetValue(idName, out var accessor))
                {
                    _idProperty = accessor;
                    Console.WriteLine($"[DEBUG] ObjectSerializer: Found ID property '{idName}' via smart detection");
                    break;
                }
            }

            if (_idProperty == null)
            {
                Console.WriteLine($"[DEBUG] ObjectSerializer: No ID property found in _properties");
            }
        }

        Console.WriteLine($"[DEBUG] ObjectSerializer: _idProperty is null: {_idProperty == null}");
        if (_idProperty != null)
        {
            Console.WriteLine($"[DEBUG] ObjectSerializer: ID property name: {_idProperty.Property.Name}");
        }
    }

    /// <summary>
    /// 序列化对象为 BSON 文档
    /// </summary>
    /// <param name="obj">对象</param>
    /// <returns>BSON 文档</returns>
    public BsonDocument Serialize(T obj)
    {
        if (obj == null) throw new ArgumentNullException(nameof(obj));

        var document = new BsonDocument();
        var entityType = typeof(T);

        // 尝试使用生成的mapper处理ID字段
        try
        {
            var mapperType = Type.GetType($"{entityType.Namespace}.{entityType.Name}Mapper");
            if (mapperType != null)
            {
                var getIdMethod = mapperType.GetMethod("GetId", new[] { entityType });
                if (getIdMethod != null)
                {
                    Console.WriteLine($"[DEBUG] Serialize: Using generated mapper for {entityType.Name}");

                    // 直接从生成的mapper获取ID并设置为_id字段
                    var idValue = (BsonValue)getIdMethod.Invoke(null, new object[] { obj })!;
                    if (!idValue.IsNull)
                    {
                        document = document.Set("_id", idValue);
                        Console.WriteLine($"[DEBUG] Serialize: Set _id field from generated mapper: {idValue}");
                    }

                    // 序列化其他属性（排除ID属性，因为已经处理了）
                    foreach (var (name, accessor) in _properties)
                    {
                        // 跳过ID相关属性，避免重复
                        if (name.Contains("Id", StringComparison.OrdinalIgnoreCase))
                        {
                            Console.WriteLine($"[DEBUG] Serialize: Skipping ID property '{name}' (handled by generated mapper)");
                            continue;
                        }

                        try
                        {
                            var value = accessor.GetValue(obj);
                            var bsonValue = BsonMapper.ConvertToBsonValue(value);
                            document = document.Set(name, bsonValue);
                        }
                        catch (Exception ex)
                        {
                            throw new InvalidOperationException($"Failed to serialize property '{name}' of type {typeof(T).Name}", ex);
                        }
                    }

                    return document;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[DEBUG] Generated mapper serialization failed: {ex.Message}");
        }

        // 普通类型的序列化逻辑（备用）
        foreach (var (name, accessor) in _properties)
        {
            try
            {
                var value = accessor.GetValue(obj);
                var bsonValue = BsonMapper.ConvertToBsonValue(value);

                // 如果这是ID属性，使用"_id"作为字段名
                var fieldName = (_idProperty != null && accessor.Property.Name == _idProperty.Property.Name) ? "_id" : name;

                // 调试信息
                if (name.Contains("Id", StringComparison.OrdinalIgnoreCase))
                {
                    Console.WriteLine($"[DEBUG] Serialize: property '{name}' -> fieldName '{fieldName}', _idProperty null: {_idProperty == null}");
                }

                document = document.Set(fieldName, bsonValue);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to serialize property '{name}' of type {typeof(T).Name}", ex);
            }
        }

        return document;
    }

    
    /// <summary>
    /// 序列化对象为 BSON 文档（基类方法）
    /// </summary>
    /// <param name="obj">对象</param>
    /// <returns>BSON 文档</returns>
    public override BsonDocument Serialize(object obj)
    {
        if (obj is not T typedObj)
            throw new ArgumentException($"Object must be of type {typeof(T).Name}", nameof(obj));

        return Serialize(typedObj);
    }

    /// <summary>
    /// 反序列化 BSON 文档为对象（基类方法）
    /// </summary>
    /// <param name="document">BSON 文档</param>
    /// <returns>对象</returns>
    public override object? Deserialize(BsonDocument document)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        var obj = Activator.CreateInstance<T>();
        if (obj == null) return null;

        var type = typeof(T);
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        foreach (var property in properties)
        {
            if (IsIgnoredProperty(property)) continue;

            // 检查文档中是否包含对应的字段
            string fieldName = property.Name;

            // 如果这是ID属性，检查"_id"字段
            if (_idProperty != null && property.Name == _idProperty.Property.Name)
            {
                fieldName = "_id";
            }

            if (document.ContainsKey(fieldName))
            {
                var value = BsonMapper.ConvertFromBsonValue(document[fieldName], property.PropertyType);
                property.SetValue(obj, value);
            }
        }

        return obj;
    }

    /// <summary>
    /// 检查属性是否应该被忽略
    /// </summary>
    /// <param name="property">属性信息</param>
    /// <returns>是否忽略</returns>
    private static bool IsIgnoredProperty(PropertyInfo property)
    {
        return property.GetCustomAttribute<BsonIgnoreAttribute>() != null;
    }

    /// <summary>
    /// 获取属性名称
    /// </summary>
    /// <param name="property">属性信息</param>
    /// <returns>属性名称</returns>
    private static string GetPropertyName(PropertyInfo property)
    {
        var attribute = property.GetCustomAttribute<BsonElementAttribute>();
        return attribute?.ElementName ?? property.Name;
    }
}

/// <summary>
/// 属性访问器
/// </summary>
/// <typeparam name="T">对象类型</typeparam>
public sealed class PropertyAccessor<T>
{
    private readonly Func<T, object?> _getter;
    private readonly Action<T, object?> _setter;

    /// <summary>
    /// 属性信息
    /// </summary>
    public PropertyInfo Property { get; }

    /// <summary>
    /// 属性类型
    /// </summary>
    public Type PropertyType => Property.PropertyType;

    /// <summary>
    /// 初始化属性访问器
    /// </summary>
    /// <param name="property">属性信息</param>
    public PropertyAccessor(PropertyInfo property)
    {
        Property = property ?? throw new ArgumentNullException(nameof(property));
        _getter = CreateGetter(property);
        _setter = CreateSetter(property);
    }

    /// <summary>
    /// 获取属性值
    /// </summary>
    /// <param name="obj">对象实例</param>
    /// <returns>属性值</returns>
    public object? GetValue(T obj)
    {
        if (obj == null) throw new ArgumentNullException(nameof(obj));
        return _getter(obj);
    }

    /// <summary>
    /// 设置属性值
    /// </summary>
    /// <param name="obj">对象实例</param>
    /// <param name="value">属性值</param>
    public void SetValue(T obj, object? value)
    {
        if (obj == null) throw new ArgumentNullException(nameof(obj));
        _setter(obj, value);
    }

    /// <summary>
    /// 创建属性获取器
    /// </summary>
    /// <param name="property">属性信息</param>
    /// <returns>获取器委托</returns>
    private static Func<T, object?> CreateGetter(PropertyInfo property)
    {
        var instance = Expression.Parameter(typeof(T), "instance");

        Expression propertyAccess;

        // 如果是 BsonDocument，使用索引器访问
        if (typeof(T) == typeof(BsonDocument))
        {
            var key = Expression.Constant(property.Name, typeof(string));
            var indexerProperty = typeof(BsonDocument).GetProperty("Item", typeof(string));
            if (indexerProperty != null)
            {
                propertyAccess = Expression.Property(instance, indexerProperty, key);
            }
            else
            {
                // 如果找不到索引器属性，返回默认值
                var defaultValue = Expression.Constant(null, typeof(object));
                propertyAccess = defaultValue;
            }
        }
        else
        {
            // 普通属性访问
            propertyAccess = Expression.Property(instance, property);
        }

        var convert = Expression.Convert(propertyAccess, typeof(object));
        var lambda = Expression.Lambda<Func<T, object?>>(convert, instance);
        return lambda.Compile();
    }

    /// <summary>
    /// 创建属性设置器
    /// </summary>
    /// <param name="property">属性信息</param>
    /// <returns>设置器委托</returns>
    private static Action<T, object?> CreateSetter(PropertyInfo property)
    {
        var instance = Expression.Parameter(typeof(T), "instance");
        var value = Expression.Parameter(typeof(object), "value");

        Expression propertyAccess;

        // 如果是 BsonDocument，使用索引器访问
        if (typeof(T) == typeof(BsonDocument))
        {
            var key = Expression.Constant(property.Name, typeof(string));
            var indexerProperty = typeof(BsonDocument).GetProperty("Item", typeof(string));
            if (indexerProperty != null)
            {
                propertyAccess = Expression.Property(instance, indexerProperty, key);
            }
            else
            {
                // 如果找不到索引器属性，返回默认值
                var defaultValue = Expression.Constant(null, typeof(object));
                propertyAccess = defaultValue;
            }
        }
        else
        {
            // 普通属性访问
            propertyAccess = Expression.Property(instance, property);
        }

        // 将值转换为BsonValue
        var convert = Expression.Call(
            typeof(BsonMapper).GetMethod(nameof(BsonMapper.ConvertToBsonValue), new[] { typeof(object) }),
            value);

        Expression assign;

        if (typeof(T) == typeof(BsonDocument))
        {
            // 对于BsonDocument，使用Set方法而不是直接赋值
            var setMethod = typeof(BsonDocument).GetMethod(nameof(BsonDocument.Set), new[] { typeof(string), typeof(BsonValue) });
            var keyConstant = Expression.Constant(property.Name, typeof(string));
            assign = Expression.Call(instance, setMethod, keyConstant, convert);
        }
        else
        {
            // 普通属性赋值
            var propertyConvert = Expression.Convert(value, property.PropertyType);
            assign = Expression.Assign(propertyAccess, propertyConvert);
        }

        var lambda = Expression.Lambda<Action<T, object?>>(assign, instance, value);
        return lambda.Compile();
    }
}

/// <summary>
/// BSON ID 属性标记
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class BsonIdAttribute : Attribute
{
}

/// <summary>
/// BSON 元素名称标记
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class BsonElementAttribute : Attribute
{
    public string ElementName { get; }

    public BsonElementAttribute(string elementName)
    {
        ElementName = elementName ?? throw new ArgumentNullException(nameof(elementName));
    }
}

/// <summary>
/// BSON 忽略属性标记
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class BsonIgnoreAttribute : Attribute
{
}