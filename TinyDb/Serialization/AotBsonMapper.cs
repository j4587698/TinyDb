using System.Diagnostics.CodeAnalysis;
using System.Linq;
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

    [ThreadStatic]
    private static HashSet<Type>? _serializingTypes;
    public static BsonDocument ToDocument<T>(T entity)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.ToDocument(entity);
        }

        return FallbackToDocument(entity);
    }

    public static T FromDocument<T>(BsonDocument document)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.FromDocument(document);
        }

        return FallbackFromDocument<T>(document);
    }

    public static BsonValue GetId<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity)
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

    public static void SetId<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity, BsonValue id)
        where T : class, new()
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            adapter.SetId(entity, id);
            return;
        }

        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty == null || id == null || id.IsNull)
        {
            return;
        }

        var converted = BsonConversion.FromBsonValue(id, idProperty.PropertyType);
        idProperty.SetValue(entity, converted);
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

    private static BsonDocument FallbackToDocument<T>(T entity)
    {
        // 初始化ThreadLocal变量
        _serializingObjects ??= new HashSet<object>();
        _serializingTypes ??= new HashSet<Type>();

        // 检查循环引用
        if (_serializingObjects.Contains(entity))
        {
            // 检测到循环引用，返回包含id的文档（如果有id属性）
            var circularDoc = new BsonDocument();
            var idProperty = EntityMetadata<T>.IdProperty;
            if (idProperty != null)
            {
                var id = idProperty.GetValue(entity);
                if (id != null)
                {
                    circularDoc = circularDoc.Set("_id", BsonConversion.ToBsonValue(id));
                }
            }
            return circularDoc;
        }

        _serializingObjects.Add(entity);
        _serializingTypes.Add(typeof(T));

        try
        {
            var document = new BsonDocument();

        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty != null)
        {
            var id = idProperty.GetValue(entity);
            if (id != null)
            {
                document = document.Set("_id", BsonConversion.ToBsonValue(id));
            }
        }

        foreach (var property in EntityMetadata<T>.Properties)
        {
            if (idProperty != null && property.Name == idProperty.Name)
            {
                continue;
            }

            var value = property.GetValue(entity);

            var key = ToCamelCase(property.Name);
            BsonValue bsonValue;
            if (value == null)
            {
                bsonValue = BsonNull.Value;
            }
            // 对于Dictionary类型，转换为BsonDocument
            else if (IsDictionaryType(property.PropertyType))
            {
                bsonValue = ConvertDictionaryToBsonDocument(value);
            }
            // 对于复杂对象，递归序列化为BsonDocument
            else if (IsComplexObjectType(property.PropertyType))
            {
                bsonValue = ToDocument(property.PropertyType, value);
            }
            // 对于集合类型，转换为BsonArray
            else if (IsCollectionType(property.PropertyType))
            {
                bsonValue = ConvertCollectionToBsonArray(value);
            }
            else
            {
                bsonValue = BsonConversion.ToBsonValue(value);
            }
            document = document.Set(key, bsonValue);
        }

        // 处理公共字段（支持AOT反射测试）
        var publicFields = typeof(T).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Where(f => !f.IsSpecialName && !f.IsLiteral); // 排除常量字段
        foreach (var field in publicFields)
        {
            var value = field.GetValue(entity);
            var key = ToCamelCase(field.Name);
            BsonValue bsonValue;
            if (value == null)
            {
                bsonValue = BsonNull.Value;
            }
            // 对于Dictionary类型，转换为BsonDocument
            else if (IsDictionaryType(field.FieldType))
            {
                bsonValue = ConvertDictionaryToBsonDocument(value);
            }
            // 对于复杂对象，递归序列化为BsonDocument
            else if (IsComplexObjectType(field.FieldType))
            {
                bsonValue = ToDocument(field.FieldType, value);
            }
            // 对于集合类型，转换为BsonArray
            else if (IsCollectionType(field.FieldType))
            {
                bsonValue = ConvertCollectionToBsonArray(value);
            }
            else
            {
                bsonValue = BsonConversion.ToBsonValue(value);
            }
            document = document.Set(key, bsonValue);
        }

            return document;
        }
        finally
        {
            // 清理循环引用检测
            _serializingObjects?.Remove(entity);
            _serializingTypes?.Remove(typeof(T));
        }
    }

    private static T FallbackFromDocument<T>(BsonDocument document)
    {
        // 对于结构体，需要特殊处理，因为结构体是值类型
        if (typeof(T).IsValueType)
        {
            return FallbackFromDocumentForStruct<T>(document);
        }

        var entity = (T)Activator.CreateInstance(typeof(T))!;

        var idProperty = EntityMetadata<T>.IdProperty;
        var propertyMap = EntityMetadata<T>.Properties.ToDictionary(prop => ToCamelCase(prop.Name));
        var fieldMap = typeof(T).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Where(f => !f.IsSpecialName && !f.IsLiteral)
            .ToDictionary(field => ToCamelCase(field.Name));

        foreach (var (key, bsonValue) in document)
        {
            if (key == "_id" && idProperty != null)
            {
                AotIdAccessor<T>.SetId(entity, bsonValue);
                continue;
            }

            // 首先尝试处理属性
            if (propertyMap.TryGetValue(key, out var property) && property.CanWrite)
            {
                if (bsonValue == null || bsonValue.IsNull)
                {
                    property.SetValue(entity, null);
                    continue;
                }

                object? value;
                // 对于Dictionary类型，特殊处理BsonDocument
                if (bsonValue is BsonDocument dictDoc && IsDictionaryType(property.PropertyType))
                {
                    value = ConvertBsonDocumentToDictionary(dictDoc, property.PropertyType);
                }
                // 对于复杂对象类型，递归使用AotBsonMapper
                else if (bsonValue is BsonDocument complexDoc && IsComplexObjectType(property.PropertyType))
                {
                    value = FromDocument(property.PropertyType, complexDoc);
                }
                // 对于集合类型，特殊处理BsonArray
                else if (bsonValue is BsonArray bsonArray && IsCollectionType(property.PropertyType))
                {
                    value = ConvertBsonArrayToCollection(bsonArray, property.PropertyType);
                }
                else
                {
                    value = BsonConversion.FromBsonValue(bsonValue, property.PropertyType);
                }

                property.SetValue(entity, value);
                continue;
            }

            // 如果没有对应的属性，尝试处理字段
            if (fieldMap.TryGetValue(key, out var field))
            {
                if (bsonValue == null || bsonValue.IsNull)
                {
                    if (field.FieldType.IsValueType)
                    {
                        // 值类型字段不能设置为null，跳过
                        continue;
                    }
                    field.SetValue(entity, null);
                    continue;
                }

                object? value;
                // 对于Dictionary类型，特殊处理BsonDocument
                if (bsonValue is BsonDocument dictDoc && IsDictionaryType(field.FieldType))
                {
                    value = ConvertBsonDocumentToDictionary(dictDoc, field.FieldType);
                }
                // 对于复杂对象类型，递归使用AotBsonMapper
                else if (bsonValue is BsonDocument complexDoc && IsComplexObjectType(field.FieldType))
                {
                    value = FromDocument(field.FieldType, complexDoc);
                }
                // 对于集合类型，特殊处理BsonArray
                else if (bsonValue is BsonArray bsonArray && IsCollectionType(field.FieldType))
                {
                    value = ConvertBsonArrayToCollection(bsonArray, field.FieldType);
                }
                else
                {
                    value = BsonConversion.FromBsonValue(bsonValue, field.FieldType);
                }

                field.SetValue(entity, value);
            }
        }

        return entity;
    }

    /// <summary>
    /// 专门处理结构体的反序列化，因为结构体是值类型，需要特殊处理
    /// </summary>
    private static T FallbackFromDocumentForStruct<T>(BsonDocument document)
    {
        // 创建默认结构体实例
        var entity = (T)Activator.CreateInstance(typeof(T))!;

        // 获取所有属性和字段
        var propertyMap = EntityMetadata<T>.Properties.ToDictionary(prop => ToCamelCase(prop.Name));
        var fieldMap = typeof(T).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance)
            .Where(f => !f.IsSpecialName && !f.IsLiteral)
            .ToDictionary(field => ToCamelCase(field.Name));

        // 由于结构体是值类型，我们需要使用反射来直接修改字段
        // 或者使用装箱/拆箱技术
        object boxedEntity = entity; // 装箱结构体

        foreach (var (key, bsonValue) in document)
        {
            // 首先尝试处理属性
            if (propertyMap.TryGetValue(key, out var property) && property.CanWrite)
            {
                if (bsonValue == null || bsonValue.IsNull)
                {
                    property.SetValue(boxedEntity, null);
                    continue;
                }

                object? value;
                // 对于Dictionary类型，特殊处理BsonDocument
                if (bsonValue is BsonDocument dictDoc && IsDictionaryType(property.PropertyType))
                {
                    value = ConvertBsonDocumentToDictionary(dictDoc, property.PropertyType);
                }
                // 对于复杂对象类型，递归使用AotBsonMapper
                else if (bsonValue is BsonDocument complexDoc && IsComplexObjectType(property.PropertyType))
                {
                    value = FromDocument(property.PropertyType, complexDoc);
                }
                // 对于集合类型，特殊处理BsonArray
                else if (bsonValue is BsonArray bsonArray && IsCollectionType(property.PropertyType))
                {
                    value = ConvertBsonArrayToCollection(bsonArray, property.PropertyType);
                }
                else
                {
                    value = BsonConversion.FromBsonValue(bsonValue, property.PropertyType);
                }

                property.SetValue(boxedEntity, value);
                continue;
            }

            // 如果没有对应的属性，尝试处理字段
            if (fieldMap.TryGetValue(key, out var field))
            {
                if (bsonValue == null || bsonValue.IsNull)
                {
                    if (field.FieldType.IsValueType)
                    {
                        // 值类型字段不能设置为null，跳过
                        continue;
                    }
                    field.SetValue(boxedEntity, null);
                    continue;
                }

                object? value;
                // 对于Dictionary类型，特殊处理BsonDocument
                if (bsonValue is BsonDocument dictDoc && IsDictionaryType(field.FieldType))
                {
                    value = ConvertBsonDocumentToDictionary(dictDoc, field.FieldType);
                }
                // 对于复杂对象类型，递归使用AotBsonMapper
                else if (bsonValue is BsonDocument complexDoc && IsComplexObjectType(field.FieldType))
                {
                    value = FromDocument(field.FieldType, complexDoc);
                }
                // 对于集合类型，特殊处理BsonArray
                else if (bsonValue is BsonArray bsonArray && IsCollectionType(field.FieldType))
                {
                    value = ConvertBsonArrayToCollection(bsonArray, field.FieldType);
                }
                else
                {
                    value = BsonConversion.FromBsonValue(bsonValue, field.FieldType);
                }

                field.SetValue(boxedEntity, value);
            }
        }

        // 拆箱并返回修改后的结构体
        return (T)boxedEntity;
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
        if (type.IsArray || IsCollectionType(type) || IsDictionaryType(type))
        {
            return false;
        }

        // 处理复杂对象类型（class 和 struct）
        return type.IsClass || (type.IsValueType && !type.IsEnum && !type.IsPrimitive);
    }

    /// <summary>
    /// 判断是否为集合类型
    /// </summary>
    private static bool IsCollectionType(Type type)
    {
        // 包含数组类型
        if (type.IsArray)
        {
            return true;
        }

        // 包含泛型集合类型（排除Dictionary，Dictionary是复杂对象）
        return type.IsGenericType &&
               (type.GetGenericTypeDefinition() == typeof(List<>) ||
                type.GetGenericTypeDefinition() == typeof(IEnumerable<>) ||
                type.GetGenericTypeDefinition() == typeof(ICollection<>) ||
                type.GetGenericTypeDefinition() == typeof(IList<>));
    }

    /// <summary>
    /// 非泛型的FromDocument方法，用于动态类型反序列化
    /// </summary>
    private static object FromDocument(Type targetType, BsonDocument document)
    {
        var method = typeof(AotBsonMapper).GetMethod("FromDocument", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
        var genericMethod = method?.MakeGenericMethod(targetType);
        return genericMethod?.Invoke(null, new[] { document }) ?? throw new InvalidOperationException($"Failed to create FromDocument method for type {targetType}");
    }

    /// <summary>
    /// 非泛型的ToDocument方法，用于动态类型序列化
    /// </summary>
    private static BsonDocument ToDocument(Type targetType, object entity)
    {
        var method = typeof(AotBsonMapper).GetMethod("ToDocument", System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static);
        var genericMethod = method?.MakeGenericMethod(targetType);
        return genericMethod?.Invoke(null, new[] { entity }) as BsonDocument ?? throw new InvalidOperationException($"Failed to create ToDocument method for type {targetType}");
    }

    /// <summary>
    /// 将BsonArray转换为集合类型
    /// </summary>
    private static object ConvertBsonArrayToCollection(BsonArray bsonArray, Type targetType)
    {
        if (bsonArray == null) throw new ArgumentNullException(nameof(bsonArray));
        if (targetType == null) throw new ArgumentNullException(nameof(targetType));

        // 处理数组类型
        if (targetType.IsArray)
        {
            return ConvertBsonArrayToArray(bsonArray, targetType);
        }

        // 处理泛型集合类型
        var genericArgs = targetType.GetGenericArguments();
        if (genericArgs.Length == 0)
        {
            throw new ArgumentException($"Target type {targetType.FullName} is not a generic collection type");
        }
        var elementType = genericArgs[0];

        // 创建List<T>的实例
        var listType = typeof(List<>).MakeGenericType(elementType);
        var list = Activator.CreateInstance(listType) as System.Collections.IList ?? throw new InvalidOperationException($"Failed to create list of type {listType}");

        // 遍历BsonArray的元素
        foreach (var bsonValue in bsonArray)
        {
            object? element;

            // 如果元素是复杂对象类型
            if (bsonValue is BsonDocument doc && IsComplexObjectType(elementType))
            {
                element = FromDocument(elementType, doc);
            }
            // 如果元素是集合类型（嵌套集合）
            else if (bsonValue is BsonArray nestedArray && IsCollectionType(elementType))
            {
                element = ConvertBsonArrayToCollection(nestedArray, elementType);
            }
            else
            {
                // 使用BsonConversion处理基本类型
                element = BsonConversion.FromBsonValue(bsonValue, elementType);
            }

            list.Add(element);
        }

        // 如果目标类型不是List<T>，尝试转换
        if (targetType != listType)
        {
            // 对于数组类型
            if (targetType.IsArray)
            {
                var array = Array.CreateInstance(elementType, list.Count);
                for (int i = 0; i < list.Count; i++)
                {
                    array.SetValue(list[i], i);
                }
                return array;
            }

            // 对于其他集合类型，尝试构造
            var constructor = targetType.GetConstructor(new[] { listType });
            if (constructor != null)
            {
                return constructor.Invoke(new[] { list });
            }

            // 如果无法转换，返回List<T>
            return list;
        }

        return list;
    }

    /// <summary>
    /// 将集合转换为BsonArray
    /// </summary>
    private static BsonArray ConvertCollectionToBsonArray(object collection)
    {
        if (collection == null) throw new ArgumentNullException(nameof(collection));

        BsonArray bsonArray = new BsonArray();

        // 如果是IEnumerable，遍历元素
        if (collection is System.Collections.IEnumerable enumerable)
        {
            foreach (var item in enumerable)
            {
                if (item == null)
                {
                    bsonArray = bsonArray.AddValue(BsonNull.Value);
                }
                // 如果元素是复杂对象，递归序列化
                else if (IsComplexObjectType(item.GetType()))
                {
                    var bsonDoc = ToDocument(item.GetType(), item);
                    bsonArray = bsonArray.AddValue(bsonDoc);
                }
                // 如果元素是集合，递归转换
                else if (IsCollectionType(item.GetType()))
                {
                    var nestedArray = ConvertCollectionToBsonArray(item);
                    bsonArray = bsonArray.AddValue(nestedArray);
                }
                else
                {
                    // 基本类型直接转换
                    bsonArray = bsonArray.AddValue(BsonConversion.ToBsonValue(item));
                }
            }
        }

        return bsonArray;
    }

    /// <summary>
    /// 判断是否为Dictionary类型
    /// </summary>
    private static bool IsDictionaryType(Type type)
    {
        if (type == null) return false;

        // 检查是否为Dictionary<K,V>
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Dictionary<,>))
        {
            return true;
        }

        // 检查是否实现了IDictionary接口
        return type.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>));
    }

    /// <summary>
    /// 将Dictionary转换为BsonDocument
    /// </summary>
    private static BsonDocument ConvertDictionaryToBsonDocument(object dictionary)
    {
        if (dictionary == null) throw new ArgumentNullException(nameof(dictionary));

        var bsonDocument = new BsonDocument();

        // 如果是IDictionary<string, object>，直接处理
        if (dictionary is System.Collections.Generic.IDictionary<string, object> stringDict)
        {
            foreach (var kvp in stringDict)
            {
                var bsonValue = kvp.Value != null ? BsonConversion.ToBsonValue(kvp.Value) : BsonNull.Value;
                bsonDocument = bsonDocument.Set(kvp.Key, bsonValue);
            }
            return bsonDocument;
        }

        // 使用反射处理其他Dictionary类型
        var dictType = dictionary.GetType();
        if (!IsDictionaryType(dictType))
        {
            throw new ArgumentException($"Object is not a dictionary: {dictType.FullName}");
        }

        // 获取Dictionary的键值类型
        var keyType = dictType.GetGenericArguments()[0];
        var valueType = dictType.GetGenericArguments()[1];

        // 只有键为string的Dictionary才支持转换为BsonDocument
        if (keyType != typeof(string))
        {
            throw new NotSupportedException($"Only Dictionary<string, T> is supported, but got Dictionary<{keyType.Name}, {valueType.Name}>");
        }

        // 获取Keys和Values属性
        var keysProperty = dictType.GetProperty("Keys");
        var valuesProperty = dictType.GetProperty("Values");
        var itemProperty = dictType.GetProperty("Item");

        if (keysProperty == null || valuesProperty == null || itemProperty == null)
        {
            throw new InvalidOperationException("Dictionary does not have expected Keys, Values, or Item properties");
        }

        var keys = keysProperty.GetValue(dictionary) as System.Collections.ICollection;
        var values = valuesProperty.GetValue(dictionary) as System.Collections.ICollection;

        if (keys == null || values == null)
        {
            throw new InvalidOperationException("Failed to get Dictionary keys or values");
        }

        var keysEnumerator = keys.GetEnumerator();
        var valuesEnumerator = values.GetEnumerator();

        while (keysEnumerator.MoveNext() && valuesEnumerator.MoveNext())
        {
            var key = keysEnumerator.Current as string;
            if (key == null)
            {
                throw new NotSupportedException("Dictionary keys must be strings for BSON serialization");
            }

            var value = valuesEnumerator.Current;
            var bsonValue = value != null ? BsonConversion.ToBsonValue(value) : BsonNull.Value;
            bsonDocument = bsonDocument.Set(key, bsonValue);
        }

        return bsonDocument;
    }

    /// <summary>
    /// 将BsonDocument转换为Dictionary
    /// </summary>
    private static object ConvertBsonDocumentToDictionary(BsonDocument bsonDocument, Type targetType)
    {
        if (bsonDocument == null) throw new ArgumentNullException(nameof(bsonDocument));
        if (targetType == null) throw new ArgumentNullException(nameof(targetType));

        if (!IsDictionaryType(targetType))
        {
            throw new ArgumentException($"Target type is not a dictionary: {targetType.FullName}");
        }

        // 获取Dictionary的键值类型
        var keyType = targetType.GetGenericArguments()[0];
        var valueType = targetType.GetGenericArguments()[1];

        // 只有键为string的Dictionary才支持
        if (keyType != typeof(string))
        {
            throw new NotSupportedException($"Only Dictionary<string, T> is supported, but got Dictionary<{keyType.Name}, {valueType.Name}>");
        }

        // 创建Dictionary实例
        var dictType = typeof(Dictionary<,>).MakeGenericType(keyType, valueType);
        var dictionary = Activator.CreateInstance(dictType) ?? throw new InvalidOperationException($"Failed to create dictionary of type {dictType.FullName}");

        // 使用反射获取Add方法
        var addMethod = dictType.GetMethod("Add", new[] { keyType, valueType });
        if (addMethod == null)
        {
            throw new InvalidOperationException("Dictionary does not have an Add method");
        }

        // 遍历BsonDocument的元素
        foreach (var element in bsonDocument)
        {
            var key = element.Key;
            var bsonValue = element.Value;

            // 转换值
            object? convertedValue;
            if (bsonValue == null || bsonValue.IsNull)
            {
                convertedValue = null;
            }
            else if (bsonValue is BsonDocument nestedDoc && IsComplexObjectType(valueType))
            {
                convertedValue = FromDocument(valueType, nestedDoc);
            }
            else if (bsonValue is BsonArray bsonArray && IsCollectionType(valueType))
            {
                convertedValue = ConvertBsonArrayToCollection(bsonArray, valueType);
            }
            else
            {
                convertedValue = BsonConversion.FromBsonValue(bsonValue, valueType);
            }

            // 添加到Dictionary
            addMethod.Invoke(dictionary, new[] { key, convertedValue });
        }

        return dictionary;
    }

    /// <summary>
    /// 将BsonArray转换为数组类型
    /// </summary>
    private static object ConvertBsonArrayToArray(BsonArray bsonArray, Type targetType)
    {
        if (bsonArray == null) throw new ArgumentNullException(nameof(bsonArray));
        if (targetType == null) throw new ArgumentNullException(nameof(targetType));
        if (!targetType.IsArray) throw new ArgumentException($"Target type {targetType.FullName} is not an array type");

        // 获取数组的元素类型
        var elementType = targetType.GetElementType() ?? throw new InvalidOperationException($"Cannot get element type for array {targetType.FullName}");

        // 创建数组
        var array = Array.CreateInstance(elementType, bsonArray.Count);

        // 遍历BsonArray的元素
        for (int i = 0; i < bsonArray.Count; i++)
        {
            var bsonValue = bsonArray[i];
            object? element;

            // 如果元素是复杂对象类型
            if (bsonValue is BsonDocument doc && IsComplexObjectType(elementType))
            {
                element = FromDocument(elementType, doc);
            }
            // 如果元素是集合类型（嵌套集合）
            else if (bsonValue is BsonArray nestedArray && IsCollectionType(elementType))
            {
                element = ConvertBsonArrayToCollection(nestedArray, elementType);
            }
            // 如果元素是数组类型（嵌套数组）
            else if (bsonValue is BsonArray nestedArray2 && elementType.IsArray)
            {
                element = ConvertBsonArrayToArray(nestedArray2, elementType);
            }
            else
            {
                // 使用BsonConversion处理基本类型
                element = BsonConversion.FromBsonValue(bsonValue, elementType);
            }

            array.SetValue(element, i);
        }

        return array;
    }
}
