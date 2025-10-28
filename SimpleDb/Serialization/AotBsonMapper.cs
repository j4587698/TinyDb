using System.Collections.Concurrent;
using System.Reflection;
using SimpleDb.Bson;

namespace SimpleDb.Serialization;

/// <summary>
/// AOT兼容的BSON映射器 - 使用源代码生成器生成的代码
/// </summary>
public static class AotBsonMapper
{
    /// <summary>
    /// 将实体转换为BSON文档（AOT兼容）
    /// </summary>
    /// <typeparam name="T">实体类型</typeparam>
    /// <param name="entity">实体实例</param>
    /// <returns>BSON文档</returns>
    public static BsonDocument ToDocument<T>(T entity)
        where T : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // 使用源代码生成器生成的AOT兼容方法
        var helperTypeName = $"{typeof(T).Namespace}.{typeof(T).Name}AotHelper";
        var helperType = typeof(T).Assembly.GetType(helperTypeName);
        if (helperType != null)
        {
            var method = helperType.GetMethod("ToDocument", new[] { typeof(T) });
            if (method != null)
            {
                return (BsonDocument)method.Invoke(null, new object[] { entity })!;
            }
        }

        // 如果没有生成的代码，回退到基本实现
        return FallbackToDocument(entity);
    }

    /// <summary>
    /// 从BSON文档创建实体（AOT兼容）
    /// </summary>
    /// <typeparam name="T">实体类型</typeparam>
    /// <param name="document">BSON文档</param>
    /// <returns>实体实例</returns>
    public static T FromDocument<T>(BsonDocument document)
        where T : class, new()
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        // 使用源代码生成器生成的AOT兼容方法
        var helperTypeName = $"{typeof(T).Namespace}.{typeof(T).Name}AotHelper";
        var helperType = typeof(T).Assembly.GetType(helperTypeName);
        if (helperType != null)
        {
            var method = helperType.GetMethod("FromDocument", new[] { typeof(BsonDocument) });
            if (method != null)
            {
                return (T)method.Invoke(null, new object[] { document })!;
            }
        }

        // 如果没有生成的代码，回退到基本实现
        return FallbackFromDocument<T>(document);
    }

    /// <summary>
    /// 获取实体的ID值（AOT兼容）
    /// </summary>
    /// <typeparam name="T">实体类型</typeparam>
    /// <param name="entity">实体实例</param>
    /// <returns>ID值</returns>
    public static BsonValue GetId<T>(T entity)
        where T : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // 使用源代码生成器生成的AOT兼容方法
        var helperTypeName = $"{typeof(T).Namespace}.{typeof(T).Name}AotHelper";
        var helperType = typeof(T).Assembly.GetType(helperTypeName);
        if (helperType != null)
        {
            var method = helperType.GetMethod("GetId", new[] { typeof(T) });
            if (method != null)
            {
                return (BsonValue)method.Invoke(null, new object[] { entity })!;
            }
        }

        // 如果没有生成的代码，返回null
        return BsonNull.Value;
    }

    /// <summary>
    /// 设置实体的ID值（AOT兼容）
    /// </summary>
    /// <typeparam name="T">实体类型</typeparam>
    /// <param name="entity">实体实例</param>
    /// <param name="id">ID值</param>
    public static void SetId<T>(T entity, BsonValue id)
        where T : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // 使用源代码生成器生成的AOT兼容方法
        var helperTypeName = $"{typeof(T).Namespace}.{typeof(T).Name}AotHelper";
        var helperType = typeof(T).Assembly.GetType(helperTypeName);
        if (helperType != null)
        {
            var method = helperType.GetMethod("SetId", new[] { typeof(T), typeof(BsonValue) });
            if (method != null)
            {
                method.Invoke(null, new object[] { entity, id });
                return;
            }
        }
    }

    /// <summary>
    /// 获取属性值（AOT兼容）
    /// </summary>
    /// <typeparam name="T">实体类型</typeparam>
    /// <param name="entity">实体实例</param>
    /// <param name="propertyName">属性名称</param>
    /// <returns>属性值</returns>
    public static object? GetPropertyValue<T>(T entity, string propertyName)
        where T : class
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        // 使用源代码生成器生成的AOT兼容方法
        var helperTypeName = $"{typeof(T).Namespace}.{typeof(T).Name}AotHelper";
        var helperType = typeof(T).Assembly.GetType(helperTypeName);
        if (helperType != null)
        {
            var method = helperType.GetMethod("GetPropertyValue", new[] { typeof(T), typeof(string) });
            if (method != null)
            {
                return method.Invoke(null, new object[] { entity, propertyName });
            }
        }

        // 如果没有生成的代码，回退到反射
        var property = typeof(T).GetProperty(propertyName);
        return property?.GetValue(entity);
    }

    /// <summary>
    /// 回退的序列化实现（仅用于未生成代码的情况）
    /// </summary>
    private static BsonDocument FallbackToDocument<T>(T entity)
        where T : class
    {
        var document = new BsonDocument();
        var type = typeof(T);

        foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var value = prop.GetValue(entity);
            if (value != null)
            {
                document[prop.Name] = ConvertValueToBson(value);
            }
        }

        return document;
    }

    /// <summary>
    /// 回退的反序列化实现（仅用于未生成代码的情况）
    /// </summary>
    private static T FallbackFromDocument<T>(BsonDocument document)
        where T : class, new()
    {
        var entity = new T();
        var type = typeof(T);

        foreach (var kvp in document)
        {
            var prop = type.GetProperty(kvp.Key);
            if (prop != null && prop.CanWrite)
            {
                var value = ConvertBsonToValue(kvp.Value, prop.PropertyType);
                prop.SetValue(entity, value);
            }
        }

        return entity;
    }

    /// <summary>
    /// 将值转换为BSON值
    /// </summary>
    private static BsonValue ConvertValueToBson(object value)
    {
        return value switch
        {
            string str => new BsonString(str),
            int i => new BsonInt32(i),
            long l => new BsonInt64(l),
            double d => new BsonDouble(d),
            float f => new BsonDouble(f),
            decimal dec => new BsonDouble((double)dec),
            bool b => new BsonBoolean(b),
            DateTime dt => new BsonDateTime(dt),
            Guid guid => new BsonBinary(guid.ToByteArray()),
            ObjectId oid => new BsonObjectId(oid),
            null => BsonNull.Value,
            _ => new BsonString(value.ToString()!)
        };
    }

    /// <summary>
    /// 将BSON值转换为指定类型
    /// </summary>
    private static object? ConvertBsonToValue(BsonValue bsonValue, Type targetType)
    {
        if (bsonValue.IsNull) return null;

        return targetType switch
        {
            var t when t == typeof(string) => bsonValue is BsonString str ? str.Value : bsonValue.ToString(),
            var t when t == typeof(int) => bsonValue is BsonInt32 i32 ? i32.Value : Convert.ToInt32(bsonValue.ToString()),
            var t when t == typeof(long) => bsonValue is BsonInt64 i64 ? i64.Value : Convert.ToInt64(bsonValue.ToString()),
            var t when t == typeof(double) => bsonValue is BsonDouble dbl ? dbl.Value : Convert.ToDouble(bsonValue.ToString()),
            var t when t == typeof(float) => bsonValue is BsonDouble dbl ? (float)dbl.Value : Convert.ToSingle(bsonValue.ToString()),
            var t when t == typeof(decimal) => bsonValue is BsonDouble dbl ? (decimal)dbl.Value : Convert.ToDecimal(bsonValue.ToString()),
            var t when t == typeof(bool) => bsonValue is BsonBoolean bl ? bl.Value : Convert.ToBoolean(bsonValue.ToString()),
            var t when t == typeof(DateTime) => bsonValue is BsonDateTime dt ? dt.Value : Convert.ToDateTime(bsonValue.ToString()),
            var t when t == typeof(Guid) => bsonValue is BsonBinary bin ? new Guid(bin.Bytes) : Guid.NewGuid(),
            var t when t == typeof(ObjectId) => bsonValue is BsonObjectId oid ? oid.Value : ObjectId.Empty,
            _ => bsonValue.ToString()
        };
    }
}