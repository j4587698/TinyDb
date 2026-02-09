using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Serialization;

namespace TinyDb.References;

/// <summary>
/// DbRef 序列化帮助类
/// </summary>
public static class DbRefSerializer
{
    /// <summary>
    /// 获取类型中所有带有 ForeignKey 属性的属性
    /// </summary>
    public static List<(PropertyInfo Property, ForeignKeyAttribute Attribute)> GetRefProperties(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type type)
    {
        var result = new List<(PropertyInfo, ForeignKeyAttribute)>();
        
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var attr = property.GetCustomAttribute<ForeignKeyAttribute>();
            if (attr != null)
            {
                result.Add((property, attr));
            }
        }
        
        return result;
    }

    /// <summary>
    /// 获取实体的 ID 值（AOT 安全）
    /// </summary>
    public static BsonValue GetEntityId(object entity)
    {
        if (entity == null) return BsonNull.Value;
        // 使用 AOT 适配器获取 ID，避免反射
        return AotBsonMapper.GetId(entity);
    }

    /// <summary>
    /// 将对象序列化为 DbRef 格式
    /// </summary>
    [Obsolete("Use GetEntityId and manual BsonDocument construction instead.")]
    public static BsonValue SerializeToDbRef(object? value, string collectionName)
    {
        if (value == null)
        {
            return BsonNull.Value;
        }

        var idValue = GetEntityId(value);
        if (idValue == BsonNull.Value)
        {
            return BsonNull.Value;
        }

        // 创建 DbRef 文档
        var dbRef = new BsonDocument()
            .Set("$ref", new BsonString(collectionName))
            .Set("$id", idValue);

        return dbRef;
    }
}

/// <summary>
/// 支持 Include 的查询构建器
/// </summary>
/// <typeparam name="T">文档类型</typeparam>
public class IncludeQueryBuilder<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> where T : class, new()
{
    private readonly TinyDbEngine _engine;
    private readonly string _collectionName;
    private readonly List<string> _includes = new();

    public IncludeQueryBuilder(TinyDbEngine engine, string collectionName)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
    }

    /// <summary>
    /// 添加要包含的引用属性
    /// </summary>
    public IncludeQueryBuilder<T> Include<TProperty>(Expression<Func<T, TProperty>> expression)
    {
        if (expression.Body is MemberExpression memberExpr)
        {
            _includes.Add(memberExpr.Member.Name);
        }
        return this;
    }

    /// <summary>
    /// 添加要包含的引用属性（通过路径）
    /// </summary>
    public IncludeQueryBuilder<T> Include(string path)
    {
        if (!string.IsNullOrWhiteSpace(path))
        {
            _includes.Add(path);
        }
        return this;
    }

    /// <summary>
    /// 获取所有文档并加载引用
    /// </summary>
    public IEnumerable<T> FindAll()
    {
        var collection = _engine.GetCollection<T>(_collectionName);
        var documents = collection.FindAll();
        
        foreach (var doc in documents)
        {
            LoadReferences(doc);
            yield return doc;
        }
    }

    /// <summary>
    /// 根据 ID 获取文档并加载引用
    /// </summary>
    public T? FindById(BsonValue id)
    {
        var collection = _engine.GetCollection<T>(_collectionName);
        var doc = collection.FindById(id);
        
        if (doc != null)
        {
            LoadReferences(doc);
        }
        
        return doc;
    }

    /// <summary>
    /// 根据条件查找文档并加载引用
    /// </summary>
    public IEnumerable<T> Find(Expression<Func<T, bool>> predicate)
    {
        var collection = _engine.GetCollection<T>(_collectionName);
        var documents = collection.Find(predicate);
        
        foreach (var doc in documents)
        {
            LoadReferences(doc);
            yield return doc;
        }
    }

    private void LoadReferences(T entity)
    {
        if (_includes.Count == 0)
        {
            return;
        }

        var type = typeof(T);
        
        foreach (var includePath in _includes)
        {
            var property = type.GetProperty(includePath);
            if (property == null) continue;

            var foreignKeyAttr = property.GetCustomAttribute<ForeignKeyAttribute>();
            if (foreignKeyAttr == null) continue;

            // 获取当前属性值（可能是 DbRef 文档或已加载的对象）
            var currentValue = property.GetValue(entity);
            if (currentValue == null) continue;

            // 如果已经是正确类型的对象，跳过
            if (property.PropertyType.IsAssignableFrom(currentValue.GetType()) && 
                currentValue.GetType() != typeof(BsonDocument))
            {
                continue;
            }

            // 从 DbRef 加载实际对象
            // 这里需要根据实际情况实现引用解析逻辑
            // 目前保留为基本实现
        }
    }
}
