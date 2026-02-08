using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// 非泛型的实体适配器接口，用于AOT兼容的类型查找
/// </summary>
public interface IAotEntityAdapter
{
    BsonDocument ToDocumentUntyped(object entity);
    object FromDocumentUntyped(BsonDocument document);
    
    /// <summary>
    /// 从文档转换为对象（返回 object 类型）
    /// </summary>
    object? FromDocumentObject(BsonDocument document);
    
    /// <summary>
    /// 创建该类型的数组（AOT 兼容）
    /// </summary>
    /// <param name="length">数组长度</param>
    /// <returns>新创建的数组</returns>
    Array CreateArray(int length);
    
    /// <summary>
    /// 创建该类型的 List 集合（AOT 兼容）
    /// </summary>
    /// <returns>新创建的 List</returns>
    System.Collections.IList CreateList();
    
    /// <summary>
    /// 创建该类型的 List 集合，并用指定元素初始化（AOT 兼容）
    /// </summary>
    /// <param name="items">要添加到集合的元素</param>
    /// <returns>包含指定元素的 List</returns>
    System.Collections.IList CreateListFrom(IEnumerable<object> items);
}

internal interface IAotEntityPropertyAccessor
{
    object? GetPropertyValueUntyped(object entity, string propertyName);
}

/// <summary>
/// 表示针对特定实体类型由源生成器生成的AOT辅助适配器
/// </summary>
public sealed class AotEntityAdapter<T> : IAotEntityAdapter, IAotEntityPropertyAccessor
{
    public Func<T, BsonDocument> ToDocument { get; }
    public Func<BsonDocument, T> FromDocument { get; }
    public Func<T, BsonValue> GetId { get; }
    public Action<T, BsonValue> SetId { get; }
    public Func<T, bool> HasValidId { get; }
    public Func<T, string, object?> GetPropertyValue { get; }

    public AotEntityAdapter(
        Func<T, BsonDocument> toDocument,
        Func<BsonDocument, T> fromDocument,
        Func<T, BsonValue> getId,
        Action<T, BsonValue> setId,
        Func<T, bool> hasValidId,
        Func<T, string, object?> getPropertyValue)
    {
        ToDocument = toDocument ?? throw new ArgumentNullException(nameof(toDocument));
        FromDocument = fromDocument ?? throw new ArgumentNullException(nameof(fromDocument));
        GetId = getId ?? throw new ArgumentNullException(nameof(getId));
        SetId = setId ?? throw new ArgumentNullException(nameof(setId));
        HasValidId = hasValidId ?? throw new ArgumentNullException(nameof(hasValidId));
        GetPropertyValue = getPropertyValue ?? throw new ArgumentNullException(nameof(getPropertyValue));
    }

    // 实现非泛型接口
    public BsonDocument ToDocumentUntyped(object entity) => ToDocument((T)entity);
    public object FromDocumentUntyped(BsonDocument document) => FromDocument(document)!;
    public object? FromDocumentObject(BsonDocument document) => FromDocument(document);
    object? IAotEntityPropertyAccessor.GetPropertyValueUntyped(object entity, string propertyName) => GetPropertyValue((T)entity, propertyName);
    
    /// <summary>
    /// 创建 T 类型的数组（AOT 兼容）
    /// </summary>
    public Array CreateArray(int length) => new T[length];
    
    /// <summary>
    /// 创建 List&lt;T&gt;（AOT 兼容）
    /// </summary>
    public System.Collections.IList CreateList() => new List<T>();
    
    /// <summary>
    /// 创建 List&lt;T&gt; 并用指定元素初始化（AOT 兼容）
    /// </summary>
    public System.Collections.IList CreateListFrom(IEnumerable<object> items)
    {
        var list = new List<T>();
        foreach (var item in items)
        {
            if (item is T typedItem)
            {
                list.Add(typedItem);
            }
            else if (item == null)
            {
                list.Add(default!);
            }
        }
        return list;
    }
}

/// <summary>
/// 管理AOT辅助适配器的注册与查询
/// </summary>
public static partial class AotHelperRegistry
{
    private static readonly ConcurrentDictionary<Type, object> _adapters = new();

    public static bool TryGetAdapter<T>([NotNullWhen(true)] out AotEntityAdapter<T>? adapter)
    {
        if (_adapters.TryGetValue(typeof(T), out var raw))
        {
            adapter = (AotEntityAdapter<T>)raw;
            return true;
        }

        adapter = null;
        return false;
    }

    /// <summary>
    /// 尝试根据类型获取非泛型适配器
    /// </summary>
    /// <param name="entityType">实体类型</param>
    /// <param name="adapter">适配器</param>
    /// <returns>是否找到适配器</returns>
    internal static bool TryGetUntypedAdapter(Type entityType, [NotNullWhen(true)] out IAotEntityAdapter? adapter)
    {
        if (_adapters.TryGetValue(entityType, out var raw) && raw is IAotEntityAdapter typedAdapter)
        {
            adapter = typedAdapter;
            return true;
        }

        adapter = null;
        return false;
    }

    /// <summary>
    /// 尝试根据类型获取适配器（公共方法）
    /// </summary>
    /// <param name="entityType">实体类型</param>
    /// <param name="adapter">适配器</param>
    /// <returns>是否找到适配器</returns>
    public static bool TryGetAdapter(Type entityType, [NotNullWhen(true)] out IAotEntityAdapter? adapter)
    {
        return TryGetUntypedAdapter(entityType, out adapter);
    }

    public static void Register<T>(AotEntityAdapter<T> adapter)
    {
        _adapters[typeof(T)] = adapter;
    }

    public static void Clear()
    {
        _adapters.Clear();
    }
}
