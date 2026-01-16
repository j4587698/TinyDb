using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// 表示针对特定实体类型由源生成器生成的AOT辅助适配器
/// </summary>
public sealed class AotEntityAdapter<T>
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
}

/// <summary>
/// 管理AOT辅助适配器的注册与查询
/// </summary>
public static partial class AotHelperRegistry
{
    private static readonly ConcurrentDictionary<Type, object> _adapters = new();

    internal static bool TryGetAdapter<T>([NotNullWhen(true)] out AotEntityAdapter<T>? adapter)
    {
        if (_adapters.TryGetValue(typeof(T), out var raw))
        {
            adapter = (AotEntityAdapter<T>)raw;
            return true;
        }

        adapter = null;
        return false;
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
