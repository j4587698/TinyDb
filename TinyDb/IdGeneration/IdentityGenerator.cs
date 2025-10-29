using System;
using System.Collections.Concurrent;
using System.Reflection;
using TinyDb.Bson;

namespace TinyDb.IdGeneration;

/// <summary>
/// 自增ID生成器
/// </summary>
public class IdentityGenerator : IIdGenerator
{
    private readonly ConcurrentDictionary<string, long> _sequences = new();

    public BsonValue GenerateId(Type entityType, PropertyInfo idProperty, string? sequenceName = null)
    {
        var key = sequenceName ?? $"{entityType.Name}_{idProperty.Name}";
        var nextValue = _sequences.AddOrUpdate(key, 1, (k, v) => v + 1);

        if (idProperty.PropertyType == typeof(int))
        {
            return new BsonInt32((int)Math.Min(nextValue, int.MaxValue));
        }

        if (idProperty.PropertyType == typeof(long))
        {
            return new BsonInt64(nextValue);
        }

        throw new InvalidOperationException($"Identity generator does not support type {idProperty.PropertyType.Name}");
    }

    public bool Supports(Type idType)
    {
        return idType == typeof(int) || idType == typeof(long);
    }
}