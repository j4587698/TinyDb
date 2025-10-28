using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using SimpleDb.Attributes;

namespace SimpleDb.Serialization;

/// <summary>
/// 缓存实体类型的元数据，避免在AOT环境中频繁反射。
/// </summary>
internal static class EntityMetadata<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>
    where T : class, new()
{
    private static readonly Lazy<PropertyInfo[]> _properties = new(() =>
        typeof(T)
            .GetProperties(BindingFlags.Instance | BindingFlags.Public)
            .Where(property => property.CanRead && property.GetMethod is { IsStatic: false })
            .Where(property => property.GetCustomAttribute<BsonIgnoreAttribute>() == null)
            .ToArray());

    private static readonly Lazy<IReadOnlyDictionary<string, PropertyInfo>> _propertyMap = new(() =>
        _properties.Value.ToDictionary(property => property.Name, StringComparer.Ordinal));

    private static readonly Lazy<PropertyInfo?> _idProperty = new(ResolveIdProperty);

    public static IReadOnlyList<PropertyInfo> Properties => _properties.Value;

    public static PropertyInfo? IdProperty => _idProperty.Value;

    public static bool TryGetProperty(string propertyName, [NotNullWhen(true)] out PropertyInfo? propertyInfo)
    {
        return _propertyMap.Value.TryGetValue(propertyName, out propertyInfo);
    }

    private static PropertyInfo? ResolveIdProperty()
    {
        var type = typeof(T);
        var map = _propertyMap.Value;

        var entityAttribute = type.GetCustomAttribute<EntityAttribute>();
        if (!string.IsNullOrWhiteSpace(entityAttribute?.IdProperty) &&
            map.TryGetValue(entityAttribute.IdProperty!, out var specified))
        {
            return specified;
        }

        foreach (var property in map.Values)
        {
            if (property.GetCustomAttribute<IdAttribute>() != null)
            {
                return property;
            }
        }

        foreach (var candidate in new[] { "Id", "_id", "ID" })
        {
            if (map.TryGetValue(candidate, out var property))
            {
                return property;
            }
        }

        return null;
    }
}
