using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using TinyDb.Attributes;

namespace TinyDb.Metadata;

/// <summary>
/// 静态元数据注册表 - 解决 AOT 下反射不可用的核心
/// </summary>
public static class MetadataRegistry
{
    private static readonly ConcurrentDictionary<Type, EntityMetadata> _staticMetadata = new();

    /// <summary>
    /// 预先注册元数据 (Source Generator 使用)
    /// </summary>
    public static void Register(Type type, EntityMetadata metadata)
    {
        if (type == null) throw new ArgumentNullException(nameof(type));
        if (metadata == null) throw new ArgumentNullException(nameof(metadata));
        _staticMetadata[type] = metadata;
    }

    public static bool TryGet(Type type, [NotNullWhen(true)] out EntityMetadata? metadata)
    {
        if (type == null) throw new ArgumentNullException(nameof(type));
        return _staticMetadata.TryGetValue(type, out metadata);
    }
}

public static class MetadataExtractor
{
    public static EntityMetadata ExtractEntityMetadata([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type type)
    {
        // 1. 优先从静态注册表中获取 (AOT 路径)
        if (type == null) throw new ArgumentNullException(nameof(type));
        if (MetadataRegistry.TryGet(type, out var staticMeta)) return staticMeta;

        // 2. 回退到反射 (JIT 路径)
        var entityAttr = type.GetCustomAttribute<EntityAttribute>();
        var collectionName = entityAttr?.Name ?? type.Name;
        var metadata = new EntityMetadata
        {
            TypeName = GetEntityTypeName(type),
            CollectionName = collectionName,
            DisplayName = GetEntityDisplayName(type),
            Description = GetEntityDescription(type)
        };

        var extracted = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite && p.GetIndexParameters().Length == 0)
            .Select(p => (Property: p, Metadata: ExtractPropertyMetadata(p)))
            .ToList();

        var idPropertyName = DetermineIdPropertyName(entityAttr, extracted.Select(p => p.Property));
        if (!string.IsNullOrWhiteSpace(idPropertyName))
        {
            foreach (var item in extracted)
            {
                item.Metadata.IsPrimaryKey = string.Equals(item.Property.Name, idPropertyName, StringComparison.Ordinal);
            }
        }

        metadata.Properties = extracted
            .Select(p => p.Metadata)
            .OrderBy(p => p.Order)
            .ToList();
        return metadata;
    }

    public static PropertyMetadata ExtractPropertyMetadata(PropertyInfo propertyInfo)
    {
        if (propertyInfo == null) throw new ArgumentNullException(nameof(propertyInfo));

        var metadataAttr = propertyInfo.GetCustomAttribute<PropertyMetadataAttribute>();
        var foreignKeyAttr = propertyInfo.GetCustomAttribute<ForeignKeyAttribute>();
        var idAttr = propertyInfo.GetCustomAttribute<IdAttribute>();

        return new PropertyMetadata
        {
            PropertyName = propertyInfo.Name,
            PropertyType = ClrTypeName.GetStableName(propertyInfo.PropertyType),
            DisplayName = metadataAttr?.DisplayName ?? propertyInfo.Name,
            Description = metadataAttr?.Description,
            Order = metadataAttr?.Order ?? 0,
            Required = metadataAttr?.Required ?? false,
            ForeignKeyCollection = foreignKeyAttr?.CollectionName,
            IsPrimaryKey = idAttr != null
        };
    }

    private static string GetEntityDisplayName(Type entityType)
    {
        var entityAttrs = entityType.GetCustomAttributes<EntityMetadataAttribute>();
        return entityAttrs.FirstOrDefault(a => !string.IsNullOrEmpty(a.DisplayName))?.DisplayName ?? entityType.Name;
    }

    private static string? GetEntityDescription(Type entityType)
    {
        var entityAttrs = entityType.GetCustomAttributes<EntityMetadataAttribute>();
        return entityAttrs.FirstOrDefault(a => !string.IsNullOrEmpty(a.Description))?.Description;
    }

    internal static string GetEntityTypeName(Type entityType)
    {
        return entityType.FullName ?? entityType.Name;
    }

    private static string? DetermineIdPropertyName(EntityAttribute? entityAttr, IEnumerable<PropertyInfo> properties)
    {
        var list = properties?.ToList() ?? new List<PropertyInfo>();

        var specified = entityAttr?.IdProperty;
        if (!string.IsNullOrWhiteSpace(specified) &&
            list.Any(p => string.Equals(p.Name, specified, StringComparison.Ordinal)))
        {
            return specified;
        }

        foreach (var prop in list)
        {
            if (prop.GetCustomAttribute<IdAttribute>() != null)
            {
                return prop.Name;
            }
        }

        var standardIdNames = new[] { "Id", "_id", "ID" };
        foreach (var idName in standardIdNames)
        {
            if (list.Any(p => string.Equals(p.Name, idName, StringComparison.Ordinal)))
            {
                return idName;
            }
        }

        return null;
    }
}
