using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using TinyDb.Attributes;

namespace TinyDb.Metadata;

/// <summary>
/// 元数据提取器，通过反射从类型中提取元数据信息
/// </summary>
public static class MetadataExtractor
{
    /// <summary>
    /// 从类型中提取实体元数据
    /// </summary>
    /// <param name="entityType">实体类型</param>
    /// <returns>实体元数据</returns>
    public static EntityMetadata ExtractEntityMetadata([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type entityType)
    {
        if (entityType == null)
            throw new ArgumentNullException(nameof(entityType));

        var entityAttr = entityType.GetCustomAttribute<EntityAttribute>();
        var collectionName = entityAttr?.Name ?? entityType.Name;

        var metadata = new EntityMetadata
        {
            TypeName = GetEntityTypeName(entityType),
            CollectionName = collectionName,
            DisplayName = GetEntityDisplayName(entityType),
            Description = GetEntityDescription(entityType)
        };

        // 提取属性元数据
        var properties = entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite)
            .Select(ExtractPropertyMetadata)
            .OrderBy(p => p.Order)
            .ToList();

        metadata.Properties = properties;
        return metadata;
    }

    /// <summary>
    /// 从属性中提取属性元数据
    /// </summary>
    /// <param name="propertyInfo">属性信息</param>
    /// <returns>属性元数据</returns>
    public static PropertyMetadata ExtractPropertyMetadata(PropertyInfo propertyInfo)
    {
        if (propertyInfo == null)
            throw new ArgumentNullException(nameof(propertyInfo));

        var metadataAttr = propertyInfo.GetCustomAttribute<PropertyMetadataAttribute>();
        var foreignKeyAttr = propertyInfo.GetCustomAttribute<ForeignKeyAttribute>();

        return new PropertyMetadata
        {
            PropertyName = propertyInfo.Name,
            PropertyType = propertyInfo.PropertyType.FullName ?? propertyInfo.PropertyType.Name,
            DisplayName = metadataAttr?.DisplayName ?? propertyInfo.Name,
            Description = metadataAttr?.Description,
            Order = metadataAttr?.Order ?? 0,
            Required = metadataAttr?.Required ?? false,
            ForeignKeyCollection = foreignKeyAttr?.CollectionName
        };
    }

    /// <summary>
    /// 获取实体显示名称
    /// </summary>
    private static string GetEntityDisplayName(Type entityType)
    {
        var entityAttrs = entityType.GetCustomAttributes<EntityMetadataAttribute>();
        return entityAttrs.FirstOrDefault(a => !string.IsNullOrEmpty(a.DisplayName))?.DisplayName ?? entityType.Name;
    }

    /// <summary>
    /// 获取实体描述
    /// </summary>
    private static string? GetEntityDescription(Type entityType)
    {
        var entityAttrs = entityType.GetCustomAttributes<EntityMetadataAttribute>();
        return entityAttrs.FirstOrDefault(a => !string.IsNullOrEmpty(a.Description))?.Description;
    }

    internal static string GetEntityTypeName(Type entityType)
    {
        return entityType.FullName ?? entityType.Name;
    }
}
