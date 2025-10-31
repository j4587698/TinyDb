namespace TinyDb.Metadata;

/// <summary>
/// 元数据查询API - 核心库提供的元数据查询接口
/// 这部分是核心库的职责，仅提供基础信息查询
/// </summary>
public static class MetadataApi
{
    /// <summary>
    /// 获取实体类型的所有属性信息（名称和类型）
    /// </summary>
    /// <param name="metadataManager">元数据管理器</param>
    /// <param name="entityType">实体类型</param>
    /// <returns>属性信息列表</returns>
    public static List<(string PropertyName, string PropertyType)> GetEntityProperties(
        this MetadataManager metadataManager,
        Type entityType)
    {
        var metadata = metadataManager.GetEntityMetadata(entityType);
        if (metadata == null)
            return new List<(string, string)>();

        return metadata.Properties
            .Select(p => (p.PropertyName, p.PropertyType))
            .ToList();
    }

    /// <summary>
    /// 获取实体类型的显示名称
    /// </summary>
    /// <param name="metadataManager">元数据管理器</param>
    /// <param name="entityType">实体类型</param>
    /// <returns>显示名称</returns>
    public static string GetEntityDisplayName(
        this MetadataManager metadataManager,
        Type entityType)
    {
        var metadata = metadataManager.GetEntityMetadata(entityType);
        return metadata?.DisplayName ?? entityType.Name;
    }

    /// <summary>
    /// 获取属性的显示名称
    /// </summary>
    /// <param name="metadataManager">元数据管理器</param>
    /// <param name="entityType">实体类型</param>
    /// <param name="propertyName">属性名称</param>
    /// <returns>属性显示名称</returns>
    public static string GetPropertyDisplayName(
        this MetadataManager metadataManager,
        Type entityType,
        string propertyName)
    {
        var metadata = metadataManager.GetEntityMetadata(entityType);
        var property = metadata?.Properties.FirstOrDefault(p => p.PropertyName == propertyName);
        return property?.DisplayName ?? propertyName;
    }

    /// <summary>
    /// 获取属性的类型信息
    /// </summary>
    /// <param name="metadataManager">元数据管理器</param>
    /// <param name="entityType">实体类型</param>
    /// <param name="propertyName">属性名称</param>
    /// <returns>属性类型名称</returns>
    public static string? GetPropertyType(
        this MetadataManager metadataManager,
        Type entityType,
        string propertyName)
    {
        var metadata = metadataManager.GetEntityMetadata(entityType);
        var property = metadata?.Properties.FirstOrDefault(p => p.PropertyName == propertyName);
        return property?.PropertyType;
    }

    /// <summary>
    /// 检查属性是否必需
    /// </summary>
    /// <param name="metadataManager">元数据管理器</param>
    /// <param name="entityType">实体类型</param>
    /// <param name="propertyName">属性名称</param>
    /// <returns>是否必需</returns>
    public static bool IsPropertyRequired(
        this MetadataManager metadataManager,
        Type entityType,
        string propertyName)
    {
        var metadata = metadataManager.GetEntityMetadata(entityType);
        var property = metadata?.Properties.FirstOrDefault(p => p.PropertyName == propertyName);
        return property?.Required ?? false;
    }

    /// <summary>
    /// 获取属性的显示顺序
    /// </summary>
    /// <param name="metadataManager">元数据管理器</param>
    /// <param name="entityType">实体类型</param>
    /// <param name="propertyName">属性名称</param>
    /// <returns>显示顺序</returns>
    public static int GetPropertyOrder(
        this MetadataManager metadataManager,
        Type entityType,
        string propertyName)
    {
        var metadata = metadataManager.GetEntityMetadata(entityType);
        var property = metadata?.Properties.FirstOrDefault(p => p.PropertyName == propertyName);
        return property?.Order ?? 0;
    }
}