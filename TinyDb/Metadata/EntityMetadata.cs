namespace TinyDb.Metadata;

/// <summary>
/// 实体元数据信息
/// </summary>
public class EntityMetadata
{
    /// <summary>
    /// 实体类型名称
    /// </summary>
    public string TypeName { get; set; } = string.Empty;

    /// <summary>
    /// 实体显示名称
    /// </summary>
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>
    /// 实体描述
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// 属性元数据集合
    /// </summary>
    public List<PropertyMetadata> Properties { get; set; } = new();
}

/// <summary>
/// 属性元数据信息
/// </summary>
public class PropertyMetadata
{
    /// <summary>
    /// 属性名称
    /// </summary>
    public string PropertyName { get; set; } = string.Empty;

    /// <summary>
    /// 属性类型完整名称
    /// </summary>
    public string PropertyType { get; set; } = string.Empty;

    /// <summary>
    /// 属性显示名称
    /// </summary>
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>
    /// 属性描述
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// 显示顺序
    /// </summary>
    public int Order { get; set; } = 0;

    /// <summary>
    /// 是否必需
    /// </summary>
    public bool Required { get; set; } = false;
}