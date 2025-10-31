using System.Diagnostics.CodeAnalysis;

namespace TinyDb.Metadata;

/// <summary>
/// 实体元数据特性，用于标记需要元数据的实体类型
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = true)]
public class EntityMetadataAttribute : Attribute
{
    /// <summary>
    /// 实体显示名称
    /// </summary>
    public string? DisplayName { get; set; }

    /// <summary>
    /// 实体描述
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// 仅显示名称的构造函数
    /// </summary>
    public EntityMetadataAttribute()
    {
    }

    /// <summary>
    /// 指定显示名称的构造函数
    /// </summary>
    public EntityMetadataAttribute(string displayName)
    {
        DisplayName = displayName ?? throw new ArgumentNullException(nameof(displayName));
    }
}

/// <summary>
/// 属性元数据特性，用于标记需要元数据的属性
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false)]
public class PropertyMetadataAttribute : Attribute
{
    /// <summary>
    /// 属性显示名称
    /// </summary>
    public string DisplayName { get; }

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

    public PropertyMetadataAttribute(string displayName)
    {
        DisplayName = displayName ?? throw new ArgumentNullException(nameof(displayName));
    }
}