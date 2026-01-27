using System.Collections.Generic;
using System.Text.Json;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.Metadata;

/// <summary>
/// 元数据文档包装类，用于正确序列化到数据库
/// </summary>
[Entity("metadata_document")]
public class MetadataDocument
{
    /// <summary>
    /// 文档ID
    /// </summary>
    [Id]
    public ObjectId Id { get; set; }

    /// <summary>
    /// 实体类型名称
    /// </summary>
    public string TypeName { get; set; } = string.Empty;

    /// <summary>
    /// 实体集合名称
    /// </summary>
    public string CollectionName { get; set; } = string.Empty;

    /// <summary>
    /// 实体显示名称
    /// </summary>
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>
    /// 实体描述
    /// </summary>
    public string Description { get; set; } = string.Empty;

    /// <summary>
    /// 属性元数据JSON字符串
    /// </summary>
    public string PropertiesJson { get; set; } = string.Empty;

    /// <summary>
    /// 创建时间
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.Now;

    /// <summary>
    /// 更新时间
    /// </summary>
    public DateTime UpdatedAt { get; set; } = DateTime.Now;

    /// <summary>
    /// 从EntityMetadata创建MetadataDocument
    /// </summary>
    /// <param name="metadata">实体元数据</param>
    /// <returns>元数据文档</returns>
    public static MetadataDocument FromEntityMetadata(EntityMetadata metadata)
    {
        var document = new MetadataDocument
        {
            Id = ObjectId.NewObjectId(),
            TypeName = metadata.TypeName,
            CollectionName = metadata.CollectionName,
            DisplayName = metadata.DisplayName,
            Description = metadata.Description ?? "",
            CreatedAt = DateTime.Now,
            UpdatedAt = DateTime.Now
        };

        document.PropertiesJson = JsonSerializer.Serialize(metadata.Properties, MetadataJsonContext.Default.ListPropertyMetadata);

        return document;
    }

    /// <summary>
    /// 转换为EntityMetadata
    /// </summary>
    /// <returns>实体元数据</returns>
    public EntityMetadata ToEntityMetadata()
    {
        var metadata = new EntityMetadata
        {
            TypeName = TypeName,
            CollectionName = CollectionName,
            DisplayName = DisplayName,
            Description = string.IsNullOrEmpty(Description) ? null : Description
        };

        // 从字符串解析属性列表
        if (!string.IsNullOrEmpty(PropertiesJson))
        {
            try
            {
                var properties = JsonSerializer.Deserialize<List<PropertyMetadata>>(PropertiesJson, MetadataJsonContext.Default.ListPropertyMetadata);
                if (properties != null)
                {
                    metadata.Properties.AddRange(properties);
                }
            }
            catch (JsonException)
            {
                metadata.Properties = new List<PropertyMetadata>();
            }
        }

        return metadata;
    }
}
