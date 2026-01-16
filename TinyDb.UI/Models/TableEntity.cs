using System;
using System.Collections.Generic;
using System.Linq;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.UI.Models;

/// <summary>
/// 表结构实体类 - 使用TinyDb的实体系统
/// </summary>
[Entity("__table_structures")]
public class TableEntity
{
    /// <summary>
    /// 表名（作为ID）
    /// </summary>
    [Id]
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// 显示名称
    /// </summary>
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>
    /// 描述
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// 创建时间
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.Now;

    /// <summary>
    /// 更新时间
    /// </summary>
    public DateTime UpdatedAt { get; set; } = DateTime.Now;

    /// <summary>
    /// 字段定义JSON
    /// </summary>
    public string FieldsJson { get; set; } = "[]";
}

/// <summary>
/// 动态表数据实体基类
/// </summary>
public class DynamicTableEntityBase
{
    /// <summary>
    /// 文档ID
    /// </summary>
    [Id]
    public string Id { get; set; } = string.Empty;

    /// <summary>
    /// 创建时间
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.Now;

    /// <summary>
    /// 更新时间
    /// </summary>
    public DateTime UpdatedAt { get; set; } = DateTime.Now;
}

/// <summary>
/// 简单的动态表实体实现
/// </summary>
public class DynamicTableEntity : DynamicTableEntityBase
{
    /// <summary>
    /// 动态数据存储
    /// </summary>
    public string? Data { get; set; }
}

/// <summary>
/// 表结构元数据管理器
/// </summary>
public static class TableMetadataManager
{
    /// <summary>
    /// 创建动态表实体类型
    /// </summary>
    public static Type CreateDynamicTableEntityType(string tableName, System.Collections.ObjectModel.ObservableCollection<TableField> fields)
    {
        // 这里简化处理，返回一个基础类型
        // 在实际项目中，可以使用源代码生成或动态类型创建
        return typeof(DynamicTableEntity);
    }

    /// <summary>
    /// 将表结构转换为实体
    /// </summary>
    public static TableEntity ToTableEntity(TableStructure table)
    {
        return new TableEntity
        {
            TableName = table.TableName,
            DisplayName = table.DisplayName,
            Description = table.Description,
            CreatedAt = table.CreatedAt,
            UpdatedAt = DateTime.Now,
            FieldsJson = System.Text.Json.JsonSerializer.Serialize(table.Fields.Select(f => new
            {
                f.FieldName,
                f.DisplayName,
                f.Description,
                f.FieldType,
                f.IsRequired,
                f.DefaultValue,
                f.MaxLength,
                f.MinValue,
                f.MaxValue,
                f.Order,
                f.IsPrimaryKey,
                f.IsUnique,
                f.IsIndexed
            }))
        };
    }

    /// <summary>
    /// 从实体转换为表结构
    /// </summary>
    public static TableStructure FromTableEntity(TableEntity entity)
    {
        var table = new TableStructure
        {
            TableName = entity.TableName,
            DisplayName = entity.DisplayName,
            Description = entity.Description,
            CreatedAt = entity.CreatedAt,
            UpdatedAt = entity.UpdatedAt,
            Fields = new System.Collections.ObjectModel.ObservableCollection<TableField>()
        };

        try
        {
            var fieldData = System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(entity.FieldsJson);
            if (fieldData != null)
            {
                foreach (var data in fieldData)
                {
                    var field = new TableField
                    {
                        FieldName = data.GetValueOrDefault("FieldName", "")?.ToString() ?? "",
                        DisplayName = data.GetValueOrDefault("DisplayName", "")?.ToString() ?? "",
                        Description = data.GetValueOrDefault("Description")?.ToString(),
                        FieldType = Enum.TryParse<TableFieldType>(data.GetValueOrDefault("FieldType", "String")?.ToString() ?? "String", out var ft) ? ft : TableFieldType.String,
                        IsRequired = data.GetValueOrDefault("IsRequired", false)?.ToString()?.ToLower() == "true",
                        DefaultValue = data.GetValueOrDefault("DefaultValue")?.ToString(),
                        Order = int.TryParse(data.GetValueOrDefault("Order", "0")?.ToString() ?? "0", out var order) ? order : 0,
                        IsPrimaryKey = data.GetValueOrDefault("IsPrimaryKey", false)?.ToString()?.ToLower() == "true",
                        IsUnique = data.GetValueOrDefault("IsUnique", false)?.ToString()?.ToLower() == "true",
                        IsIndexed = data.GetValueOrDefault("IsIndexed", false)?.ToString()?.ToLower() == "true"
                    };

                    // 处理数值类型字段
                    if (data.ContainsKey("MaxLength") && int.TryParse(data["MaxLength"]?.ToString(), out var maxLen))
                        field.MaxLength = maxLen;
                    if (data.ContainsKey("MinValue") && double.TryParse(data["MinValue"]?.ToString(), out var minVal))
                        field.MinValue = minVal;
                    if (data.ContainsKey("MaxValue") && double.TryParse(data["MaxValue"]?.ToString(), out var maxVal))
                        field.MaxValue = maxVal;

                    table.Fields.Add(field);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 反序列化字段失败: {ex.Message}");
        }

        return table;
    }
}