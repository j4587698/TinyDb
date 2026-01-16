using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.ComponentModel;
using TinyDb.Metadata;

namespace TinyDb.UI.Models;

/// <summary>
/// 表结构模型
/// </summary>
public class TableStructure : INotifyPropertyChanged
{
    private ObservableCollection<TableField> _fields = new();

    /// <summary>
    /// 表名（集合名）
    /// </summary>
    [Required]
    public string TableName { get; set; } = string.Empty;

    /// <summary>
    /// 显示名称
    /// </summary>
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>
    /// 表描述
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// 字段列表
    /// </summary>
    public ObservableCollection<TableField> Fields
    {
        get => _fields;
        set
        {
            _fields = value;
            OnPropertyChanged(nameof(Fields));
        }
    }

    /// <summary>
    /// 创建时间
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.Now;

    /// <summary>
    /// 更新时间
    /// </summary>
    public DateTime UpdatedAt { get; set; } = DateTime.Now;

    /// <summary>
    /// 记录数量
    /// </summary>
    public long RecordCount { get; set; } = 0;

    /// <summary>
    /// 是否有元数据
    /// </summary>
    public bool HasMetadata { get; set; } = false;

    /// <summary>
    /// 属性更改事件
    /// </summary>
    public event PropertyChangedEventHandler? PropertyChanged;

    /// <summary>
    /// 触发属性更改通知
    /// </summary>
    protected virtual void OnPropertyChanged(string propertyName)
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}

/// <summary>
/// 表字段模型
/// </summary>
public class TableField : INotifyPropertyChanged
{
    private string _fieldName = string.Empty;
    private string _displayName = string.Empty;
    private TableFieldType _fieldType = TableFieldType.String;
    private bool _isRequired;
    private bool _isPrimaryKey;
    private bool _isUnique;
    private bool _isIndexed;

    /// <summary>
    /// 字段名
    /// </summary>
    [Required]
    public string FieldName
    {
        get => _fieldName;
        set
        {
            _fieldName = value;
            OnPropertyChanged(nameof(FieldName));
        }
    }

    /// <summary>
    /// 字段类型
    /// </summary>
    [Required]
    public TableFieldType FieldType
    {
        get => _fieldType;
        set
        {
            _fieldType = value;
            OnPropertyChanged(nameof(FieldType));
        }
    }

    /// <summary>
    /// 显示名称
    /// </summary>
    public string DisplayName
    {
        get => _displayName;
        set
        {
            _displayName = value;
            OnPropertyChanged(nameof(DisplayName));
        }
    }

    /// <summary>
    /// 字段描述
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// 是否必需
    /// </summary>
    public bool IsRequired
    {
        get => _isRequired;
        set
        {
            _isRequired = value;
            OnPropertyChanged(nameof(IsRequired));
        }
    }

    /// <summary>
    /// 默认值
    /// </summary>
    public string? DefaultValue { get; set; }

    /// <summary>
    /// 最大长度（字符串类型）
    /// </summary>
    public int? MaxLength { get; set; }

    /// <summary>
    /// 最小值（数字类型）
    /// </summary>
    public double? MinValue { get; set; }

    /// <summary>
    /// 最大值（数字类型）
    /// </summary>
    public double? MaxValue { get; set; }

    /// <summary>
    /// 显示顺序
    /// </summary>
    public int Order { get; set; } = 0;

    /// <summary>
    /// 是否为主键
    /// </summary>
    public bool IsPrimaryKey
    {
        get => _isPrimaryKey;
        set
        {
            _isPrimaryKey = value;
            OnPropertyChanged(nameof(IsPrimaryKey));
        }
    }

    /// <summary>
    /// 是否唯一
    /// </summary>
    public bool IsUnique
    {
        get => _isUnique;
        set
        {
            _isUnique = value;
            OnPropertyChanged(nameof(IsUnique));
        }
    }

    /// <summary>
    /// 是否为索引
    /// </summary>
    public bool IsIndexed
    {
        get => _isIndexed;
        set
        {
            _isIndexed = value;
            OnPropertyChanged(nameof(IsIndexed));
        }
    }

    /// <summary>
    /// 密码相关配置
    /// </summary>
    public PasswordMetadata? PasswordConfig { get; set; }

    /// <summary>
    /// 属性更改事件
    /// </summary>
    public event PropertyChangedEventHandler? PropertyChanged;

    /// <summary>
    /// 触发属性更改通知
    /// </summary>
    protected virtual void OnPropertyChanged(string propertyName)
    {
        PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propertyName));
    }
}

/// <summary>
/// 表字段类型枚举
/// </summary>
public enum TableFieldType
{
    String,
    Integer,
    Long,
    Double,
    Decimal,
    Boolean,
    DateTime,
    DateTimeOffset,
    Guid,
    Binary,
    Json,
    Array,
    Object,
    Reference
}

/// <summary>
/// 表结构与TinyDb元数据的转换工具
/// </summary>
public static class TableStructureConverter
{
    /// <summary>
    /// 从EntityMetadata转换为TableStructure
    /// </summary>
    public static TableStructure FromEntityMetadata(EntityMetadata metadata, string tableName)
    {
        var table = new TableStructure
        {
            TableName = tableName,
            DisplayName = metadata.DisplayName,
            Description = metadata.Description,
            CreatedAt = DateTime.Now,
            UpdatedAt = DateTime.Now,
            HasMetadata = true
        };

        foreach (var prop in metadata.Properties.OrderBy(p => p.Order))
        {
            var field = new TableField
            {
                FieldName = prop.PropertyName,
                DisplayName = prop.DisplayName,
                Description = prop.Description,
                FieldType = ConvertToTableFieldType(prop.PropertyType),
                IsRequired = prop.Required,
                Order = prop.Order,
                PasswordConfig = prop.Password
            };

            table.Fields.Add(field);
        }

        return table;
    }

    /// <summary>
    /// 从TableStructure转换为EntityMetadata
    /// </summary>
    public static EntityMetadata ToEntityMetadata(TableStructure table)
    {
        var metadata = new EntityMetadata
        {
            TypeName = table.TableName,
            DisplayName = table.DisplayName,
            Description = table.Description
        };

        foreach (var field in table.Fields.OrderBy(f => f.Order))
        {
            var prop = new PropertyMetadata
            {
                PropertyName = field.FieldName,
                DisplayName = field.DisplayName,
                Description = field.Description,
                PropertyType = ConvertFromTableFieldType(field.FieldType),
                Required = field.IsRequired,
                Order = field.Order,
                Password = field.PasswordConfig
            };

            metadata.Properties.Add(prop);
        }

        return metadata;
    }

    /// <summary>
    /// 转换字段类型为TableFieldType
    /// </summary>
    private static TableFieldType ConvertToTableFieldType(string propertyType)
    {
        return propertyType switch
        {
            "System.String" => TableFieldType.String,
            "System.Int32" => TableFieldType.Integer,
            "System.Int64" => TableFieldType.Long,
            "System.Single" => TableFieldType.Double,
            "System.Double" => TableFieldType.Double,
            "System.Decimal" => TableFieldType.Decimal,
            "System.Boolean" => TableFieldType.Boolean,
            "System.DateTime" => TableFieldType.DateTime,
            "System.DateTimeOffset" => TableFieldType.DateTimeOffset,
            "System.Guid" => TableFieldType.Guid,
            "System.Byte[]" => TableFieldType.Binary,
            _ when propertyType.Contains("Dictionary") => TableFieldType.Object,
            _ when propertyType.Contains("List") => TableFieldType.Array,
            _ => TableFieldType.String
        };
    }

    /// <summary>
    /// 转换TableFieldType为字符串类型
    /// </summary>
    private static string ConvertFromTableFieldType(TableFieldType fieldType)
    {
        return fieldType switch
        {
            TableFieldType.String => "System.String",
            TableFieldType.Integer => "System.Int32",
            TableFieldType.Long => "System.Int64",
            TableFieldType.Double => "System.Double",
            TableFieldType.Decimal => "System.Decimal",
            TableFieldType.Boolean => "System.Boolean",
            TableFieldType.DateTime => "System.DateTime",
            TableFieldType.DateTimeOffset => "System.DateTimeOffset",
            TableFieldType.Guid => "System.Guid",
            TableFieldType.Binary => "System.Byte[]",
            TableFieldType.Json => "System.String",
            TableFieldType.Array => "System.Collections.Generic.List`1",
            TableFieldType.Object => "System.Collections.Generic.Dictionary`2",
            TableFieldType.Reference => "System.String",
            _ => "System.String"
        };
    }
}