using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;

namespace TinyDb.UI.Models;

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

public class TableStructure : INotifyPropertyChanged
{
    private string _tableName = string.Empty;
    private string _displayName = string.Empty;
    private string? _description;
    private ObservableCollection<TableField> _fields = new();
    private DateTime _createdAt = DateTime.Now;
    private DateTime _updatedAt = DateTime.Now;
    private long _recordCount;
    private bool _hasMetadata;

    public string TableName
    {
        get => _tableName;
        set { _tableName = value; OnPropertyChanged(nameof(TableName)); }
    }

    public string DisplayName
    {
        get => _displayName;
        set { _displayName = value; OnPropertyChanged(nameof(DisplayName)); }
    }

    public string? Description
    {
        get => _description;
        set { _description = value; OnPropertyChanged(nameof(Description)); }
    }

    public ObservableCollection<TableField> Fields
    {
        get => _fields;
        set { _fields = value; OnPropertyChanged(nameof(Fields)); }
    }

    public DateTime CreatedAt
    {
        get => _createdAt;
        set { _createdAt = value; OnPropertyChanged(nameof(CreatedAt)); }
    }

    public DateTime UpdatedAt
    {
        get => _updatedAt;
        set { _updatedAt = value; OnPropertyChanged(nameof(UpdatedAt)); }
    }

    public long RecordCount
    {
        get => _recordCount;
        set { _recordCount = value; OnPropertyChanged(nameof(RecordCount)); }
    }

    public bool HasMetadata
    {
        get => _hasMetadata;
        set { _hasMetadata = value; OnPropertyChanged(nameof(HasMetadata)); }
    }

    public event PropertyChangedEventHandler? PropertyChanged;
    protected void OnPropertyChanged(string name) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}

public class TableField : INotifyPropertyChanged
{
    private string _fieldName = string.Empty;
    private string _displayName = string.Empty;
    private string? _description;
    private TableFieldType _fieldType = TableFieldType.String;
    private bool _isRequired;
    private bool _isPrimaryKey;
    private bool _isUnique;
    private bool _isIndexed;
    private string? _defaultValue;
    private int? _maxLength;
    private double? _minValue;
    private double? _maxValue;
    private int _order;

    public string FieldName
    {
        get => _fieldName;
        set { _fieldName = value; OnPropertyChanged(nameof(FieldName)); }
    }

    public string DisplayName
    {
        get => _displayName;
        set { _displayName = value; OnPropertyChanged(nameof(DisplayName)); }
    }

    public string? Description
    {
        get => _description;
        set { _description = value; OnPropertyChanged(nameof(Description)); }
    }

    public TableFieldType FieldType
    {
        get => _fieldType;
        set { _fieldType = value; OnPropertyChanged(nameof(FieldType)); }
    }

    public bool IsRequired
    {
        get => _isRequired;
        set { _isRequired = value; OnPropertyChanged(nameof(IsRequired)); }
    }

    public bool IsPrimaryKey
    {
        get => _isPrimaryKey;
        set { _isPrimaryKey = value; OnPropertyChanged(nameof(IsPrimaryKey)); }
    }

    public bool IsUnique
    {
        get => _isUnique;
        set { _isUnique = value; OnPropertyChanged(nameof(IsUnique)); }
    }

    public bool IsIndexed
    {
        get => _isIndexed;
        set { _isIndexed = value; OnPropertyChanged(nameof(IsIndexed)); }
    }

    public string? DefaultValue
    {
        get => _defaultValue;
        set { _defaultValue = value; OnPropertyChanged(nameof(DefaultValue)); }
    }

    public int? MaxLength
    {
        get => _maxLength;
        set { _maxLength = value; OnPropertyChanged(nameof(MaxLength)); }
    }

    public double? MinValue
    {
        get => _minValue;
        set { _minValue = value; OnPropertyChanged(nameof(MinValue)); }
    }

    public double? MaxValue
    {
        get => _maxValue;
        set { _maxValue = value; OnPropertyChanged(nameof(MaxValue)); }
    }

    public int Order
    {
        get => _order;
        set { _order = value; OnPropertyChanged(nameof(Order)); }
    }

    // 适配旧代码中的 PasswordConfig 引用
    public object? PasswordConfig { get; set; }

    public event PropertyChangedEventHandler? PropertyChanged;
    protected void OnPropertyChanged(string name) => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
}
