using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using TinyDb.UI.Models;
using TinyDb.UI.Services;
using TinyDb.Core;

namespace TinyDb.UI.ViewModels;

/// <summary>
/// 表结构管理视图模型
/// </summary>
public partial class TableStructureViewModel : ViewModelBase
{
    private readonly DatabaseService _databaseService;
    private readonly TableStructureService _tableService;

    [ObservableProperty]
    private ObservableCollection<TableStructure> _tables = new();

    [ObservableProperty]
    private TableStructure? _selectedTable;

    [ObservableProperty]
    private TableStructure? _editingTable;

    [ObservableProperty]
    private bool _isEditing;

    [ObservableProperty]
    private bool _isLoading;

    [ObservableProperty]
    private string _statusMessage = "就绪";

    [ObservableProperty]
    private string _newTableName = string.Empty;

    [ObservableProperty]
    private bool _isCreatingNewTable;

    [ObservableProperty]
    private TableField? _selectedField;

    // 简化的绑定属性，用于XAML中的复杂表达式
    public bool ShowNewTableNameInput => !IsEditing && IsCreatingNewTable;
    public bool ShowTableEditing => IsEditing && EditingTable != null;
    public bool ShowTablePreview => !IsEditing && SelectedTable != null;
    public bool ShowFieldEditor => IsEditing && SelectedField != null;

    // 添加调试属性
    public bool DebugShowDataGrid => true; // 强制显示用于调试

    // 当相关属性变化时，通知计算属性更新
    partial void OnIsEditingChanged(bool value)
    {
        OnPropertyChanged(nameof(ShowNewTableNameInput));
        OnPropertyChanged(nameof(ShowTableEditing));
        OnPropertyChanged(nameof(ShowTablePreview));
        OnPropertyChanged(nameof(ShowFieldEditor));
        UpdateUIState();
    }
    partial void OnEditingTableChanged(TableStructure? value) => OnPropertyChanged(nameof(ShowTableEditing));
    partial void OnSelectedTableChanged(TableStructure? value)
    {
        OnPropertyChanged(nameof(ShowTablePreview));
        UpdateUIState();
    }
    partial void OnSelectedFieldChanged(TableField? value) => OnPropertyChanged(nameof(ShowFieldEditor));
    partial void OnNewTableNameChanged(string value)
    {
        OnPropertyChanged(nameof(ShowNewTableNameInput));
        UpdateUIState();
    }

    [ObservableProperty]
    private ObservableCollection<string> _availableFieldTypes = new()
    {
        "字符串 (String)",
        "整数 (Integer)",
        "长整数 (Long)",
        "浮点数 (Double)",
        "小数 (Decimal)",
        "布尔值 (Boolean)",
        "日期时间 (DateTime)",
        "GUID",
        "二进制 (Binary)",
        "JSON",
        "数组 (Array)",
        "对象 (Object)",
        "引用 (Reference)"
    };

    // UI状态属性
    private bool _canCreateTable;
    private bool _canEditTable;
    private bool _canDeleteTable;
    private bool _canSaveTable;
    private bool _canCancelEdit;

    public bool CanCreateTable
    {
        get => _canCreateTable;
        private set => SetProperty(ref _canCreateTable, value);
    }

    public bool CanEditTable
    {
        get => _canEditTable;
        private set => SetProperty(ref _canEditTable, value);
    }

    public bool CanDeleteTable
    {
        get => _canDeleteTable;
        private set => SetProperty(ref _canDeleteTable, value);
    }

    public bool CanSaveTable
    {
        get => _canSaveTable;
        private set => SetProperty(ref _canSaveTable, value);
    }

    public bool CanCancelEdit
    {
        get => _canCancelEdit;
        private set => SetProperty(ref _canCancelEdit, value);
    }

    public TableStructureViewModel(DatabaseService databaseService)
    {
        _databaseService = databaseService;
        _tableService = new TableStructureService(databaseService);

        UpdateUIState();
    }

    /// <summary>
    /// 设置数据库引擎
    /// </summary>
    public void SetEngine(TinyDbEngine engine)
    {
        _tableService.SetEngine(engine);
    }

    [RelayCommand]
    public async Task LoadTables()
    {
        if (!_databaseService.IsConnected) return;

        try
        {
            IsLoading = true;
            StatusMessage = "正在加载表列表...";

            var tables = await _tableService.GetAllTablesAsync();

            Tables.Clear();
            foreach (var table in tables)
            {
                Tables.Add(table);
            }

            StatusMessage = $"已加载 {tables.Count} 个表";
        }
        catch (Exception ex)
        {
            StatusMessage = $"加载表列表失败: {ex.Message}";
        }
        finally
        {
            IsLoading = false;
        }
    }

    [RelayCommand]
    private void CreateNewTable()
    {
        // 如果还没有显示输入框，先显示输入框
        if (!IsCreatingNewTable)
        {
            IsCreatingNewTable = true;
            OnPropertyChanged(nameof(ShowNewTableNameInput));
            StatusMessage = "请输入表名，然后点击创建";
            return;
        }

        // 如果已经显示了输入框，但表名为空，提示用户
        if (string.IsNullOrWhiteSpace(NewTableName))
        {
            StatusMessage = "请输入表名";
            return;
        }

        // 创建新表
        var newTable = new TableStructure
        {
            TableName = NewTableName.Trim(),
            DisplayName = NewTableName.Trim(),
            Fields = new ObservableCollection<TableField>
            {
                new TableField
                {
                    FieldName = "Id",
                    DisplayName = "ID",
                    FieldType = TableFieldType.String,
                    IsPrimaryKey = true,
                    IsRequired = true,
                    Order = 0
                },
                new TableField
                {
                    FieldName = "Name",
                    DisplayName = "名称",
                    FieldType = TableFieldType.String,
                    IsRequired = true,
                    Order = 1
                }
            }
        };

        EditingTable = newTable;
        IsEditing = true;
        IsCreatingNewTable = false; // 隐藏输入框
        NewTableName = string.Empty; // 清空输入
        OnPropertyChanged(nameof(ShowNewTableNameInput));
        StatusMessage = "正在创建新表";
        UpdateUIState();
    }

    [RelayCommand]
    private void EditTable()
    {
        if (SelectedTable == null) return;

        // 创建副本进行编辑
        EditingTable = new TableStructure
        {
            TableName = SelectedTable.TableName,
            DisplayName = SelectedTable.DisplayName,
            Description = SelectedTable.Description,
            Fields = new ObservableCollection<TableField>(SelectedTable.Fields.Select(f => new TableField
            {
                FieldName = f.FieldName,
                DisplayName = f.DisplayName,
                FieldType = f.FieldType,
                Description = f.Description,
                IsRequired = f.IsRequired,
                DefaultValue = f.DefaultValue,
                MaxLength = f.MaxLength,
                MinValue = f.MinValue,
                MaxValue = f.MaxValue,
                Order = f.Order,
                IsPrimaryKey = f.IsPrimaryKey,
                IsUnique = f.IsUnique,
                IsIndexed = f.IsIndexed,
                PasswordConfig = f.PasswordConfig
            }))
        };

        IsEditing = true;
        StatusMessage = "正在编辑表结构";
        UpdateUIState();
    }

    [RelayCommand]
    private async Task SaveTable()
    {
        if (EditingTable == null)
        {
            StatusMessage = "没有正在编辑的表";
            return;
        }

        try
        {
            IsLoading = true;
            StatusMessage = "正在保存表结构...";

            // 调试信息
            StatusMessage = $"正在保存表: {EditingTable.TableName}, 字段数: {EditingTable.Fields.Count}";

            // 验证表结构
            if (!ValidateTableStructure(EditingTable))
            {
                StatusMessage = "表结构验证失败，请检查必填字段";
                IsLoading = false;
                return;
            }

            if (SelectedTable == null || EditingTable.TableName != SelectedTable.TableName)
            {
                // 创建新表
                await _tableService.CreateTableAsync(EditingTable);
                StatusMessage = $"已创建表: {EditingTable.TableName}";
            }
            else
            {
                // 更新表结构 - 重新生成元数据
                await _tableService.UpdateTableAsync(EditingTable);
                StatusMessage = $"已更新表: {EditingTable.TableName}";
            }

            // 刷新表列表
            await LoadTables();

            // 自动选择刚保存的表
            var savedTable = Tables.FirstOrDefault(t => t.TableName == EditingTable.TableName);
            if (savedTable != null)
            {
                SelectedTable = savedTable;
                StatusMessage += $" | 已选择表: {savedTable.TableName}";
            }

            IsEditing = false;
            EditingTable = null;
            UpdateUIState();
        }
        catch (Exception ex)
        {
            StatusMessage = $"保存表失败: {ex.Message}";
        }
        finally
        {
            IsLoading = false;
        }
    }

    [RelayCommand]
    private void CancelEdit()
    {
        // 如果正在新建表，取消新建状态
        if (IsCreatingNewTable)
        {
            IsCreatingNewTable = false;
            NewTableName = string.Empty;
            OnPropertyChanged(nameof(ShowNewTableNameInput));
        }

        EditingTable = null;
        IsEditing = false;
        StatusMessage = "已取消编辑";
        UpdateUIState();
    }

    [RelayCommand]
    private async Task DeleteTable()
    {
        if (SelectedTable == null) return;

        try
        {
            IsLoading = true;
            StatusMessage = $"正在删除表: {SelectedTable.TableName}";

            await _tableService.DropTableAsync(SelectedTable.TableName);

            // 从列表中移除
            Tables.Remove(SelectedTable);
            SelectedTable = null;

            StatusMessage = $"已删除表: {SelectedTable.TableName}";
            UpdateUIState();
        }
        catch (Exception ex)
        {
            StatusMessage = $"删除表失败: {ex.Message}";
        }
        finally
        {
            IsLoading = false;
        }
    }

    [RelayCommand]
    private void AddField()
    {
        if (EditingTable == null)
        {
            StatusMessage = "没有正在编辑的表";
            return;
        }

        // 调试信息
        var fieldCountBefore = EditingTable.Fields.Count;
        StatusMessage = $"添加前字段数: {fieldCountBefore}";

        var newField = new TableField
        {
            FieldName = $"Field{EditingTable.Fields.Count + 1}",
            DisplayName = $"字段 {EditingTable.Fields.Count + 1}",
            FieldType = TableFieldType.String,
            Order = EditingTable.Fields.Count
        };

        // 重新创建ObservableCollection以确保DataGrid能正确响应
        var newFields = new ObservableCollection<TableField>(EditingTable.Fields);
        newFields.Add(newField);
        EditingTable.Fields = newFields;

        // 强制刷新所有相关属性
        OnPropertyChanged(nameof(EditingTable));
        OnPropertyChanged(nameof(EditingTable.Fields));
        OnPropertyChanged(nameof(ShowTableEditing));
        OnPropertyChanged(nameof(ShowFieldEditor));
        OnPropertyChanged(nameof(DebugShowDataGrid));

        // 详细调试信息
        var fieldCountAfter = EditingTable.Fields.Count;
        StatusMessage = $"已添加新字段: {newField.FieldName}={newField.DisplayName}, 类型={newField.FieldType} (总数: {fieldCountAfter})";

        // 列出所有字段
        var fieldNames = string.Join(", ", EditingTable.Fields.Select(f => f.FieldName));
        StatusMessage += $" | 字段列表: [{fieldNames}] | ShowTableEditing: {ShowTableEditing} | EditingTable不为空: {EditingTable != null} | IsEditing: {IsEditing}";

        // 额外的UI强制刷新
        System.Threading.Tasks.Task.Delay(50).ContinueWith(_ =>
        {
            OnPropertyChanged(nameof(EditingTable.Fields));
        });
    }

    [RelayCommand]
    private void RemoveField(TableField field)
    {
        if (EditingTable == null || field == null) return;

        if (field.IsPrimaryKey)
        {
            StatusMessage = "不能删除主键字段";
            return;
        }

        EditingTable.Fields.Remove(field);

        // 重新排序
        for (int i = 0; i < EditingTable.Fields.Count; i++)
        {
            EditingTable.Fields[i].Order = i;
        }

        StatusMessage = "已删除字段";
    }

    /// <summary>
    /// 验证表结构
    /// </summary>
    private bool ValidateTableStructure(TableStructure table)
    {
        if (string.IsNullOrWhiteSpace(table.TableName))
            return false;

        if (!table.Fields.Any(f => f.IsPrimaryKey))
            return false;

        if (table.Fields.GroupBy(f => f.FieldName).Any(g => g.Count() > 1))
            return false;

        foreach (var field in table.Fields)
        {
            if (string.IsNullOrWhiteSpace(field.FieldName))
                return false;

            if (field.IsPrimaryKey && field.FieldType != TableFieldType.String && field.FieldType != TableFieldType.Guid)
                return false;
        }

        return true;
    }

    /// <summary>
    /// 更新UI状态
    /// </summary>
    private void UpdateUIState()
    {
        CanCreateTable = _databaseService.IsConnected && !IsEditing;
        CanEditTable = _databaseService.IsConnected && !IsEditing && SelectedTable != null;
        CanDeleteTable = _databaseService.IsConnected && !IsEditing && SelectedTable != null;

        // 保存按钮逻辑：编辑中且有编辑表时启用（验证在保存时进行）
        CanSaveTable = IsEditing && EditingTable != null;

        CanCancelEdit = IsEditing;

        // 调试信息
        StatusMessage += $" [UI状态: 编辑={IsEditing}, 可保存={CanSaveTable}]";
    }
}