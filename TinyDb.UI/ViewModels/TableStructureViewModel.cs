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

public partial class TableStructureViewModel : ViewModelBase
{
    private readonly DatabaseService _databaseService;
    private readonly TableStructureService _tableService;

    [ObservableProperty] private ObservableCollection<TableStructure> _tables = new();
    [ObservableProperty] private TableStructure? _editingTable;
    [ObservableProperty] private bool _isEditing;
    [ObservableProperty] private bool _isLoading;
    [ObservableProperty] private string _statusMessage = "就绪";
    [ObservableProperty] private TableField? _selectedField;

    public event Action? SaveSuccess;

    // 当 EditingTable 或 IsEditing 变化时，通知 CanSaveTable 更新
    [System.Runtime.CompilerServices.ModuleInitializer]
    internal static void Init() { }

    public bool CanSaveTable => IsEditing && EditingTable != null;

    partial void OnEditingTableChanged(TableStructure? value) => OnPropertyChanged(nameof(CanSaveTable));
    partial void OnIsEditingChanged(bool value) => OnPropertyChanged(nameof(CanSaveTable));

    public TableStructureViewModel(DatabaseService databaseService)
    {
        _databaseService = databaseService;
        _tableService = new TableStructureService(databaseService);
    }

    public void SetEngine(TinyDbEngine engine) => _tableService.SetEngine(engine);

    [RelayCommand]
    public void CreateNewTable()
    {
        var table = new TableStructure
        {
            TableName = "NewTable_" + DateTime.Now.ToString("HHmmss"),
            Fields = new ObservableCollection<TableField>()
        };
        // 初始主键
        table.Fields.Add(new TableField { FieldName = "Id", FieldType = TableFieldType.String, IsPrimaryKey = true, IsRequired = true });
        
        EditingTable = table;
        IsEditing = true;
        StatusMessage = "正在设计新表...";
    }

    [RelayCommand]
    private void AddField()
    {
        if (EditingTable == null) return;
        var field = new TableField { FieldName = "NewField", FieldType = TableFieldType.String };
        EditingTable.Fields.Add(field);
        SelectedField = field;
    }

    [RelayCommand]
    private void RemoveField()
    {
        if (EditingTable != null && SelectedField != null && !SelectedField.IsPrimaryKey)
            EditingTable.Fields.Remove(SelectedField);
    }

    [RelayCommand]
    private async Task SaveTable()
    {
        if (EditingTable == null) return;
        
        string name = EditingTable.TableName?.Trim() ?? "";
        if (string.IsNullOrWhiteSpace(name))
        {
            StatusMessage = "错误：请输入表名";
            return;
        }

        // 仅允许字母、数字和下划线
        if (!System.Text.RegularExpressions.Regex.IsMatch(name, @"^[a-zA-Z0-9_]+$"))
        {
            StatusMessage = "错误：表名仅限字母、数字或下划线";
            return;
        }
        
        // 简单校验字段名是否重复
        if (EditingTable.Fields.Select(x => x.FieldName).Distinct().Count() != EditingTable.Fields.Count)
        {
            StatusMessage = "错误：字段名不能重复";
            return;
        }
        
        try {
            IsLoading = true;
            StatusMessage = "正在保存设计...";
            
            await _tableService.CreateTableAsync(EditingTable);
            StatusMessage = "保存成功";
            await Task.Delay(300);
            SaveSuccess?.Invoke();
        } 
        catch (Exception ex) { StatusMessage = "失败: " + ex.Message; }
        finally { IsLoading = false; }
    }

    [RelayCommand]
    private void CancelEdit() => SaveSuccess?.Invoke();
}