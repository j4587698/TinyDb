using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading.Tasks;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using TinyDb.UI.Models;
using TinyDb.UI.Services;

namespace TinyDb.UI.ViewModels;

/// <summary>
/// 单元格视图模型
/// </summary>
public partial class CellViewModel : ObservableObject
{
    [ObservableProperty] private string _fieldName = string.Empty;
    [ObservableProperty] private object? _value;
    [ObservableProperty] private bool _isPrimaryKey;

    public CellViewModel(string name, object? val, bool isPk = false)
    {
        FieldName = name;
        Value = val;
        IsPrimaryKey = isPk;
    }
}

/// <summary>
/// 行视图模型
/// </summary>
public partial class RowViewModel : ObservableObject
{
    [ObservableProperty] private ObservableCollection<CellViewModel> _cells = new();
    
    // 方便根据字段名获取值
    public object? GetValue(string fieldName) => 
        Cells.FirstOrDefault(c => c.FieldName == fieldName)?.Value;

    public void SetValue(string fieldName, object? val)
    {
        var cell = Cells.FirstOrDefault(c => c.FieldName == fieldName);
        if (cell != null) cell.Value = val;
    }
}

public partial class TableTabViewModel : ViewModelBase
{
    private readonly DatabaseService _databaseService;
    
    [ObservableProperty] private string _tableName;
    [ObservableProperty] private ObservableCollection<RowViewModel> _rows = new();
    [ObservableProperty] private RowViewModel? _selectedRow;
    [ObservableProperty] private bool _isLoading;
    [ObservableProperty] private string _statusMessage = string.Empty;
    [ObservableProperty] private ObservableCollection<TableField> _fields = new();

    public TableTabViewModel(string tableName, DatabaseService databaseService)
    {
        _tableName = tableName;
        _databaseService = databaseService;
        _ = LoadDataAsync();
    }

    [RelayCommand]
    public async Task LoadDataAsync()
    {
        try {
            IsLoading = true;
            
            // 1. 获取表模式
            var schemaFields = await _databaseService.GetFieldsForCollectionAsync(TableName);
            await Avalonia.Threading.Dispatcher.UIThread.InvokeAsync(() => {
                Fields.Clear();
                foreach (var f in schemaFields) Fields.Add(f);
            });

            // 2. 加载数据并转换为行模型
            var docs = await _databaseService.GetDocumentsAsync(TableName);
            
            await Avalonia.Threading.Dispatcher.UIThread.InvokeAsync(() => {
                Rows.Clear();
                foreach (var doc in docs)
                {
                    Rows.Add(CreateRowFromDoc(doc));
                }
                StatusMessage = $"已加载 {Rows.Count} 条记录";
            });
        } catch (Exception ex) { StatusMessage = "加载失败: " + ex.Message; }
        finally { IsLoading = false; }
    }

    private RowViewModel CreateRowFromDoc(DocumentItem doc)
    {
        var row = new RowViewModel();
        // 解析文档内容
        var contentDict = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object?>>(doc.Content) ?? new();

        // 关键改进：严格按照 Fields (元数据) 投影
        foreach (var field in Fields)
        {
            object? val = null;
            
            // 逻辑：如果是主键，优先取 doc.Id (底层的 _id)
            if (field.IsPrimaryKey)
            {
                val = doc.Id;
            }
            else 
            {
                // 否则从 Dictionary 里找对应命名的值
                // 尝试各种大小写匹配以增强兼容性
                var key = contentDict.Keys.FirstOrDefault(k => k.Equals(field.FieldName, StringComparison.OrdinalIgnoreCase));
                if (key != null) val = contentDict[key];
            }
            
            row.Cells.Add(new CellViewModel(field.FieldName, val, field.IsPrimaryKey));
        }
        return row;
    }

    [RelayCommand]
    private void AddRow()
    {
        var row = new RowViewModel();
        foreach (var field in Fields)
        {
            var val = field.IsPrimaryKey ? "(新行)" : "";
            row.Cells.Add(new CellViewModel(field.FieldName, val, field.IsPrimaryKey));
        }
        Rows.Add(row);
        SelectedRow = row;
    }

    [RelayCommand]
    private async Task SaveChangesAsync()
    {
        try {
            IsLoading = true;
            int count = 0;
            foreach (var row in Rows)
            {
                var idCell = row.Cells.FirstOrDefault(c => c.IsPrimaryKey);
                if (idCell == null) continue;

                var id = idCell.Value?.ToString();
                
                // 将 Cell 集合转回 Dictionary 用于保存
                var dict = row.Cells.ToDictionary(c => c.FieldName, c => c.Value);
                var json = System.Text.Json.JsonSerializer.Serialize(dict);

                if (id == "(新行)") {
                    await _databaseService.InsertDocumentAsync(TableName, json);
                    count++;
                } else if (!string.IsNullOrEmpty(id)) {
                    await _databaseService.UpdateDocumentAsync(TableName, id, json);
                    count++;
                }
            }
            StatusMessage = $"成功保存 {count} 条变更";
            await LoadDataAsync();
        } catch (Exception ex) { StatusMessage = "保存失败: " + ex.Message; }
        finally { IsLoading = false; }
    }

    [RelayCommand]
    private async Task DeleteRowAsync()
    {
        if (SelectedRow == null) return;
        var id = SelectedRow.Cells.FirstOrDefault(c => c.IsPrimaryKey)?.Value?.ToString();
        
        if (!string.IsNullOrEmpty(id) && id != "(新行)")
            await _databaseService.DeleteDocumentAsync(TableName, id);
        
        Rows.Remove(SelectedRow);
    }
}