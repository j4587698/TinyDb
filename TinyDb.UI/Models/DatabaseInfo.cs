using System;
using System.Collections.Generic;
using System.Linq;

namespace TinyDb.UI.Models;

/// <summary>
/// 数据库信息模型
/// </summary>
public class DatabaseInfo
{
    public string FilePath { get; set; } = string.Empty;
    public string DatabaseName { get; set; } = string.Empty;
    public string Version { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; }
    public DateTime ModifiedAt { get; set; }
    public long FileSize { get; set; }
    public int CollectionCount { get; set; }
    public bool IsReadOnly { get; set; }
    public bool EnableJournaling { get; set; }
    public uint TotalPages { get; set; }
    public uint UsedPages { get; set; }
    public uint FreePages => TotalPages - UsedPages;
    public double CacheHitRatio { get; set; }
    public int CachedPages { get; set; }

    public string FormattedFileSize => FormatFileSize(FileSize);
    public string FormulatedCreatedDate => CreatedAt.ToString("yyyy-MM-dd HH:mm:ss");
    public string FormulatedModifiedDate => ModifiedAt.ToString("yyyy-MM-dd HH:mm:ss");
    public string PageUsageInfo => $"{UsedPages}/{TotalPages} ({(TotalPages > 0 ? (UsedPages * 100.0 / TotalPages) : 0):F1}%)";
    public string CacheInfo => $"{CachedPages} pages ({CacheHitRatio:P1})";

    private static string FormatFileSize(long bytes)
    {
        string[] sizes = { "B", "KB", "MB", "GB", "TB" };
        double len = bytes;
        int order = 0;
        while (len >= 1024 && order < sizes.Length - 1)
        {
            order++;
            len = len / 1024;
        }
        return $"{len:F1} {sizes[order]}";
    }
}

/// <summary>
/// 集合信息模型
/// </summary>
public class CollectionInfo
{
    public string Name { get; set; } = string.Empty;
    public string DocumentType { get; set; } = string.Empty;
    public long DocumentCount { get; set; }
    public long CachedDocumentCount { get; set; }
    public List<string> Indexes { get; set; } = new();

    public string DocumentCountInfo => $"{DocumentCount:N0} documents";
    public string CacheInfo => $"{CachedDocumentCount:N0} cached";
    public string IndexInfo => $"{Indexes.Count} index(es)";
}

/// <summary>
/// 文档项模型
/// </summary>
public class DocumentItem : System.ComponentModel.INotifyPropertyChanged
{
    private string _id = string.Empty;
    private string _type = string.Empty;
    private string _content = string.Empty;
    private string _collectionName = string.Empty;
    private DateTime _createdAt;
    private DateTime _modifiedAt;
    private int _size;
    private Dictionary<string, object> _parsedFields = new();

    public string Id
    {
        get => _id;
        set { _id = value; OnPropertyChanged(nameof(Id)); }
    }

    public string Type
    {
        get => _type;
        set { _type = value; OnPropertyChanged(nameof(Type)); }
    }

    public string Content
    {
        get => _content;
        set {
            _content = value;
            OnPropertyChanged(nameof(Content));
            OnPropertyChanged(nameof(DisplayContent));
            ParseContent();
        }
    }

    public string CollectionName
    {
        get => _collectionName;
        set { _collectionName = value; OnPropertyChanged(nameof(CollectionName)); }
    }

    public DateTime CreatedAt
    {
        get => _createdAt;
        set { _createdAt = value; OnPropertyChanged(nameof(CreatedAt)); OnPropertyChanged(nameof(FormulatedCreatedDate)); }
    }

    public DateTime ModifiedAt
    {
        get => _modifiedAt;
        set { _modifiedAt = value; OnPropertyChanged(nameof(ModifiedAt)); OnPropertyChanged(nameof(FormulatedModifiedDate)); }
    }

    public int Size
    {
        get => _size;
        set { _size = value; OnPropertyChanged(nameof(Size)); OnPropertyChanged(nameof(FormattedSize)); }
    }

    /// <summary>
    /// 解析后的JSON字段
    /// </summary>
    public Dictionary<string, object> ParsedFields
    {
        get => _parsedFields;
        private set { _parsedFields = value; OnPropertyChanged(nameof(ParsedFields)); }
    }

    public string FormattedSize => FormatFileSize(Size);
    public string FormulatedCreatedDate => CreatedAt.ToString("yyyy-MM-dd HH:mm:ss");
    public string FormulatedModifiedDate => ModifiedAt.ToString("yyyy-MM-dd HH:mm:ss");
    public string DisplayContent => GetDisplayContent(Content);

    public event System.ComponentModel.PropertyChangedEventHandler? PropertyChanged;

    protected virtual void OnPropertyChanged(string propertyName)
    {
        PropertyChanged?.Invoke(this, new System.ComponentModel.PropertyChangedEventArgs(propertyName));
    }

    private static string FormatFileSize(int bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        return $"{bytes / (1024.0 * 1024.0):F1} MB";
    }

    private static string GetDisplayContent(string content)
    {
        if (string.IsNullOrEmpty(content))
            return "(空内容)";

        // 如果是JSON，尝试提取有意义的信息
        if (content.TrimStart().StartsWith("{") && content.TrimEnd().EndsWith("}"))
        {
            try
            {
                using var jsonDoc = System.Text.Json.JsonDocument.Parse(content);
                var root = jsonDoc.RootElement;

                // 优先显示name字段，如果没有则显示其他有意义的字段
                if (root.TryGetProperty("name", out var nameProp) && nameProp.ValueKind != System.Text.Json.JsonValueKind.Null)
                    return TruncateText(nameProp.ToString(), 50);

                if (root.TryGetProperty("Name", out var NameProp) && NameProp.ValueKind != System.Text.Json.JsonValueKind.Null)
                    return TruncateText(NameProp.ToString(), 50);

                if (root.TryGetProperty("title", out var titleProp) && titleProp.ValueKind != System.Text.Json.JsonValueKind.Null)
                    return TruncateText(titleProp.ToString(), 50);

                if (root.TryGetProperty("description", out var descProp) && descProp.ValueKind != System.Text.Json.JsonValueKind.Null && descProp.ToString().Length > 0)
                    return TruncateText(descProp.ToString(), 50);

                // 如果都没有，显示ID
                if (root.TryGetProperty("Id", out var idProp))
                    return $"ID: {TruncateText(idProp.ToString(), 30)}";
            }
            catch
            {
                // JSON解析失败，使用原始内容
            }
        }

        return TruncateText(content, 200);
    }

    /// <summary>
    /// 解析JSON内容并填充到ParsedFields字典中
    /// </summary>
    private void ParseContent()
    {
        ParsedFields.Clear();

        if (string.IsNullOrWhiteSpace(Content))
            return;

        try
        {
            using var jsonDoc = System.Text.Json.JsonDocument.Parse(Content);
            var root = jsonDoc.RootElement;

            if (root.ValueKind == System.Text.Json.JsonValueKind.Object)
            {
                var fields = new Dictionary<string, object>();

                foreach (var property in root.EnumerateObject())
                {
                    var value = ConvertJsonElementToObject(property.Value);
                    fields[property.Name] = value;
                }

                ParsedFields = fields;
            }
        }
        catch (System.Text.Json.JsonException)
        {
            // JSON解析失败，保持ParsedFields为空
        }
        catch (Exception)
        {
            // 其他异常，保持ParsedFields为空
        }
    }

    /// <summary>
    /// 将JsonElement转换为C#对象
    /// </summary>
    private static object ConvertJsonElementToObject(System.Text.Json.JsonElement element)
    {
        return element.ValueKind switch
        {
            System.Text.Json.JsonValueKind.String => element.GetString() ?? string.Empty,
            System.Text.Json.JsonValueKind.Number => element.GetDecimal(),
            System.Text.Json.JsonValueKind.True or System.Text.Json.JsonValueKind.False => element.GetBoolean(),
            System.Text.Json.JsonValueKind.Null => null,
            System.Text.Json.JsonValueKind.Array => $"Array[{element.GetArrayLength()}]",
            System.Text.Json.JsonValueKind.Object => $"Object{{{element.EnumerateObject().Count()}}}",
            _ => element.ToString()
        };
    }

    /// <summary>
    /// 获取指定字段的显示值
    /// </summary>
    public string GetFieldValue(string fieldName)
    {
        // 优先从解析的字段中查找
        if (ParsedFields.TryGetValue(fieldName, out var value))
        {
            return value?.ToString() ?? string.Empty;
        }

        // 如果是系统字段，直接返回对应属性
        return fieldName.ToLowerInvariant() switch
        {
            "id" => Id,
            "type" => Type,
            "size" => FormattedSize,
            "createdat" or "created" or "created_date" => FormulatedCreatedDate,
            "modifiedat" or "modified" or "modified_date" => FormulatedModifiedDate,
            _ => string.Empty
        };
    }

    private static string TruncateText(string text, int maxLength)
    {
        if (string.IsNullOrEmpty(text) || text.Length <= maxLength)
            return text;
        return text.Substring(0, maxLength) + "...";
    }
}

/// <summary>
/// 文档行模型，用于表格显示
/// </summary>
public class DocumentRow
{
    public DocumentItem Document { get; set; } = new();
    public List<string> FieldValues { get; set; } = new();

    public DocumentRow(DocumentItem document, List<string> fields)
    {
        Document = document;
        FieldValues = fields.Select(field => document.GetFieldValue(field)).ToList();
    }
}

/// <summary>
/// 查询结果模型
/// </summary>
public class QueryResult
{
    public string Query { get; set; } = string.Empty;
    public List<DocumentItem> Documents { get; set; } = new();
    public int TotalCount { get; set; }
    public TimeSpan ExecutionTime { get; set; }
    public string ErrorMessage { get; set; } = string.Empty;
    public bool IsSuccess => string.IsNullOrEmpty(ErrorMessage);

    public string ResultInfo => IsSuccess
        ? $"Found {TotalCount:N0} documents in {ExecutionTime.TotalMilliseconds:F1}ms"
        : $"Error: {ErrorMessage}";
}