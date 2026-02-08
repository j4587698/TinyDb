using System;

namespace TinyDb.Attributes;

/// <summary>
/// 索引属性，用于标记实体类的属性作为索引字段
/// </summary>
[AttributeUsage(AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
public sealed class IndexAttribute : Attribute
{
    /// <summary>
    /// 索引名称，如果不指定则自动生成
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// 是否为唯一索引
    /// </summary>
    public bool Unique { get; set; }

    /// <summary>
    /// 索引排序方向
    /// </summary>
    public IndexSortDirection SortDirection { get; set; } = IndexSortDirection.Ascending;

    /// <summary>
    /// 索引优先级（数值越小优先级越高）
    /// </summary>
    public int Priority { get; set; } = 0;

    /// <summary>
    /// 初始化索引属性
    /// </summary>
    public IndexAttribute()
    {
    Unique = false;
    SortDirection = IndexSortDirection.Ascending;
        Priority = 0;
    }

    /// <summary>
    /// 初始化索引属性
    /// </summary>
    /// <param name="unique">是否为唯一索引</param>
    public IndexAttribute(bool unique)
    {
        Unique = unique;
        SortDirection = IndexSortDirection.Ascending;
        Priority = 0;
    }

    /// <summary>
    /// 初始化索引属性
    /// </summary>
    /// <param name="name">索引名称</param>
    public IndexAttribute(string name)
    {
        Name = name;
        Unique = false;
        SortDirection = IndexSortDirection.Ascending;
        Priority = 0;
    }

    /// <summary>
    /// 初始化索引属性
    /// </summary>
    /// <param name="name">索引名称</param>
    /// <param name="unique">是否为唯一索引</param>
    public IndexAttribute(string name, bool unique)
    {
        Name = name;
        Unique = unique;
        SortDirection = IndexSortDirection.Ascending;
        Priority = 0;
    }

    /// <summary>
    /// 初始化索引属性
    /// </summary>
    /// <param name="unique">是否为唯一索引</param>
    /// <param name="sortDirection">排序方向</param>
    public IndexAttribute(bool unique, IndexSortDirection sortDirection)
    {
        Unique = unique;
        SortDirection = sortDirection;
        Priority = 0;
    }

    /// <summary>
    /// 初始化索引属性
    /// </summary>
    /// <param name="name">索引名称</param>
    /// <param name="unique">是否为唯一索引</param>
    /// <param name="sortDirection">排序方向</param>
    public IndexAttribute(string name, bool unique, IndexSortDirection sortDirection)
    {
        Name = name;
        Unique = unique;
        SortDirection = sortDirection;
        Priority = 0;
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        var parts = new List<string>();

        if (!string.IsNullOrEmpty(Name))
            parts.Add($"Name={Name}");

        parts.Add($"Unique={Unique}");
        parts.Add($"Sort={SortDirection}");

        if (Priority != 0)
            parts.Add($"Priority={Priority}");

        return $"Index({string.Join(", ", parts)})";
    }
}

/// <summary>
/// 索引排序方向
/// </summary>
public enum IndexSortDirection
{
    /// <summary>
    /// 升序
    /// </summary>
    Ascending = 1,

    /// <summary>
    /// 降序
    /// </summary>
    Descending = -1
}

/// <summary>
/// 复合索引属性，用于标记多个属性组成的复合索引
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = true, Inherited = true)]
public sealed class CompositeIndexAttribute : Attribute
{
    /// <summary>
    /// 索引名称
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// 索引字段列表
    /// </summary>
    public string[] Fields { get; }

    /// <summary>
    /// 是否为唯一索引
    /// </summary>
    public bool Unique { get; set; }

    /// <summary>
    /// 初始化复合索引属性
    /// </summary>
    /// <param name="fields">索引字段列表</param>
    public CompositeIndexAttribute(params string[] fields)
    {
        Fields = fields ?? throw new ArgumentNullException(nameof(fields));
        Name = $"idx_{string.Join("_", Fields)}";
        Unique = false;
    }

    /// <summary>
    /// 初始化复合索引属性
    /// </summary>
    /// <param name="name">索引名称</param>
    /// <param name="fields">索引字段列表</param>
    public CompositeIndexAttribute(string name, params string[] fields)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Fields = fields ?? throw new ArgumentNullException(nameof(fields));
        Unique = false;
    }

    /// <summary>
    /// 初始化复合索引属性
    /// </summary>
    /// <param name="name">索引名称</param>
    /// <param name="unique">是否为唯一索引</param>
    /// <param name="fields">索引字段列表</param>
    public CompositeIndexAttribute(string name, bool unique, params string[] fields)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
        Fields = fields ?? throw new ArgumentNullException(nameof(fields));
        Unique = unique;
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"CompositeIndex(Name={Name}, Fields=[{string.Join(", ", Fields)}], Unique={Unique})";
    }
}
