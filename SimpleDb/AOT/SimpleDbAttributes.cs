namespace SimpleDb.AOT;

/// <summary>
/// 标记类为 SimpleDb 实体，启用 AOT 优化
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class SimpleDbEntityAttribute : Attribute
{
    /// <summary>
    /// 集合名称
    /// </summary>
    public string? CollectionName { get; set; }

    /// <summary>
    /// 是否启用自动索引
    /// </summary>
    public bool AutoIndex { get; set; } = true;

    /// <summary>
    /// 初始化 SimpleDbEntity 属性
    /// </summary>
    public SimpleDbEntityAttribute()
    {
    }

    /// <summary>
    /// 初始化 SimpleDbEntity 属性
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    public SimpleDbEntityAttribute(string collectionName)
    {
        CollectionName = collectionName;
    }
}

/// <summary>
/// 标记属性为 SimpleDb ID 字段
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false)]
public sealed class SimpleDbIdAttribute : Attribute
{
    /// <summary>
    /// 是否自动生成
    /// </summary>
    public bool AutoGenerate { get; set; } = true;

    /// <summary>
    /// 初始化 SimpleDbId 属性
    /// </summary>
    public SimpleDbIdAttribute()
    {
    }
}

/// <summary>
/// 标记集合以启用 AOT 优化
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class SimpleDbCollectionAttribute : Attribute
{
    /// <summary>
    /// 集合名称
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// 是否启用索引
    /// </summary>
    public bool EnableIndex { get; set; } = true;

    /// <summary>
    /// 初始化 SimpleDbCollection 属性
    /// </summary>
    /// <param name="name">集合名称</param>
    public SimpleDbCollectionAttribute(string name)
    {
        Name = name ?? throw new ArgumentNullException(nameof(name));
    }
}

/// <summary>
/// 标记属性为索引字段
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false)]
public sealed class SimpleDbIndexAttribute : Attribute
{
    /// <summary>
    /// 是否唯一索引
    /// </summary>
    public bool Unique { get; set; }

    /// <summary>
    /// 索引名称
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// 初始化 SimpleDbIndex 属性
    /// </summary>
    public SimpleDbIndexAttribute()
    {
    }

    /// <summary>
    /// 初始化 SimpleDbIndex 属性
    /// </summary>
    /// <param name="unique">是否唯一</param>
    public SimpleDbIndexAttribute(bool unique)
    {
        Unique = unique;
    }
}

/// <summary>
/// 标记类为 AOT 优化的查询构建器
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class SimpleDbQueryBuilderAttribute : Attribute
{
    /// <summary>
    /// 是否启用缓存
    /// </summary>
    public bool EnableCache { get; set; } = true;

    /// <summary>
    /// 缓存大小
    /// </summary>
    public int CacheSize { get; set; } = 100;

    /// <summary>
    /// 初始化 SimpleDbQueryBuilder 属性
    /// </summary>
    public SimpleDbQueryBuilderAttribute()
    {
    }
}