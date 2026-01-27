using System;

namespace TinyDb.Attributes;

/// <summary>
/// 标记实体类以支持源代码生成器优化
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class EntityAttribute : Attribute
{
    /// <summary>
    /// 实体的集合名称（可选），如果不指定则使用类名
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// 手动指定ID属性名称（可选，如果不指定则自动查找Id或_id）
    /// </summary>
    public string? IdProperty { get; set; }

    public EntityAttribute()
    {
    }

    public EntityAttribute(string name)
    {
        Name = name;
    }

    public EntityAttribute(string name, string idProperty)
    {
        Name = name;
        IdProperty = idProperty;
    }
}

/// <summary>
/// 标记实体的ID属性
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false)]
public sealed class IdAttribute : Attribute
{
}

/// <summary>
/// 标记需要忽略的属性
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false)]
public sealed class BsonIgnoreAttribute : Attribute
{
}