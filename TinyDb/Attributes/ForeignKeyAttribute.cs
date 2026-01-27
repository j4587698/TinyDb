using System;

namespace TinyDb.Attributes;

/// <summary>
/// 标记属性为外键，指向另一个集合
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false)]
public sealed class ForeignKeyAttribute : Attribute
{
    /// <summary>
    /// 关联的集合名称
    /// </summary>
    public string CollectionName { get; }

    /// <summary>
    /// 初始化外键属性
    /// </summary>
    /// <param name="collectionName">关联的集合名称</param>
    public ForeignKeyAttribute(string collectionName)
    {
        CollectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
    }
}
