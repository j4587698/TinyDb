using System;

namespace TinyDb.Attributes;

/// <summary>
/// 指定ID生成策略
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false)]
public sealed class IdGenerationAttribute : Attribute
{
    /// <summary>
    /// ID生成策略
    /// </summary>
    public IdGenerationStrategy Strategy { get; set; }

    /// <summary>
    /// 序列名称（用于自增ID）
    /// </summary>
    public string? SequenceName { get; set; }

    public IdGenerationAttribute(IdGenerationStrategy strategy = IdGenerationStrategy.None)
    {
        Strategy = strategy;
    }

    public IdGenerationAttribute(IdGenerationStrategy strategy, string sequenceName)
    {
        Strategy = strategy;
        SequenceName = sequenceName;
    }
}

/// <summary>
/// ID生成策略枚举
/// </summary>
public enum IdGenerationStrategy
{
    /// <summary>
    /// 不自动生成
    /// </summary>
    None,

    /// <summary>
    /// ObjectId自动生成
    /// </summary>
    ObjectId,

    /// <summary>
    /// 整数自增
    /// </summary>
    IdentityInt,

    /// <summary>
    /// 长整数自增
    /// </summary>
    IdentityLong,

    /// <summary>
    /// GUID v7自动生成
    /// </summary>
    GuidV7,

    /// <summary>
    /// UUID v4自动生成
    /// </summary>
    GuidV4
}