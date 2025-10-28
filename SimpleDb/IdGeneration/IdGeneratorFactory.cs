using System;
using System.Collections.Generic;
using SimpleDb.Attributes;

namespace SimpleDb.IdGeneration;

/// <summary>
/// ID生成器工厂
/// </summary>
public static class IdGeneratorFactory
{
    private static readonly Dictionary<IdGenerationStrategy, IIdGenerator> _generators = new()
    {
        { IdGenerationStrategy.ObjectId, new ObjectIdGenerator() },
        { IdGenerationStrategy.IdentityInt, new IdentityGenerator() },
        { IdGenerationStrategy.IdentityLong, new IdentityGenerator() },
        { IdGenerationStrategy.GuidV7, new GuidV7Generator() },
        { IdGenerationStrategy.GuidV4, new GuidV4Generator() }
    };

    /// <summary>
    /// 获取指定策略的ID生成器
    /// </summary>
    /// <param name="strategy">生成策略</param>
    /// <returns>ID生成器实例</returns>
    public static IIdGenerator GetGenerator(IdGenerationStrategy strategy)
    {
        if (_generators.TryGetValue(strategy, out var generator))
        {
            return generator;
        }

        throw new NotSupportedException($"ID generation strategy '{strategy}' is not supported");
    }

    /// <summary>
    /// 注册自定义ID生成器
    /// </summary>
    /// <param name="strategy">生成策略</param>
    /// <param name="generator">生成器实例</param>
    public static void RegisterGenerator(IdGenerationStrategy strategy, IIdGenerator generator)
    {
        _generators[strategy] = generator;
    }
}