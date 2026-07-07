using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using TinyDb.Attributes;

namespace TinyDb.IdGeneration;

/// <summary>
/// ID生成器工厂
/// </summary>
public static class IdGeneratorFactory
{
    private static readonly ConcurrentDictionary<IdGenerationStrategy, IIdGenerator> _generators = new(
        new KeyValuePair<IdGenerationStrategy, IIdGenerator>[]
    {
        new(IdGenerationStrategy.ObjectId, new ObjectIdGenerator()),
        new(IdGenerationStrategy.IdentityInt, new IdentityGenerator()),
        new(IdGenerationStrategy.IdentityLong, new IdentityGenerator()),
        new(IdGenerationStrategy.GuidV7, new GuidV7Generator()),
        new(IdGenerationStrategy.GuidV4, new GuidV4Generator())
    });

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
