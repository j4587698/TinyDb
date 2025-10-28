using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using SimpleDb.Attributes;
using SimpleDb.Bson;

namespace SimpleDb.IdGeneration;

/// <summary>
/// ID生成辅助工具
/// </summary>
public static class IdGenerationHelper<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>
    where T : class, new()
{
    /// <summary>
    /// 检查实体是否需要生成ID
    /// </summary>
    /// <param name="entity">实体实例</param>
    /// <returns>是否需要生成ID</returns>
    public static bool ShouldGenerateId(T entity)
    {
        if (entity == null) return false;

        var idProperty = SimpleDb.Serialization.EntityMetadata<T>.IdProperty;
        if (idProperty == null) return false;

        // 检查ID是否已有值
        var currentValue = idProperty.GetValue(entity);
        if (currentValue != null && !IsEmptyValue(currentValue))
        {
            return false;
        }

        // 检查是否有生成策略
        var generationAttribute = idProperty.GetCustomAttribute<IdGenerationAttribute>();
        return generationAttribute?.Strategy != IdGenerationStrategy.None;
    }

    /// <summary>
    /// 为实体生成ID
    /// </summary>
    /// <param name="entity">实体实例</param>
    /// <returns>是否成功生成ID</returns>
    public static bool GenerateIdForEntity(T entity)
    {
        if (entity == null) return false;

        var idProperty = SimpleDb.Serialization.EntityMetadata<T>.IdProperty;
        if (idProperty == null) return false;

        var generationAttribute = idProperty.GetCustomAttribute<IdGenerationAttribute>();
        if (generationAttribute?.Strategy == IdGenerationStrategy.None || generationAttribute?.Strategy == null)
        {
            return false;
        }

        try
        {
            var generator = IdGeneratorFactory.GetGenerator(generationAttribute.Strategy);
            if (!generator.Supports(idProperty.PropertyType))
            {
                return false;
            }

            var newId = generator.GenerateId(typeof(T), idProperty, generationAttribute.SequenceName);
            var convertedValue = SimpleDb.Serialization.BsonConversion.FromBsonValue(newId, idProperty.PropertyType);
            idProperty.SetValue(entity, convertedValue);

            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// 检查值是否为空值
    /// </summary>
    /// <param name="value">要检查的值</param>
    /// <returns>是否为空值</returns>
    private static bool IsEmptyValue(object value)
    {
        return value switch
        {
            ObjectId objectId => objectId == ObjectId.Empty,
            string str => string.IsNullOrWhiteSpace(str),
            Guid guid => guid == Guid.Empty,
            int i => i == 0,
            long l => l == 0,
            null => true,
            _ => false
        };
    }

    /// <summary>
    /// 获取实体的ID生成策略
    /// </summary>
    /// <returns>ID生成策略</returns>
    public static IdGenerationStrategy GetIdGenerationStrategy()
    {
        var idProperty = SimpleDb.Serialization.EntityMetadata<T>.IdProperty;
        if (idProperty == null) return IdGenerationStrategy.None;

        var generationAttribute = idProperty.GetCustomAttribute<IdGenerationAttribute>();
        return generationAttribute?.Strategy ?? IdGenerationStrategy.None;
    }
}