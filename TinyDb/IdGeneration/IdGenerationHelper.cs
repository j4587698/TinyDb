using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using TinyDb.Attributes;
using TinyDb.Bson;

namespace TinyDb.IdGeneration;

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

        var idProperty = TinyDb.Serialization.EntityMetadata<T>.IdProperty;
        if (idProperty == null) return false;

        // 检查ID是否已有值
        var currentValue = idProperty.GetValue(entity);
        if (!IsEmptyValue(currentValue)) return false;

        // 检查是否有生成策略
        var generationAttribute = idProperty.GetCustomAttribute<IdGenerationAttribute>();
        return generationAttribute != null && generationAttribute.Strategy != IdGenerationStrategy.None;
    }

    /// <summary>
    /// 为实体生成ID
    /// </summary>
    /// <param name="entity">实体实例</param>
    /// <returns>是否成功生成ID</returns>
    public static bool GenerateIdForEntity(T entity)
    {
        if (entity == null) return false;

        var idProperty = TinyDb.Serialization.EntityMetadata<T>.IdProperty;
        if (idProperty == null) return false;

        var generationAttribute = idProperty.GetCustomAttribute<IdGenerationAttribute>();
        if (generationAttribute == null || generationAttribute.Strategy == IdGenerationStrategy.None)
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
            var convertedValue = ConvertGeneratedId(newId, idProperty.PropertyType);
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
    private static bool IsEmptyValue(object? value)
    {
        return value switch
        {
            null => true,
            ObjectId objectId => objectId == ObjectId.Empty,
            string str => string.IsNullOrWhiteSpace(str),
            Guid guid => guid == Guid.Empty,
            int i => i == 0,
            long l => l == 0,
            _ => false
        };
    }

    /// <summary>
    /// 获取实体的ID生成策略
    /// </summary>
    /// <returns>ID生成策略</returns>
    public static IdGenerationStrategy GetIdGenerationStrategy()
    {
        var idProperty = TinyDb.Serialization.EntityMetadata<T>.IdProperty;
        if (idProperty == null) return IdGenerationStrategy.None;

        var generationAttribute = idProperty.GetCustomAttribute<IdGenerationAttribute>();
        return generationAttribute?.Strategy ?? IdGenerationStrategy.None;
    }

    private static object? ConvertGeneratedId(BsonValue bsonValue, Type targetType)
    {
        if (targetType == null) throw new ArgumentNullException(nameof(targetType));

        var nonNullableType = Nullable.GetUnderlyingType(targetType) ?? targetType;
        var rawValue = bsonValue.RawValue;

        if (rawValue == null)
        {
            return null;
        }

        if (nonNullableType.IsInstanceOfType(rawValue))
        {
            return rawValue;
        }

        if (nonNullableType == typeof(string))
        {
            return rawValue.ToString();
        }

        if (nonNullableType == typeof(Guid))
        {
            return rawValue switch
            {
                byte[] bytes when bytes.Length == 16 => new Guid(bytes),
                string str => Guid.Parse(str),
                _ => Guid.Parse(rawValue.ToString() ?? string.Empty)
            };
        }

        if (nonNullableType == typeof(ObjectId))
        {
            return rawValue switch
            {
                string str => ObjectId.Parse(str),
                _ => ObjectId.Parse(rawValue.ToString() ?? string.Empty)
            };
        }

        if (nonNullableType.IsEnum)
        {
            return Enum.Parse(nonNullableType, rawValue.ToString() ?? string.Empty, ignoreCase: true);
        }

        return Convert.ChangeType(rawValue, nonNullableType);
    }
}
