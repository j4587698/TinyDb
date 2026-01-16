using System;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Bson;
using TinyDb.IdGeneration;

namespace TinyDb.Serialization;

/// <summary>
/// 为实体类型提供AOT友好的ID访问操作。
/// </summary>
public static class AotIdAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>
{
    /// <summary>
    /// 获取实体的 ID。
    /// </summary>
    /// <param name="entity">实体实例。</param>
    /// <returns>BSON 格式的 ID。</returns>
    public static BsonValue GetId(T entity)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.GetId(entity);
        }

        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty == null)
        {
            return BsonNull.Value;
        }

        var value = idProperty.GetValue(entity);
        return value != null ? BsonConversion.ToBsonValue(value) : BsonNull.Value;
    }

    /// <summary>
    /// 设置实体的 ID。
    /// </summary>
    /// <param name="entity">实体实例。</param>
    /// <param name="id">新的 ID 值。</param>
    public static void SetId(T entity, BsonValue id)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            adapter.SetId(entity, id);
            return;
        }

        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty == null || id == null || id.IsNull)
        {
            return;
        }

        var converted = ConvertIdValue(id, idProperty.PropertyType);
        idProperty.SetValue(entity, converted);
    }

    /// <summary>
    /// 检查实体是否具有有效的 ID。
    /// </summary>
    /// <param name="entity">实体实例。</param>
    /// <returns>如果 ID 有效则为 true。</returns>
    public static bool HasValidId(T entity)
    {
        if (entity == null) return false;

        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.HasValidId(entity);
        }

        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty == null)
        {
            return false;
        }

        var value = idProperty.GetValue(entity);
        if (value == null)
        {
            return false;
        }

        return value switch
        {
            ObjectId objectId => objectId != ObjectId.Empty,
            string str => !string.IsNullOrWhiteSpace(str),
            Guid guid => guid != Guid.Empty,
            int i => i != 0,
            long l => l != 0,
            _ => true
        };
    }

    /// <summary>
    /// 为实体生成ID（如果需要）
    /// </summary>
    /// <param name="entity">实体实例</param>
    /// <returns>是否成功生成ID</returns>
    public static bool GenerateIdIfNeeded(T entity)
    {
        if (entity == null) return false;

        var idProperty = TinyDb.Serialization.EntityMetadata<T>.IdProperty;
        if (idProperty == null) return false;

        return AutoIdGenerator.GenerateIdIfNeeded(entity, idProperty);
    }

    /// <summary>
    /// 将 BSON 值转换为 ID 属性的目标类型。
    /// </summary>
    /// <param name="bsonValue">BSON 值。</param>
    /// <param name="targetType">目标类型。</param>
    /// <returns>转换后的对象。</returns>
    private static object? ConvertIdValue(BsonValue bsonValue, Type targetType)
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
                Guid guid => guid,
                string str => Guid.Parse(str),
                _ => Guid.Parse(rawValue.ToString() ?? string.Empty)
            };
        }

        if (nonNullableType == typeof(ObjectId))
        {
            return rawValue switch
            {
                ObjectId objectId => objectId,
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
