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

        if (entity is BsonDocument doc)
        {
            return doc.ContainsKey("_id") ? doc["_id"] : BsonNull.Value;
        }

        if (!AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            throw new InvalidOperationException(
                $"Type '{typeof(T).FullName}' must have [Entity] attribute for AOT serialization. " +
                $"Add [Entity] attribute to the type to enable source generator support.");
        }

        return adapter.GetId(entity);
    }

    /// <summary>
    /// 设置实体的 ID。
    /// </summary>
    /// <param name="entity">实体实例。</param>
    /// <param name="id">新的 ID 值。</param>
    public static void SetId(T entity, BsonValue id)
    {
        if (entity == null) throw new ArgumentNullException(nameof(entity));

        if (entity is BsonDocument)
        {
            // BsonDocument is immutable, cannot set ID in place
            return;
        }

        if (!AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            throw new InvalidOperationException(
                $"Type '{typeof(T).FullName}' must have [Entity] attribute for AOT serialization. " +
                $"Add [Entity] attribute to the type to enable source generator support.");
        }

        adapter.SetId(entity, id);
    }

    /// <summary>
    /// 检查实体是否具有有效的 ID。
    /// </summary>
    /// <param name="entity">实体实例。</param>
    /// <returns>如果 ID 有效则为 true。</returns>
    public static bool HasValidId(T entity)
    {
        if (entity == null) return false;

        if (entity is BsonDocument doc)
        {
            return doc.ContainsKey("_id") && !doc["_id"].IsNull;
        }

        if (!AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            throw new InvalidOperationException(
                $"Type '{typeof(T).FullName}' must have [Entity] attribute for AOT serialization. " +
                $"Add [Entity] attribute to the type to enable source generator support.");
        }

        return adapter.HasValidId(entity);
    }

    /// <summary>
    /// 为实体生成ID（如果需要）
    /// </summary>
    /// <param name="entity">实体实例</param>
    /// <returns>是否成功生成ID</returns>
    public static bool GenerateIdIfNeeded(T entity)
    {
        if (entity == null) return false;

        if (entity is BsonDocument)
        {
            // BsonDocument is immutable, cannot generate ID in place
            return false;
        }

        if (!AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            throw new InvalidOperationException(
                $"Type '{typeof(T).FullName}' must have [Entity] attribute for AOT serialization. " +
                $"Add [Entity] attribute to the type to enable source generator support.");
        }

        // 如果已有有效 ID，则不需要生成
        if (adapter.HasValidId(entity))
        {
            return false;
        }

        // 获取 ID 属性信息并生成 ID
        var idProperty = EntityMetadata<T>.IdProperty;
        if (idProperty == null) return false;

        return AutoIdGenerator.GenerateIdIfNeeded(entity, idProperty);
    }
}
