using System;
using System.Diagnostics.CodeAnalysis;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// 针对旧版 API 的兼容层，内部委托给 AOT 友好的实现。
/// </summary>
public static class BsonMapper
{
    /// <summary>
    /// 将实体转换为 BSON 文档。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">要转换的实体。</param>
    /// <returns>BSON 文档。</returns>
    public static BsonDocument ToDocument<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(T entity)
        where T : class, new()
    {
        return AotBsonMapper.ToDocument(entity);
    }

    /// <summary>
    /// 将 BSON 文档转换为实体对象。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="document">BSON 文档。</param>
    /// <returns>转换后的实体对象，如果文档为空则返回默认值。</returns>
    public static T? ToObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(BsonDocument document)
        where T : class, new()
    {
        return document == null ? default : AotBsonMapper.FromDocument<T>(document);
    }

    /// <summary>
    /// 获取实体的 ID。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">实体对象。</param>
    /// <returns>BSON 格式的 ID 值。</returns>
    public static BsonValue GetId<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(T entity)
        where T : class, new()
    {
        return AotIdAccessor<T>.GetId(entity);
    }

    /// <summary>
    /// 设置实体的 ID。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="entity">实体对象。</param>
    /// <param name="id">新的 ID 值。</param>
    public static void SetId<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(T entity, BsonValue id)
        where T : class, new()
    {
        AotIdAccessor<T>.SetId(entity, id);
    }

    /// <summary>
    /// 将普通对象转换为 BSON 值。
    /// </summary>
    /// <param name="value">普通对象。</param>
    /// <returns>BSON 值。</returns>
    public static BsonValue ConvertToBsonValue(object? value)
    {
        return value == null ? BsonNull.Value : BsonConversion.ToBsonValue(value);
    }

    /// <summary>
    /// 将 BSON 值转换为目标类型的对象。
    /// </summary>
    /// <param name="bsonValue">BSON 值。</param>
    /// <param name="targetType">目标类型。</param>
    /// <returns>转换后的对象。</returns>
    public static object? ConvertFromBsonValue(BsonValue bsonValue, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.Interfaces)] Type targetType)
    {
        return AotBsonMapper.ConvertValue(bsonValue, targetType);
    }
}
