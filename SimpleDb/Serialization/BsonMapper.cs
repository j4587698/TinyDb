using System;
using System.Diagnostics.CodeAnalysis;
using SimpleDb.Bson;

namespace SimpleDb.Serialization;

/// <summary>
/// 针对旧版 API 的兼容层，内部委托给 AOT 友好的实现。
/// </summary>
public static class BsonMapper
{
    public static BsonDocument ToDocument<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity)
        where T : class, new()
    {
        return AotBsonMapper.ToDocument(entity);
    }

    public static T? ToObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(BsonDocument document)
        where T : class, new()
    {
        return document == null ? default : AotBsonMapper.FromDocument<T>(document);
    }

    public static BsonValue GetId<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity)
        where T : class, new()
    {
        return AotIdAccessor<T>.GetId(entity);
    }

    public static void SetId<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(T entity, BsonValue id)
        where T : class, new()
    {
        AotIdAccessor<T>.SetId(entity, id);
    }

    public static BsonValue ConvertToBsonValue(object? value)
    {
        return value == null ? BsonNull.Value : BsonConversion.ToBsonValue(value);
    }

    public static object? ConvertFromBsonValue(BsonValue bsonValue, Type targetType)
    {
        return BsonConversion.FromBsonValue(bsonValue, targetType);
    }
}
