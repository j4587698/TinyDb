using System;
using System.Reflection;
using TinyDb.Bson;

namespace TinyDb.IdGeneration;

/// <summary>
/// GUID v4 生成器（随机UUID）
/// </summary>
public class GuidV4Generator : IIdGenerator
{
    public BsonValue GenerateId(Type entityType, PropertyInfo idProperty, string? sequenceName = null)
    {
        var guid = Guid.NewGuid();
        return idProperty.PropertyType == typeof(Guid)
            ? new BsonBinary(guid.ToByteArray(), BsonBinary.BinarySubType.Uuid)
            : new BsonString(guid.ToString());
    }

    public bool Supports(Type idType)
    {
        return idType == typeof(string) || idType == typeof(Guid);
    }
}