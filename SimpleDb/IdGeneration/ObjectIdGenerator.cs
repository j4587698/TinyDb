using System;
using System.Reflection;
using SimpleDb.Bson;

namespace SimpleDb.IdGeneration;

/// <summary>
/// ObjectId 生成器
/// </summary>
public class ObjectIdGenerator : IIdGenerator
{
    public BsonValue GenerateId(Type entityType, PropertyInfo idProperty, string? sequenceName = null)
    {
        return new BsonObjectId(ObjectId.NewObjectId());
    }

    public bool Supports(Type idType)
    {
        return idType == typeof(ObjectId);
    }
}