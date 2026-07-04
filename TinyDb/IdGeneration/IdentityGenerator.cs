using System;
using System.Reflection;
using TinyDb.Bson;

namespace TinyDb.IdGeneration;

/// <summary>
/// 自增ID生成器
/// </summary>
public class IdentityGenerator : IIdGenerator
{
    public BsonValue GenerateId(Type entityType, PropertyInfo idProperty, string? sequenceName = null)
    {
        throw new InvalidOperationException(
            "Identity ID generation requires a TinyDbEngine instance so the sequence can be persisted.");
    }

    public bool Supports(Type idType)
    {
        return false;
    }
}
