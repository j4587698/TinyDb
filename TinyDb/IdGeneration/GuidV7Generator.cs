using System;
using System.Reflection;
using TinyDb.Bson;

namespace TinyDb.IdGeneration;

/// <summary>
/// GUID v7 生成器（基于时间的UUID）
/// </summary>
public class GuidV7Generator : IIdGenerator
{
    public BsonValue GenerateId(Type entityType, PropertyInfo idProperty, string? sequenceName = null)
    {
        var guid = CreateGuidV7();
        return idProperty.PropertyType == typeof(Guid)
            ? new BsonBinary(guid)
            : new BsonString(guid.ToString());
    }

    public bool Supports(Type idType)
    {
        return idType == typeof(string) || idType == typeof(Guid);
    }

    /// <summary>
    /// 创建 GUID v7
    /// GUID v7 格式：时间排序的 UUID，包含 Unix 时间戳毫秒
    /// </summary>
    private static Guid CreateGuidV7()
    {
        return AutoIdGenerator.CreateGuidV7();
    }
}
