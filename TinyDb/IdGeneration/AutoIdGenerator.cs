using System;
using System.Reflection;
using TinyDb.Attributes;
using TinyDb.Bson;
using TinyDb.Core;

namespace TinyDb.IdGeneration;

/// <summary>
/// 自动ID生成器 - 根据ID类型自动选择生成策略
/// </summary>
public static class AutoIdGenerator
{
    public static long GetNextIdentityValue(string key)
    {
        if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

        throw new InvalidOperationException(
            "Identity ID generation requires a TinyDbEngine instance so the sequence can be persisted.");
    }

    public static long GetNextIdentityValue(TinyDbEngine engine, string collectionName, string sequenceName, Type idType)
    {
        var value = CreateIdentityValue(engine, collectionName, sequenceName, idType);
        return value switch
        {
            BsonInt32 int32 => int32.Value,
            BsonInt64 int64 => int64.Value,
            _ => throw new InvalidOperationException("Unexpected identity BSON value type.")
        };
    }

    public static BsonValue CreateIdentityValue(TinyDbEngine engine, string collectionName, string sequenceName, Type idType)
    {
        if (engine == null) throw new ArgumentNullException(nameof(engine));
        if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentException("Collection name cannot be empty.", nameof(collectionName));
        if (string.IsNullOrWhiteSpace(sequenceName)) throw new ArgumentException("Sequence name cannot be empty.", nameof(sequenceName));
        if (idType == null) throw new ArgumentNullException(nameof(idType));

        return engine.AllocateIdentityId(collectionName, sequenceName, idType);
    }

    public static object? CreateIdValue(Type idType, IdGenerationStrategy strategy, string? sequenceName, string defaultSequenceName)
    {
        if (idType == null) throw new ArgumentNullException(nameof(idType));
        if (string.IsNullOrWhiteSpace(defaultSequenceName)) throw new ArgumentNullException(nameof(defaultSequenceName));

        var generationType = Nullable.GetUnderlyingType(idType) ?? idType;
        return strategy switch
        {
            IdGenerationStrategy.ObjectId when generationType == typeof(ObjectId) => ObjectId.NewObjectId(),
            IdGenerationStrategy.IdentityInt when generationType == typeof(int) => null,
            IdGenerationStrategy.IdentityLong when generationType == typeof(long) => null,
            IdGenerationStrategy.GuidV7 when generationType == typeof(Guid) => CreateGuidV7(),
            IdGenerationStrategy.GuidV7 when generationType == typeof(string) => CreateGuidV7().ToString(),
            IdGenerationStrategy.GuidV4 when generationType == typeof(Guid) => Guid.NewGuid(),
            IdGenerationStrategy.GuidV4 when generationType == typeof(string) => Guid.NewGuid().ToString(),
            _ => null
        };
    }

    /// <summary>
    /// 为实体自动生成ID（如果需要）
    /// </summary>
    /// <param name="entity">实体实例</param>
    /// <param name="idProperty">ID属性</param>
    /// <returns>是否成功生成了ID</returns>
    public static bool GenerateIdIfNeeded(object entity, PropertyInfo idProperty)
    {
        if (entity == null || idProperty == null) return false;

        // 获取当前ID值
        var currentValue = idProperty.GetValue(entity);
        if (currentValue != null && !IsEmptyValue(currentValue))
        {
            return false; // ID已有值，不需要生成
        }

        var idType = Nullable.GetUnderlyingType(idProperty.PropertyType) ?? idProperty.PropertyType;

        // 根据类型自动生成ID
        return idType switch
        {
            var t when t == typeof(int) || t == typeof(long) => false,
            var t when t == typeof(Guid) => GenerateGuidId(entity, idProperty),
            var t when t == typeof(string) => GenerateStringGuidId(entity, idProperty),
            var t when t == typeof(ObjectId) => GenerateObjectId(entity, idProperty),
            _ => false
        };
    }

    /// <summary>
    /// 生成Guid类型的ID（GUID v7）
    /// </summary>
    private static bool GenerateGuidId(object entity, PropertyInfo idProperty)
    {
        var guid = CreateGuidV7();
        idProperty.SetValue(entity, guid);
        return true;
    }

    /// <summary>
    /// 生成string类型的ID（GUID v7字符串）
    /// </summary>
    private static bool GenerateStringGuidId(object entity, PropertyInfo idProperty)
    {
        var guid = CreateGuidV7();
        idProperty.SetValue(entity, guid.ToString());
        return true;
    }

    /// <summary>
    /// 生成ObjectId类型的ID
    /// </summary>
    private static bool GenerateObjectId(object entity, PropertyInfo idProperty)
    {
        var objectId = ObjectId.NewObjectId();
        idProperty.SetValue(entity, objectId);
        return true;
    }

    /// <summary>
    /// 创建 GUID v7
    /// </summary>
    public static Guid CreateGuidV7()
    {
#if NET9_0_OR_GREATER
        // .NET 9+ 使用内置的 GUID v7 生成方法
        return Guid.CreateVersion7();
#else
        var unixTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        Span<byte> guidBytes = stackalloc byte[16];
        guidBytes[0] = (byte)(unixTimeMs >> 40);
        guidBytes[1] = (byte)(unixTimeMs >> 32);
        guidBytes[2] = (byte)(unixTimeMs >> 24);
        guidBytes[3] = (byte)(unixTimeMs >> 16);
        guidBytes[4] = (byte)(unixTimeMs >> 8);
        guidBytes[5] = (byte)unixTimeMs;
        System.Security.Cryptography.RandomNumberGenerator.Fill(guidBytes[6..]);

        // 设置版本位 (bits 12-15) 为 0111 (version 7)
        guidBytes[6] = (byte)((guidBytes[6] & 0x0F) | 0x70);

        // 设置变体位 (bits 6-7 of byte 8) 为 10 (RFC 4122 variant)
        guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80);

        return CreateGuidFromRfcBytes(guidBytes);
#endif
    }

    private static Guid CreateGuidFromRfcBytes(ReadOnlySpan<byte> bytes)
    {
        var data1 = unchecked((int)(
            ((uint)bytes[0] << 24) |
            ((uint)bytes[1] << 16) |
            ((uint)bytes[2] << 8) |
            bytes[3]));
        var data2 = unchecked((short)((bytes[4] << 8) | bytes[5]));
        var data3 = unchecked((short)((bytes[6] << 8) | bytes[7]));

        return new Guid(
            data1,
            data2,
            data3,
            bytes[8],
            bytes[9],
            bytes[10],
            bytes[11],
            bytes[12],
            bytes[13],
            bytes[14],
            bytes[15]);
    }

    /// <summary>
    /// 检查值是否为空值
    /// </summary>
    private static bool IsEmptyValue(object value)
    {
        return value switch
        {
            ObjectId objectId => objectId == ObjectId.Empty,
            string str => string.IsNullOrWhiteSpace(str),
            Guid guid => guid == Guid.Empty,
            int i => i == 0,
            long l => l == 0,
            _ => false
        };
    }

}

/// <summary>
/// 内部序列管理器
/// </summary>
public static class IdentitySequences
{
    /// <summary>
    /// 获取序列的下一个值
    /// </summary>
    /// <param name="key">序列键</param>
    /// <returns>下一个值</returns>
    public static long GetNextValue(string key)
    {
        if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));

        throw new InvalidOperationException(
            "Identity sequences are persisted by TinyDbEngine. Use AutoIdGenerator.CreateIdentityValue with an engine instance.");
    }

    /// <summary>
    /// 重置所有序列（主要用于测试）
    /// </summary>
    public static void ResetAll()
    {
    }

    /// <summary>
    /// 重置指定序列
    /// </summary>
    /// <param name="key">序列键</param>
    public static void Reset(string key)
    {
        if (string.IsNullOrWhiteSpace(key)) throw new ArgumentNullException(nameof(key));
    }
}
