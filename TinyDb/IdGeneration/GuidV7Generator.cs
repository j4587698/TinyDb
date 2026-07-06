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
        return new BsonString(guid.ToString());
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
#if NET9_0_OR_GREATER
        // .NET 9+ 使用内置的 GUID v7 生成方法
        return Guid.CreateVersion7();
#else
        // .NET 8 使用自定义实现
        // 获取当前 Unix 时间戳毫秒
        var unixTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        Span<byte> guidBytes = stackalloc byte[16];

        // 前6字节：Unix 时间戳毫秒的大端序
        guidBytes[0] = (byte)(unixTimeMs >> 40);
        guidBytes[1] = (byte)(unixTimeMs >> 32);
        guidBytes[2] = (byte)(unixTimeMs >> 24);
        guidBytes[3] = (byte)(unixTimeMs >> 16);
        guidBytes[4] = (byte)(unixTimeMs >> 8);
        guidBytes[5] = (byte)unixTimeMs;

        // 后10字节：随机数据（使用加密安全的 RNG 以避免碰撞）
        System.Security.Cryptography.RandomNumberGenerator.Fill(guidBytes.Slice(6));

        // 设置版本位 (bits 12-15) 为 0111 (version 7)
        guidBytes[7] = (byte)((guidBytes[7] & 0x0F) | 0x70);

        // 设置变体位 (bits 6-7 of byte 8) 为 10 (RFC 4122 variant)
        guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80);

        return new Guid(guidBytes);
#endif
    }
}
