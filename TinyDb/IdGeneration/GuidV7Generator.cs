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
        // 获取当前 Unix 时间戳毫秒
        var unixTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // 创建随机字节
        var randomBytes = new byte[10];
        Random.Shared.NextBytes(randomBytes);

        // 构造 GUID v7 的16字节
        var guidBytes = new byte[16];

        // 前6字节：Unix 时间戳毫秒的大端序
        var timeBytes = BitConverter.GetBytes(unixTimeMs);
        if (BitConverter.IsLittleEndian)
        {
            Array.Reverse(timeBytes);
        }
        Buffer.BlockCopy(timeBytes, 2, guidBytes, 0, 6); // 取高6字节

        // 接下来的4字节：随机数据
        Buffer.BlockCopy(randomBytes, 0, guidBytes, 6, 4);

        // 最后6字节：随机数据，但设置版本和变体
        Buffer.BlockCopy(randomBytes, 4, guidBytes, 10, 6);

        // 设置版本位 (bits 12-15) 为 0111 (version 7)
        guidBytes[7] = (byte)((guidBytes[7] & 0x0F) | 0x70);

        // 设置变体位 (bits 6-7 of byte 8) 为 10 (RFC 4122 variant)
        guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80);

        return new Guid(guidBytes);
    }
}