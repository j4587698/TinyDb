using System;
using System.Reflection;
using TinyDb.Bson;

namespace TinyDb.IdGeneration;

/// <summary>
/// 自动ID生成器 - 根据ID类型自动选择生成策略
/// </summary>
public static class AutoIdGenerator
{
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

        var idType = idProperty.PropertyType;

        // 根据类型自动生成ID
        return idType switch
        {
            var t when t == typeof(int) => GenerateIntId(entity, idProperty),
            var t when t == typeof(long) => GenerateLongId(entity, idProperty),
            var t when t == typeof(Guid) => GenerateGuidId(entity, idProperty),
            var t when t == typeof(string) => GenerateStringGuidId(entity, idProperty),
            var t when t == typeof(ObjectId) => GenerateObjectId(entity, idProperty),
            _ => false
        };
    }

    /// <summary>
    /// 生成int类型的自增ID
    /// </summary>
    private static bool GenerateIntId(object entity, PropertyInfo idProperty)
    {
        var key = $"{entity.GetType().Name}_{idProperty.Name}_int";
        var nextValue = IdentitySequences.GetNextValue(key);

        if (nextValue > int.MaxValue)
        {
            return false; // 超出int范围
        }

        idProperty.SetValue(entity, (int)nextValue);
        return true;
    }

    /// <summary>
    /// 生成long类型的自增ID
    /// </summary>
    private static bool GenerateLongId(object entity, PropertyInfo idProperty)
    {
        var key = $"{entity.GetType().Name}_{idProperty.Name}_long";
        var nextValue = IdentitySequences.GetNextValue(key);

        idProperty.SetValue(entity, nextValue);
        return true;
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
    private static Guid CreateGuidV7()
    {
        // 获取当前 Unix 时间戳毫秒
        var unixTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // 创建随机字节 (使用加密安全的 RNG 以避免碰撞)
        var randomBytes = new byte[10];
        System.Security.Cryptography.RandomNumberGenerator.Fill(randomBytes);

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
            null => true,
            _ => false
        };
    }
}

/// <summary>
/// 内部序列管理器
/// </summary>
public static class IdentitySequences
{
    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, long> _sequences = new();

    /// <summary>
    /// 获取序列的下一个值
    /// </summary>
    /// <param name="key">序列键</param>
    /// <returns>下一个值</returns>
    public static long GetNextValue(string key)
    {
        return _sequences.AddOrUpdate(key, 1, (k, v) => v + 1);
    }

    /// <summary>
    /// 重置所有序列（主要用于测试）
    /// </summary>
    public static void ResetAll()
    {
        _sequences.Clear();
    }

    /// <summary>
    /// 重置指定序列
    /// </summary>
    /// <param name="key">序列键</param>
    public static void Reset(string key)
    {
        _sequences.TryRemove(key, out _);
    }
}