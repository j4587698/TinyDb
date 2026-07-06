using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace TinyDb.Bson;

/// <summary>
/// BSON ObjectId 类型，12字节的唯一标识符
/// </summary>
[Serializable]
public readonly struct ObjectId : IComparable<ObjectId>, IEquatable<ObjectId>, IConvertible
{
    private const int ObjectIdSize = 12;
    private static readonly DateTime UnixEpoch = DateTime.UnixEpoch;
    private static readonly int MachineHash = CreateMachineHash();
    private static int _counter = RandomNumberGenerator.GetInt32(0x01000000);

    private readonly uint _timestamp;
    private readonly ulong _tail;

    /// <summary>
    /// 获取 ObjectId 的字节数组表示
    /// </summary>
    public byte[] ToByteArray()
    {
        var bytes = new byte[ObjectIdSize];
        CopyTo(bytes);
        return bytes;
    }

    public void CopyTo(Span<byte> destination)
    {
        if (destination.Length < ObjectIdSize)
        {
            throw new ArgumentException("ObjectId destination must be at least 12 bytes long", nameof(destination));
        }

        BinaryPrimitives.WriteUInt32BigEndian(destination, _timestamp);
        BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(4), _tail);
    }

    /// <summary>
    /// 获取时间戳部分
    /// </summary>
    public DateTime Timestamp => UnixEpoch.AddSeconds(TimestampUnixSeconds);

    /// <summary>
    /// 获取无符号 Unix 时间戳秒数。
    /// </summary>
    public uint TimestampUnixSeconds => _timestamp;

    /// <summary>
    /// 获取时间戳秒数
    /// </summary>
    public int TimestampSeconds => unchecked((int)TimestampUnixSeconds);

    /// <summary>
    /// 获取机器标识
    /// </summary>
    public int Machine
    {
        get
        {
            return (int)((_tail >> 40) & 0x00FFFFFFUL);
        }
    }

    /// <summary>
    /// 获取进程标识
    /// </summary>
    public short Pid
    {
        get
        {
            return unchecked((short)((_tail >> 24) & 0xFFFFUL));
        }
    }

    /// <summary>
    /// 获取计数器
    /// </summary>
    public int Counter
    {
        get
        {
            return (int)(_tail & 0x00FFFFFFUL);
        }
    }

    /// <summary>
    /// 空的 ObjectId
    /// </summary>
    public static ObjectId Empty => new();

    /// <summary>
    /// 初始化一个新的 ObjectId 实例
    /// </summary>
    public ObjectId()
    {
        _timestamp = 0;
        _tail = 0;
    }

    /// <summary>
    /// 使用字节数组初始化 ObjectId
    /// </summary>
    /// <param name="bytes">12字节的数组</param>
    /// <exception cref="ArgumentException">字节数组长度必须为12</exception>
    public ObjectId(byte[] bytes)
    {
        if (bytes == null) throw new ArgumentNullException(nameof(bytes));
        if (bytes.Length != ObjectIdSize)
            throw new ArgumentException("ObjectId byte array must be 12 bytes long", nameof(bytes));

        _timestamp = BinaryPrimitives.ReadUInt32BigEndian(bytes);
        _tail = BinaryPrimitives.ReadUInt64BigEndian(bytes.AsSpan(4));
    }

    /// <summary>
    /// 使用字节 Span 初始化 ObjectId
    /// </summary>
    /// <param name="bytes">12字节的 Span</param>
    /// <exception cref="ArgumentException">字节长度必须为12</exception>
    public ObjectId(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length != ObjectIdSize)
            throw new ArgumentException("ObjectId bytes must be 12 bytes long", nameof(bytes));

        _timestamp = BinaryPrimitives.ReadUInt32BigEndian(bytes);
        _tail = BinaryPrimitives.ReadUInt64BigEndian(bytes.Slice(4));
    }

    /// <summary>
    /// 使用字符串表示初始化 ObjectId
    /// </summary>
    /// <param name="value">24字符的十六进制字符串</param>
    /// <exception cref="ArgumentException">字符串格式不正确</exception>
    public ObjectId(string value)
    {
        if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(value));
        if (value.Length != 24)
            throw new ArgumentException("ObjectId string must be 24 characters long", nameof(value));

        Span<byte> bytes = stackalloc byte[ObjectIdSize];
        for (int i = 0; i < ObjectIdSize; i++)
        {
            bytes[i] = ParseHexByte(value, i * 2);
        }

        _timestamp = BinaryPrimitives.ReadUInt32BigEndian(bytes);
        _tail = BinaryPrimitives.ReadUInt64BigEndian(bytes.Slice(4));
    }

    /// <summary>
    /// 使用时间戳、机器ID、进程ID和计数器初始化 ObjectId
    /// </summary>
    public ObjectId(DateTime timestamp, int machine, short pid, int counter)
    {
        _timestamp = GetObjectIdTimestampSeconds(timestamp);
        _tail = PackTail(machine, pid, counter);
    }

    /// <summary>
    /// 生成新的 ObjectId
    /// </summary>
    /// <returns>新的 ObjectId</returns>
    public static ObjectId NewObjectId()
    {
        var timestamp = GetObjectIdTimestampSeconds(DateTime.UtcNow);
        var counter = Interlocked.Increment(ref _counter) & 0x00FFFFFF;
        return new ObjectId(timestamp, MachineHash, GetCurrentProcessId(), counter);
    }

    private ObjectId(uint timestamp, int machine, short pid, int counter)
    {
        _timestamp = timestamp;
        _tail = PackTail(machine, pid, counter);
    }

    private static int CreateMachineHash()
    {
        Span<byte> random = stackalloc byte[8];
        RandomNumberGenerator.Fill(random);

        var identity = $"{Environment.MachineName}:{Environment.ProcessId}:";
        var identityBytes = Encoding.UTF8.GetBytes(identity);
        var input = new byte[identityBytes.Length + random.Length];
        identityBytes.CopyTo(input);
        random.CopyTo(input.AsSpan(identityBytes.Length));

        var hash = SHA256.HashData(input);
        return (hash[0] << 16) | (hash[1] << 8) | hash[2];
    }

    private static ulong PackTail(int machine, short pid, int counter)
    {
        var machinePart = (uint)machine & 0x00FFFFFFU;
        var pidPart = unchecked((ushort)pid);
        var counterPart = (uint)counter & 0x00FFFFFFU;
        return ((ulong)machinePart << 40) | ((ulong)pidPart << 24) | counterPart;
    }

    private static byte ParseHexByte(string value, int index)
    {
        return (byte)((FromHex(value[index]) << 4) | FromHex(value[index + 1]));
    }

    private static int FromHex(char c)
    {
        if ((uint)(c - '0') <= 9) return c - '0';
        if ((uint)(c - 'a') <= 5) return c - 'a' + 10;
        if ((uint)(c - 'A') <= 5) return c - 'A' + 10;
        throw new FormatException("ObjectId string contains an invalid hex character.");
    }

    private static uint GetObjectIdTimestampSeconds(DateTime timestamp)
    {
        var unixSeconds = (long)(timestamp.ToUniversalTime() - UnixEpoch).TotalSeconds;
        if (unixSeconds < uint.MinValue || unixSeconds > uint.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(timestamp), "ObjectId timestamp must be between 1970-01-01 and 2106-02-07 UTC.");
        }

        return (uint)unixSeconds;
    }

    private static short GetCurrentProcessId()
    {
        return (short)Environment.ProcessId;
    }

    /// <summary>
    /// 解析字符串为 ObjectId
    /// </summary>
    /// <param name="s">字符串表示</param>
    /// <returns>ObjectId 实例</returns>
    public static ObjectId Parse(string s)
    {
        return new ObjectId(s);
    }

    /// <summary>
    /// 尝试解析字符串为 ObjectId
    /// </summary>
    /// <param name="s">字符串表示</param>
    /// <param name="value">解析结果</param>
    /// <returns>是否解析成功</returns>
    public static bool TryParse(string s, out ObjectId value)
    {
        try
        {
            value = new ObjectId(s);
            return true;
        }
        catch (Exception ex) when (ex is FormatException or ArgumentException)
        {
            value = Empty;
            return false;
        }
    }

    /// <summary>
    /// 转换为24字符的十六进制字符串
    /// </summary>
    public override string ToString()
    {
        return string.Create(ObjectIdSize * 2, this, static (chars, objectId) =>
        {
            Span<byte> bytes = stackalloc byte[ObjectIdSize];
            objectId.CopyTo(bytes);

            for (var i = 0; i < bytes.Length; i++)
            {
                var value = bytes[i];
                chars[i * 2] = ToHexChar(value >> 4);
                chars[i * 2 + 1] = ToHexChar(value & 0x0F);
            }
        });
    }

    private static char ToHexChar(int value)
    {
        return (char)(value < 10 ? '0' + value : 'a' + value - 10);
    }

    /// <summary>
    /// 比较两个 ObjectId
    /// </summary>
    public int CompareTo(ObjectId other)
    {
        var timestampComparison = _timestamp.CompareTo(other._timestamp);
        if (timestampComparison != 0)
        {
            return Math.Sign(timestampComparison);
        }

        return Math.Sign(_tail.CompareTo(other._tail));
    }

    /// <summary>
    /// 检查相等性
    /// </summary>
    public bool Equals(ObjectId other)
    {
        return _timestamp == other._timestamp && _tail == other._tail;
    }

    /// <summary>
    /// 检查相等性
    /// </summary>
    public override bool Equals(object? obj)
    {
        return obj is ObjectId other && Equals(other);
    }

    /// <summary>
    /// 获取哈希码
    /// </summary>
    public override int GetHashCode()
    {
        return _timestamp == 0 && _tail == 0
            ? 0
            : HashCode.Combine(_timestamp, _tail);
    }

    /// <summary>
    /// 相等操作符
    /// </summary>
    public static bool operator ==(ObjectId left, ObjectId right)
    {
        return left.Equals(right);
    }

    /// <summary>
    /// 不等操作符
    /// </summary>
    public static bool operator !=(ObjectId left, ObjectId right)
    {
        return !left.Equals(right);
    }

    /// <summary>
    /// 大于操作符
    /// </summary>
    public static bool operator >(ObjectId left, ObjectId right)
    {
        return left.CompareTo(right) > 0;
    }

    /// <summary>
    /// 小于操作符
    /// </summary>
    public static bool operator <(ObjectId left, ObjectId right)
    {
        return left.CompareTo(right) < 0;
    }

    /// <summary>
    /// 大于等于操作符
    /// </summary>
    public static bool operator >=(ObjectId left, ObjectId right)
    {
        return left.CompareTo(right) >= 0;
    }

    /// <summary>
    /// 小于等于操作符
    /// </summary>
    public static bool operator <=(ObjectId left, ObjectId right)
    {
        return left.CompareTo(right) <= 0;
    }

    // IConvertible 实现
    TypeCode IConvertible.GetTypeCode() => TypeCode.Object;

    bool IConvertible.ToBoolean(IFormatProvider? provider) => throw new InvalidCastException();
    byte IConvertible.ToByte(IFormatProvider? provider) => throw new InvalidCastException();
    char IConvertible.ToChar(IFormatProvider? provider) => throw new InvalidCastException();
    DateTime IConvertible.ToDateTime(IFormatProvider? provider) => Timestamp;
    decimal IConvertible.ToDecimal(IFormatProvider? provider) => throw new InvalidCastException();
    double IConvertible.ToDouble(IFormatProvider? provider) => throw new InvalidCastException();
    short IConvertible.ToInt16(IFormatProvider? provider) => throw new InvalidCastException();
    int IConvertible.ToInt32(IFormatProvider? provider) => throw new InvalidCastException();
    long IConvertible.ToInt64(IFormatProvider? provider) => throw new InvalidCastException();
    sbyte IConvertible.ToSByte(IFormatProvider? provider) => throw new InvalidCastException();
    float IConvertible.ToSingle(IFormatProvider? provider) => throw new InvalidCastException();
    string IConvertible.ToString(IFormatProvider? provider) => ToString();
    object IConvertible.ToType(Type conversionType, IFormatProvider? provider)
    {
        if (conversionType == typeof(string)) return ToString();
        if (conversionType == typeof(byte[])) return ToByteArray();
        if (conversionType == typeof(ObjectId)) return this;
        if (conversionType == typeof(object)) return this;

        // Handle nullable types
        if (conversionType.IsGenericType && conversionType.GetGenericTypeDefinition() == typeof(Nullable<>))
        {
            var underlyingType = Nullable.GetUnderlyingType(conversionType);
            if (underlyingType == typeof(ObjectId)) return this;
            if (underlyingType == typeof(DateTime)) return Timestamp;
        }

        throw new InvalidCastException($"Cannot convert ObjectId to {conversionType.Name}");
    }

    ushort IConvertible.ToUInt16(IFormatProvider? provider) => throw new InvalidCastException();
    uint IConvertible.ToUInt32(IFormatProvider? provider) => throw new InvalidCastException();
    ulong IConvertible.ToUInt64(IFormatProvider? provider) => throw new InvalidCastException();
}
