using System.Security.Cryptography;
using System.Text;

namespace TinyDb.Bson;

/// <summary>
/// BSON ObjectId 类型，12字节的唯一标识符
/// </summary>
[Serializable]
public readonly struct ObjectId : IComparable<ObjectId>, IEquatable<ObjectId>, IConvertible
{
    private const int ObjectIdSize = 12;
    private static readonly RandomNumberGenerator Rng = RandomNumberGenerator.Create();
    private static int _counter = new Random().Next();

    private readonly byte[] _bytes;

    /// <summary>
    /// 获取 ObjectId 的字节数组表示
    /// </summary>
    public ReadOnlySpan<byte> ToByteArray() => _bytes ?? throw new InvalidOperationException("ObjectId is not initialized");

    /// <summary>
    /// 获取时间戳部分
    /// </summary>
    public DateTime Timestamp => new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(TimestampSeconds);

    /// <summary>
    /// 获取时间戳秒数
    /// </summary>
    public int TimestampSeconds => _bytes != null ?
        BitConverter.ToInt32(_bytes, 0) :
        throw new InvalidOperationException("ObjectId is not initialized");

    /// <summary>
    /// 获取机器标识
    /// </summary>
    public int Machine => _bytes != null ?
        BitConverter.ToInt32(_bytes, 4) & 0x00FFFFFF :
        throw new InvalidOperationException("ObjectId is not initialized");

    /// <summary>
    /// 获取进程标识
    /// </summary>
    public short Pid => _bytes != null ?
        BitConverter.ToInt16(_bytes, 7) :
        throw new InvalidOperationException("ObjectId is not initialized");

    /// <summary>
    /// 获取计数器
    /// </summary>
    public int Counter => _bytes != null ?
        BitConverter.ToInt32(_bytes, 9) & 0x00FFFFFF :
        throw new InvalidOperationException("ObjectId is not initialized");

    /// <summary>
    /// 空的 ObjectId
    /// </summary>
    public static ObjectId Empty => new();

    /// <summary>
    /// 初始化一个新的 ObjectId 实例
    /// </summary>
    public ObjectId()
    {
        _bytes = new byte[ObjectIdSize];
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

        _bytes = new byte[ObjectIdSize];
        Array.Copy(bytes, _bytes, ObjectIdSize);
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

        _bytes = new byte[ObjectIdSize];
        for (int i = 0; i < ObjectIdSize; i++)
        {
            var hex = value.Substring(i * 2, 2);
            _bytes[i] = Convert.ToByte(hex, 16);
        }
    }

    /// <summary>
    /// 使用时间戳、机器ID、进程ID和计数器初始化 ObjectId
    /// </summary>
    public ObjectId(DateTime timestamp, int machine, short pid, int counter)
    {
        _bytes = new byte[ObjectIdSize];

        var timestampSeconds = (int)(timestamp - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;

        var timestampBytes = BitConverter.GetBytes(timestampSeconds);
        var machineBytes = BitConverter.GetBytes(machine);
        var pidBytes = BitConverter.GetBytes(pid);
        var counterBytes = BitConverter.GetBytes(counter);

        Array.Copy(timestampBytes, 0, _bytes, 0, 4);
        Array.Copy(machineBytes, 0, _bytes, 4, 3);
        Array.Copy(pidBytes, 0, _bytes, 7, 2);
        Array.Copy(counterBytes, 0, _bytes, 9, 3);
    }

    /// <summary>
    /// 生成新的 ObjectId
    /// </summary>
    /// <returns>新的 ObjectId</returns>
    public static ObjectId NewObjectId()
    {
        var bytes = new byte[ObjectIdSize];

        // 时间戳 (4 bytes)
        var timestamp = (int)DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds;
        var timestampBytes = BitConverter.GetBytes(timestamp);
        Array.Copy(timestampBytes, 0, bytes, 0, 4);

        // 机器标识 (3 bytes)
        var machineBytes = GetMachineHash();
        Array.Copy(machineBytes, 0, bytes, 4, 3);

        // 进程ID (2 bytes)
        var pidBytes = BitConverter.GetBytes(GetCurrentProcessId());
        Array.Copy(pidBytes, 0, bytes, 7, 2);

        // 计数器 (3 bytes)
        var counter = Interlocked.Increment(ref _counter) & 0x00FFFFFF;
        var counterBytes = BitConverter.GetBytes(counter);
        Array.Copy(counterBytes, 0, bytes, 9, 3);

        return new ObjectId(bytes);
    }

    private static byte[] GetMachineHash()
    {
        var machineName = Environment.MachineName;
        var hash = MD5.HashData(Encoding.UTF8.GetBytes(machineName));
        return new[] { hash[0], hash[1], hash[2] };
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
        catch
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
        if (_bytes == null) return "000000000000000000000000";

        var sb = new StringBuilder(24);
        foreach (var b in _bytes)
        {
            sb.Append(b.ToString("x2"));
        }
        return sb.ToString();
    }

    /// <summary>
    /// 比较两个 ObjectId
    /// </summary>
    public int CompareTo(ObjectId other)
    {
        if (_bytes == null) return other._bytes == null ? 0 : -1;
        if (other._bytes == null) return 1;

        for (int i = 0; i < ObjectIdSize; i++)
        {
            var cmp = _bytes[i].CompareTo(other._bytes[i]);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    /// <summary>
    /// 检查相等性
    /// </summary>
    public bool Equals(ObjectId other)
    {
        return CompareTo(other) == 0;
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
        if (_bytes == null) return 0;

        var hash = 17;
        for (int i = 0; i < ObjectIdSize; i += 4)
        {
            hash = hash * 31 + BitConverter.ToInt32(_bytes, i);
        }
        return hash;
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
        if (conversionType == typeof(DateTime)) return Timestamp;
        if (conversionType == typeof(byte[])) return ToByteArray().ToArray();
        if (conversionType == typeof(ObjectId)) return this;
        if (conversionType == typeof(object)) return this;

        // Handle nullable types
        if (conversionType.IsGenericType && conversionType.GetGenericTypeDefinition() == typeof(Nullable<>))
        {
            var underlyingType = Nullable.GetUnderlyingType(conversionType);
            if (underlyingType == typeof(ObjectId)) return this;
            if (underlyingType == typeof(string)) return ToString();
            if (underlyingType == typeof(DateTime)) return Timestamp;
        }

        throw new InvalidCastException($"Cannot convert ObjectId to {conversionType.Name}");
    }

    ushort IConvertible.ToUInt16(IFormatProvider? provider) => throw new InvalidCastException();
    uint IConvertible.ToUInt32(IFormatProvider? provider) => throw new InvalidCastException();
    ulong IConvertible.ToUInt64(IFormatProvider? provider) => throw new InvalidCastException();
}