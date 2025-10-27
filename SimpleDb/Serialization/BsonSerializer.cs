using System.Buffers;
using System.Text;
using System.Globalization;
using SimpleDb.Bson;

namespace SimpleDb.Serialization;

/// <summary>
/// BSON 序列化器，负责将 BSON 对象序列化和反序列化为字节数组
/// </summary>
public static class BsonSerializer
{
    /// <summary>
    /// 默认编码
    /// </summary>
    private static readonly Encoding DefaultEncoding = Encoding.UTF8;

    /// <summary>
    /// 序列化 BsonValue
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <returns>字节数组</returns>
    public static byte[] Serialize(BsonValue value)
    {
        using var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);
        writer.WriteValue(value);
        return stream.ToArray();
    }

    /// <summary>
    /// 反序列化为 BsonValue
    /// </summary>
    /// <param name="data">字节数组</param>
    /// <returns>BSON 值</returns>
    public static BsonValue Deserialize(byte[] data)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));
        if (data.Length == 0) return BsonNull.Value;

        using var stream = new MemoryStream(data);
        using var reader = new BsonReader(stream);
        return reader.ReadValue();
    }

    /// <summary>
    /// 序列化 BsonDocument
    /// </summary>
    /// <param name="document">BSON 文档</param>
    /// <returns>字节数组</returns>
    public static byte[] SerializeDocument(BsonDocument document)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        using var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);
        writer.WriteDocument(document);
        return stream.ToArray();
    }

    /// <summary>
    /// 反序列化为 BsonDocument
    /// </summary>
    /// <param name="data">字节数组</param>
    /// <returns>BSON 文档</returns>
    public static BsonDocument DeserializeDocument(byte[] data)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));
        if (data.Length == 0) return new BsonDocument();

        using var stream = new MemoryStream(data);
        using var reader = new BsonReader(stream);
        return reader.ReadDocument();
    }

    /// <summary>
    /// 序列化 BsonArray
    /// </summary>
    /// <param name="array">BSON 数组</param>
    /// <returns>字节数组</returns>
    public static byte[] SerializeArray(BsonArray array)
    {
        if (array == null) throw new ArgumentNullException(nameof(array));

        using var stream = new MemoryStream();
        using var writer = new BsonWriter(stream);
        writer.WriteArray(array);
        return stream.ToArray();
    }

    /// <summary>
    /// 反序列化为 BsonArray
    /// </summary>
    /// <param name="data">字节数组</param>
    /// <returns>BSON 数组</returns>
    public static BsonArray DeserializeArray(byte[] data)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));
        if (data.Length == 0) return new BsonArray();

        using var stream = new MemoryStream(data);
        using var reader = new BsonReader(stream);
        return reader.ReadArray();
    }

    /// <summary>
    /// 计算序列化后的大小
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <returns>大小（字节）</returns>
    public static int CalculateSize(BsonValue value)
    {
        var calculator = new SizeCalculator();
        return calculator.CalculateSize(value);
    }

    /// <summary>
    /// 计算文档序列化后的大小
    /// </summary>
    /// <param name="document">BSON 文档</param>
    /// <returns>大小（字节）</returns>
    public static int CalculateDocumentSize(BsonDocument document)
    {
        var calculator = new SizeCalculator();
        return calculator.CalculateDocumentSize(document);
    }

    /// <summary>
    /// 计算数组序列化后的大小
    /// </summary>
    /// <param name="array">BSON 数组</param>
    /// <returns>大小（字节）</returns>
    public static int CalculateArraySize(BsonArray array)
    {
        var calculator = new SizeCalculator();
        return calculator.CalculateArraySize(array);
    }
}

/// <summary>
/// BSON 写入器，负责将 BSON 数据写入流
/// </summary>
public sealed class BsonWriter : IDisposable
{
    private readonly Stream _stream;
    private readonly BinaryWriter _writer;
    private bool _disposed;

    /// <summary>
    /// 初始化 BSON 写入器
    /// </summary>
    /// <param name="stream">输出流</param>
    public BsonWriter(Stream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _writer = new BinaryWriter(stream, Encoding.UTF8, true);
    }

    /// <summary>
    /// 写入 BSON 值
    /// </summary>
    /// <param name="value">BSON 值</param>
    public void WriteValue(BsonValue value)
    {
        ThrowIfDisposed();

        switch (value)
        {
            case BsonNull:
                WriteNull();
                break;
            case BsonString str:
                WriteString(str.Value);
                break;
            case BsonInt32 i32:
                WriteInt32(i32.Value);
                break;
            case BsonInt64 i64:
                WriteInt64(i64.Value);
                break;
            case BsonDouble dbl:
                WriteDouble(dbl.Value);
                break;
            case BsonBoolean bl:
                WriteBoolean(bl.Value);
                break;
            case BsonObjectId oid:
                WriteObjectId(oid.Value);
                break;
            case BsonDateTime dt:
                WriteDateTime(dt.Value);
                break;
            case BsonDocument doc:
                WriteDocument(doc);
                break;
            case BsonArray arr:
                WriteArray(arr);
                break;
            default:
                throw new NotSupportedException($"BSON type {value.BsonType} is not supported");
        }
    }

    /// <summary>
    /// 写入 BSON 文档
    /// </summary>
    /// <param name="document">BSON 文档</param>
    public void WriteDocument(BsonDocument document)
    {
        ThrowIfDisposed();
        if (document == null) throw new ArgumentNullException(nameof(document));

        // 记录文档开始位置，稍后写入大小
        var sizePosition = _stream.Position;
        _writer.Write(0); // 占位符，稍后更新

        // 写入文档元素
        foreach (var kvp in document)
        {
            WriteElement(kvp.Key, kvp.Value);
        }

        // 写入结束标记
        _writer.Write((byte)BsonType.End);

        // 计算并写入文档大小
        var endPosition = _stream.Position;
        var documentSize = (int)(endPosition - sizePosition);
        _stream.Seek(sizePosition, SeekOrigin.Begin);
        _writer.Write(documentSize);
        _stream.Seek(endPosition, SeekOrigin.Begin);
    }

    /// <summary>
    /// 写入 BSON 数组
    /// </summary>
    /// <param name="array">BSON 数组</param>
    public void WriteArray(BsonArray array)
    {
        ThrowIfDisposed();
        if (array == null) throw new ArgumentNullException(nameof(array));

        // 记录数组开始位置，稍后写入大小
        var sizePosition = _stream.Position;
        _writer.Write(0); // 占位符，稍后更新

        // 写入数组元素
        for (int i = 0; i < array.Count; i++)
        {
            WriteElement(i.ToString(), array[i]);
        }

        // 写入结束标记
        _writer.Write((byte)BsonType.End);

        // 计算并写入数组大小
        var endPosition = _stream.Position;
        var arraySize = (int)(endPosition - sizePosition);
        _stream.Seek(sizePosition, SeekOrigin.Begin);
        _writer.Write(arraySize);
        _stream.Seek(endPosition, SeekOrigin.Begin);
    }

    /// <summary>
    /// 写入文档元素
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="value">值</param>
    private void WriteElement(string key, BsonValue value)
    {
        // 写入类型字节
        _writer.Write((byte)value.BsonType);

        // 写入键名（以 null 结尾）
        WriteCString(key);

        // 写入值
        WriteValue(value);
    }

    /// <summary>
    /// 写入 C 字符串（以 null 结尾的字符串）
    /// </summary>
    /// <param name="value">字符串值</param>
    public void WriteCString(string value)
    {
        ThrowIfDisposed();
        if (value == null) throw new ArgumentNullException(nameof(value));

        var bytes = Encoding.UTF8.GetBytes(value);
        _writer.Write(bytes);
        _writer.Write((byte)0); // null 终止符
    }

    /// <summary>
    /// 写入字符串
    /// </summary>
    /// <param name="value">字符串值</param>
    public void WriteString(string value)
    {
        ThrowIfDisposed();
        if (value == null) throw new ArgumentNullException(nameof(value));

        var bytes = Encoding.UTF8.GetBytes(value);
        _writer.Write(bytes.Length + 1); // 长度（包含 null 终止符）
        _writer.Write(bytes);
        _writer.Write((byte)0); // null 终止符
    }

    /// <summary>
    /// 写入 32 位整数
    /// </summary>
    /// <param name="value">整数值</param>
    public void WriteInt32(int value)
    {
        ThrowIfDisposed();
        _writer.Write(value);
    }

    /// <summary>
    /// 写入 64 位整数
    /// </summary>
    /// <param name="value">整数值</param>
    public void WriteInt64(long value)
    {
        ThrowIfDisposed();
        _writer.Write(value);
    }

    /// <summary>
    /// 写入双精度浮点数
    /// </summary>
    /// <param name="value">浮点数值</param>
    public void WriteDouble(double value)
    {
        ThrowIfDisposed();
        _writer.Write(value);
    }

    /// <summary>
    /// 写入布尔值
    /// </summary>
    /// <param name="value">布尔值</param>
    public void WriteBoolean(bool value)
    {
        ThrowIfDisposed();
        _writer.Write(value ? (byte)1 : (byte)0);
    }

    /// <summary>
    /// 写入 ObjectId
    /// </summary>
    /// <param name="value">ObjectId 值</param>
    public void WriteObjectId(ObjectId value)
    {
        ThrowIfDisposed();
        var bytes = value.ToByteArray();
        _writer.Write(bytes);
    }

    /// <summary>
    /// 写入 DateTime
    /// </summary>
    /// <param name="value">DateTime 值</param>
    public void WriteDateTime(DateTime value)
    {
        ThrowIfDisposed();
        // BSON DateTime 存储为从 Unix 纪元开始的毫秒数
        var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var milliseconds = (long)(value - unixEpoch).TotalMilliseconds;
        _writer.Write(milliseconds);
    }

    /// <summary>
    /// 写入 null 值
    /// </summary>
    public void WriteNull()
    {
        ThrowIfDisposed();
        // null 值没有数据部分
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(BsonWriter));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _writer?.Dispose();
            _stream?.Dispose();
            _disposed = true;
        }
    }
}

/// <summary>
/// BSON 读取器，负责从流中读取 BSON 数据
/// </summary>
public sealed class BsonReader : IDisposable
{
    private readonly Stream _stream;
    private readonly BinaryReader _reader;
    private bool _disposed;

    /// <summary>
    /// 初始化 BSON 读取器
    /// </summary>
    /// <param name="stream">输入流</param>
    public BsonReader(Stream stream)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _reader = new BinaryReader(stream, Encoding.UTF8, true);
    }

    /// <summary>
    /// 读取 BSON 值（需要先读取类型字节）
    /// </summary>
    /// <param name="type">已读取的 BSON 类型</param>
    /// <returns>BSON 值</returns>
    public BsonValue ReadValue(BsonType type)
    {
        ThrowIfDisposed();

        return type switch
        {
            BsonType.Null => BsonNull.Value,
            BsonType.String => ReadString(),
            BsonType.Int32 => ReadInt32(),
            BsonType.Int64 => ReadInt64(),
            BsonType.Double => ReadDouble(),
            BsonType.Boolean => ReadBoolean(),
            BsonType.ObjectId => ReadObjectId(),
            BsonType.DateTime => ReadDateTime(),
            BsonType.Document => ReadDocument(),
            BsonType.Array => ReadArray(),
            BsonType.End => throw new InvalidOperationException("Unexpected end marker"),
            _ => throw new NotSupportedException($"BSON type {type} is not supported")
        };
    }

    /// <summary>
    /// 读取 BSON 值（包含类型字节）
    /// </summary>
    /// <returns>BSON 值</returns>
    public BsonValue ReadValue()
    {
        ThrowIfDisposed();
        var type = (BsonType)_reader.ReadByte();
        return ReadValue(type);
    }

    /// <summary>
    /// 读取 BSON 文档
    /// </summary>
    /// <returns>BSON 文档</returns>
    public BsonDocument ReadDocument()
    {
        ThrowIfDisposed();

        var documentSize = _reader.ReadInt32();
        var startPosition = _stream.Position;
        var document = new BsonDocument();

        while (true)
        {
            var type = (BsonType)_reader.ReadByte();

            if (type == BsonType.End)
                break;

            var key = ReadCString();
            var value = ReadTypedValue(type);

            document = document.Set(key, value);
        }

        var endPosition = _stream.Position;
        // BSON文档大小包含开头4字节的大小字段，所以实际内容大小应该是文档大小-4
        var expectedContentSize = documentSize - 4;
        var actualContentSize = (int)(endPosition - startPosition);

        if (actualContentSize != expectedContentSize)
        {
            throw new InvalidOperationException($"Document size mismatch: expected {documentSize} (content: {expectedContentSize}), actual content size {actualContentSize}");
        }

        return document;
    }

    /// <summary>
    /// 读取 BSON 数组
    /// </summary>
    /// <returns>BSON 数组</returns>
    public BsonArray ReadArray()
    {
        ThrowIfDisposed();

        var arraySize = _reader.ReadInt32();
        var startPosition = _stream.Position;
        var array = new BsonArray();

        while (true)
        {
            var type = (BsonType)_reader.ReadByte();

            if (type == BsonType.End)
                break;

            var key = ReadCString(); // 数组索引
            var value = ReadTypedValue(type);

            array = array.AddValue(value);
        }

        var endPosition = _stream.Position;
        // BSON数组大小包含开头4字节的大小字段，所以实际内容大小应该是数组大小-4
        var expectedContentSize = arraySize - 4;
        var actualContentSize = (int)(endPosition - startPosition);

        if (actualContentSize != expectedContentSize)
        {
            throw new InvalidOperationException($"Array size mismatch: expected {arraySize} (content: {expectedContentSize}), actual content size {actualContentSize}");
        }

        return array;
    }

    /// <summary>
    /// 读取指定类型的值
    /// </summary>
    /// <param name="type">BSON 类型</param>
    /// <returns>BSON 值</returns>
    private BsonValue ReadTypedValue(BsonType type)
    {
        return ReadValue(type);
    }

    /// <summary>
    /// 读取 C 字符串（以 null 结尾的字符串）
    /// </summary>
    /// <returns>字符串</returns>
    public string ReadCString()
    {
        ThrowIfDisposed();

        var bytes = new List<byte>();
        byte b;

        while ((b = _reader.ReadByte()) != 0)
        {
            bytes.Add(b);
        }

        return Encoding.UTF8.GetString(bytes.ToArray());
    }

    /// <summary>
    /// 读取字符串
    /// </summary>
    /// <returns>字符串</returns>
    public BsonString ReadString()
    {
        ThrowIfDisposed();

        var length = _reader.ReadInt32();
        var bytes = _reader.ReadBytes(length - 1); // 不包含 null 终止符
        var nullTerminator = _reader.ReadByte();

        if (nullTerminator != 0)
        {
            throw new InvalidOperationException("String null terminator expected");
        }

        var value = Encoding.UTF8.GetString(bytes);
        return new BsonString(value);
    }

    /// <summary>
    /// 读取 32 位整数
    /// </summary>
    /// <returns>整数</returns>
    public BsonInt32 ReadInt32()
    {
        ThrowIfDisposed();
        return new BsonInt32(_reader.ReadInt32());
    }

    /// <summary>
    /// 读取 64 位整数
    /// </summary>
    /// <returns>整数</returns>
    public BsonInt64 ReadInt64()
    {
        ThrowIfDisposed();
        return new BsonInt64(_reader.ReadInt64());
    }

    /// <summary>
    /// 读取双精度浮点数
    /// </summary>
    /// <returns>浮点数</returns>
    public BsonDouble ReadDouble()
    {
        ThrowIfDisposed();
        return new BsonDouble(_reader.ReadDouble());
    }

    /// <summary>
    /// 读取布尔值
    /// </summary>
    /// <returns>布尔值</returns>
    public BsonBoolean ReadBoolean()
    {
        ThrowIfDisposed();
        var value = _reader.ReadByte();
        return new BsonBoolean(value != 0);
    }

    /// <summary>
    /// 读取 ObjectId
    /// </summary>
    /// <returns>ObjectId</returns>
    public BsonObjectId ReadObjectId()
    {
        ThrowIfDisposed();
        var bytes = _reader.ReadBytes(12);
        return new BsonObjectId(new ObjectId(bytes));
    }

    /// <summary>
    /// 读取 DateTime
    /// </summary>
    /// <returns>DateTime</returns>
    public BsonDateTime ReadDateTime()
    {
        ThrowIfDisposed();
        var milliseconds = _reader.ReadInt64();
        var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var dateTime = unixEpoch.AddMilliseconds(milliseconds);
        return new BsonDateTime(dateTime);
    }

    /// <summary>
    /// 检查是否已释放
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(BsonReader));
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        if (!_disposed)
        {
            _reader?.Dispose();
            _stream?.Dispose();
            _disposed = true;
        }
    }
}