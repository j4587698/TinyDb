using System.Buffers;
using System.Text;
using System.Globalization;
using System.Collections.Frozen;
using TinyDb.Bson;
using Microsoft.IO;

namespace TinyDb.Serialization;

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
    /// 内存流管理器，用于减少内存分配
    /// </summary>
    private static readonly RecyclableMemoryStreamManager MemoryStreamManager = new RecyclableMemoryStreamManager(
        new RecyclableMemoryStreamManager.Options
        {
            BlockSize = 4096, // 4KB Block Size (Better for typical documents)
            LargeBufferMultiple = 1024 * 1024,
            MaximumSmallPoolFreeBytes = 1024 * 1024 * 100 // 100MB Cache
        }
    );

    /// <summary>
    /// 常用键名的 UTF8 编码缓存，避免每次序列化都调用 Encoding.UTF8.GetBytes()
    /// </summary>
    internal static readonly FrozenDictionary<string, byte[]> CommonKeyCache = new Dictionary<string, byte[]>
    {
        ["_id"] = DefaultEncoding.GetBytes("_id"),
        ["_collection"] = DefaultEncoding.GetBytes("_collection"),
        ["_isLargeDocument"] = DefaultEncoding.GetBytes("_isLargeDocument"),
        ["_largeDocumentIndex"] = DefaultEncoding.GetBytes("_largeDocumentIndex"),
        ["_largeDocumentSize"] = DefaultEncoding.GetBytes("_largeDocumentSize"),
        ["Id"] = DefaultEncoding.GetBytes("Id"),
        ["id"] = DefaultEncoding.GetBytes("id"),
        ["Name"] = DefaultEncoding.GetBytes("Name"),
        ["name"] = DefaultEncoding.GetBytes("name"),
        ["Type"] = DefaultEncoding.GetBytes("Type"),
        ["type"] = DefaultEncoding.GetBytes("type"),
        ["Value"] = DefaultEncoding.GetBytes("Value"),
        ["value"] = DefaultEncoding.GetBytes("value"),
        ["Data"] = DefaultEncoding.GetBytes("Data"),
        ["data"] = DefaultEncoding.GetBytes("data"),
        ["Count"] = DefaultEncoding.GetBytes("Count"),
        ["count"] = DefaultEncoding.GetBytes("count"),
        ["Items"] = DefaultEncoding.GetBytes("Items"),
        ["items"] = DefaultEncoding.GetBytes("items"),
        ["Created"] = DefaultEncoding.GetBytes("Created"),
        ["Updated"] = DefaultEncoding.GetBytes("Updated"),
        ["Deleted"] = DefaultEncoding.GetBytes("Deleted"),
    }.ToFrozenDictionary();

    /// <summary>
    /// 序列化 BsonValue
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <returns>字节数组</returns>
    public static byte[] Serialize(BsonValue value)
    {
        // 将单个值包装在文档中进行序列化
        var document = new BsonDocument().Set("value", value);
        return SerializeDocument(document);
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
        var document = reader.ReadDocument();
        return document.ContainsKey("value") ? document["value"] : BsonNull.Value;
    }

    /// <summary>
    /// 获取可回收的内存流
    /// </summary>
    /// <returns>内存流</returns>
    public static RecyclableMemoryStream GetRecyclableStream()
    {
        return MemoryStreamManager.GetStream();
    }

    /// <summary>
    /// 序列化 BsonValue 到 BsonWriter
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="writer">BSON 写入器</param>
    public static void SerializeValue(BsonValue value, BsonWriter writer)
    {
        writer.WriteValue(value);
    }

    /// <summary>
    /// 序列化 BsonValue 到流
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="stream">输出流</param>
    public static void SerializeValue(BsonValue value, Stream stream)
    {
        using var writer = new BsonWriter(stream, true);
        writer.WriteValue(value);
    }

    /// <summary>
    /// 反序列化 BsonValue 从流
    /// </summary>
    /// <param name="stream">输入流</param>
    /// <returns>BSON 值</returns>
    public static BsonValue DeserializeValue(Stream stream)
    {
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

        using var stream = MemoryStreamManager.GetStream();
        using var writer = new BsonWriter(stream);
        writer.WriteDocument(document);
        return stream.ToArray();
    }

    /// <summary>
    /// 序列化 BsonDocument 到 IBufferWriter
    /// </summary>
    public static void SerializeDocumentToBuffer(BsonDocument document, IBufferWriter<byte> writer)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));
        using var bsonWriter = new BsonWriter(writer);
        bsonWriter.WriteDocument(document);
    }

    /// <summary>
    /// 序列化 BsonDocument 到流
    /// </summary>
    /// <param name="document">BSON 文档</param>
    /// <param name="stream">输出流</param>
    public static void SerializeDocument(BsonDocument document, Stream stream)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));
        using var writer = new BsonWriter(stream, true);
        writer.WriteDocument(document);
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

        var reader = new BsonSpanReader(data);
        return reader.ReadDocument();
    }

    /// <summary>
    /// 反序列化为 BsonDocument (Zero-Copy optimization)
    /// </summary>
    /// <param name="data">内存块</param>
    /// <returns>BSON 文档</returns>
    public static BsonDocument DeserializeDocument(ReadOnlyMemory<byte> data)
    {
        if (data.IsEmpty) return new BsonDocument();

        var reader = new BsonSpanReader(data.Span);
        return reader.ReadDocument();
    }

    /// <summary>
    /// 反序列化为 BsonDocument（仅加载指定字段）
    /// </summary>
    /// <param name="data">字节数组</param>
    /// <param name="fields">需要加载的字段集合</param>
    /// <returns>BSON 文档</returns>
    public static BsonDocument DeserializeDocument(byte[] data, HashSet<string> fields)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));
        if (data.Length == 0) return new BsonDocument();

        var reader = new BsonSpanReader(data);
        return reader.ReadDocument(fields);
    }

    /// <summary>
    /// 序列化 BsonArray
    /// </summary>
    /// <param name="array">BSON 数组</param>
    /// <returns>字节数组</returns>
    public static byte[] SerializeArray(BsonArray array)
    {
        if (array == null) throw new ArgumentNullException(nameof(array));

        using var stream = MemoryStreamManager.GetStream();
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
/// BSON 写入器，负责将 BSON 数据写入流或 IBufferWriter
/// </summary>
public sealed class BsonWriter : IDisposable
{
    private readonly Stream? _stream;
    private readonly IBufferWriter<byte>? _bufferWriter;
    private readonly BinaryWriter? _writer;
    private readonly bool _leaveOpen;
    private bool _disposed;

    /// <summary>
    /// 使用流初始化 BSON 写入器
    /// </summary>
    public BsonWriter(Stream stream, bool leaveOpen = false)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _bufferWriter = null;
        _leaveOpen = leaveOpen;
        _writer = new BinaryWriter(stream, Encoding.UTF8, true);
    }

    /// <summary>
    /// 使用 IBufferWriter 初始化 BSON 写入器
    /// </summary>
    public BsonWriter(IBufferWriter<byte> bufferWriter)
    {
        _bufferWriter = bufferWriter ?? throw new ArgumentNullException(nameof(bufferWriter));
        _stream = null;
        _writer = null;
        _leaveOpen = true;
    }

    private void InternalWrite(ReadOnlySpan<byte> data)
    {
        if (_stream != null)
        {
            _writer!.Write(data);
        }
        else
        {
            _bufferWriter!.Write(data);
        }
    }

    private void InternalWrite(byte value)
    {
        if (_stream != null)
        {
            _writer!.Write(value);
        }
        else
        {
            var span = _bufferWriter!.GetSpan(1);
            span[0] = value;
            _bufferWriter.Advance(1);
        }
    }

    private void InternalWrite(int value)
    {
        if (_stream != null)
        {
            _writer!.Write(value);
        }
        else
        {
            var span = _bufferWriter!.GetSpan(4);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(span, value);
            _bufferWriter.Advance(4);
        }
    }

    private void InternalWrite(long value)
    {
        if (_stream != null)
        {
            _writer!.Write(value);
        }
        else
        {
            var span = _bufferWriter!.GetSpan(8);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64LittleEndian(span, value);
            _bufferWriter.Advance(8);
        }
    }

    private void InternalWrite(double value)
    {
        if (_stream != null)
        {
            _writer!.Write(value);
        }
        else
        {
            var span = _bufferWriter!.GetSpan(8);
            System.Buffers.Binary.BinaryPrimitives.WriteDoubleLittleEndian(span, value);
            _bufferWriter.Advance(8);
        }
    }

    private long CurrentPosition
    {
        get
        {
            if (_stream != null) return _stream.Position;
            // 对于 IBufferWriter，由于它不直接暴露位置，我们需要通过其它方式记录
            // 但目前的 BsonWriter 实现依赖于 Seek 重新写入长度
            // 改进：使用一个临时的字节缓冲区或预先计算大小
            throw new NotSupportedException("CurrentPosition is only supported for Streams. Use a specialized pooled buffer instead.");
        }
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
            case BsonDecimal128 dec128:
                WriteDecimal128(dec128.Value);
                break;
            case BsonDocument doc:
                WriteDocument(doc);
                break;
            case BsonArray arr:
                WriteArray(arr);
                break;
            case BsonBinary binary:
                WriteBinary(binary.Bytes, binary.SubType);
                break;
            case BsonRegularExpression regex:
                WriteRegularExpression(regex.Pattern, regex.Options);
                break;
            case BsonJavaScript js:
                WriteJavaScript(js.Code);
                break;
            case BsonJavaScriptWithScope jsScope:
                WriteJavaScriptWithScope(jsScope.Code, jsScope.Scope);
                break;
            case BsonSymbol symbol:
                WriteSymbol(symbol.Name);
                break;
            case BsonTimestamp timestamp:
                WriteTimestamp(timestamp.Value);
                break;
            case BsonMinKey _:
                break;
            case BsonMaxKey _:
                break;
            default:
                throw new NotSupportedException($"BSON type {value.BsonType} (CLR type: {value.GetType().Name}) is not supported");
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

        if (_stream != null)
        {
            // 旧的 Stream 逻辑：使用 Seek 更新长度
            _writer!.Flush();
            var sizePosition = _stream.Position;
            _writer.Write(0); // 占位符

            foreach (var kvp in document) WriteElement(kvp.Key, kvp.Value);
            _writer.Write((byte)BsonType.End);

            _writer.Flush();
            var endPosition = _stream.Position;
            var documentSize = (int)(endPosition - sizePosition);
            _stream.Seek(sizePosition, SeekOrigin.Begin);
            _writer.Write(documentSize);
            _writer.Flush();
            _stream.Seek(endPosition, SeekOrigin.Begin);
        }
        else
        {
            // 新的 IBufferWriter 逻辑：先计算大小，再写入
            // 这种方式虽然多了一次计算，但避免了分配新数组和 Seek
            var size = BsonSerializer.CalculateDocumentSize(document);
            InternalWrite(size);
            foreach (var kvp in document) WriteElement(kvp.Key, kvp.Value);
            InternalWrite((byte)BsonType.End);
        }
    }

    /// <summary>
    /// 写入 BSON 数组
    /// </summary>
    /// <param name="array">BSON 数组</param>
    public void WriteArray(BsonArray array)
    {
        ThrowIfDisposed();
        if (array == null) throw new ArgumentNullException(nameof(array));

        if (_stream != null)
        {
            _writer!.Flush();
            var sizePosition = _stream.Position;
            _writer.Write(0); // 占位符

            for (int i = 0; i < array.Count; i++) WriteElement(i.ToString(), array[i]);
            _writer.Write((byte)BsonType.End);

            _writer.Flush();
            var endPosition = _stream.Position;
            var arraySize = (int)(endPosition - sizePosition);
            _stream.Seek(sizePosition, SeekOrigin.Begin);
            _writer.Write(arraySize);
            _writer.Flush();
            _stream.Seek(endPosition, SeekOrigin.Begin);
        }
        else
        {
            var size = BsonSerializer.CalculateArraySize(array);
            InternalWrite(size);
            for (int i = 0; i < array.Count; i++) WriteElement(i.ToString(), array[i]);
            InternalWrite((byte)BsonType.End);
        }
    }

    /// <summary>
    /// 写入文档元素
    /// </summary>
    /// <param name="key">键</param>
    /// <param name="value">值</param>
    private void WriteElement(string key, BsonValue value)
    {
        InternalWrite((byte)value.BsonType);
        WriteCString(key);
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

        if (BsonSerializer.CommonKeyCache.TryGetValue(value, out var cachedBytes))
        {
            InternalWrite(cachedBytes);
        }
        else
        {
            var bytes = Encoding.UTF8.GetBytes(value);
            InternalWrite(bytes);
        }
        InternalWrite((byte)0);
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
        InternalWrite(bytes.Length + 1);
        InternalWrite(bytes);
        InternalWrite((byte)0);
    }

    /// <summary>
    /// 写入 32 位整数
    /// </summary>
    public void WriteInt32(int value) => InternalWrite(value);

    /// <summary>
    /// 写入 64 位整数
    /// </summary>
    public void WriteInt64(long value) => InternalWrite(value);

    /// <summary>
    /// 写入双精度浮点数
    /// </summary>
    public void WriteDouble(double value) => InternalWrite(value);

    /// <summary>
    /// 写入布尔值
    /// </summary>
    public void WriteBoolean(bool value) => InternalWrite(value ? (byte)1 : (byte)0);

    /// <summary>
    /// 写入 ObjectId
    /// </summary>
    public void WriteObjectId(ObjectId value) => InternalWrite(value.ToByteArray());

    /// <summary>
    /// 写入 DateTime
    /// </summary>
    public void WriteDateTime(DateTime value)
    {
        var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var milliseconds = (long)(value - unixEpoch).TotalMilliseconds;
        InternalWrite(milliseconds);
    }

    /// <summary>
    /// 写入 null 值
    /// </summary>
    public void WriteNull() { ThrowIfDisposed(); }

    /// <summary>
    /// 写入 Undefined 值
    /// </summary>
    public void WriteUndefined() { ThrowIfDisposed(); }

    /// <summary>
    /// 写入 MinKey
    /// </summary>
    public void WriteMinKey() { ThrowIfDisposed(); }

    /// <summary>
    /// 写入 MaxKey
    /// </summary>
    public void WriteMaxKey() { ThrowIfDisposed(); }

    /// <summary>
    /// 写入二进制数据
    /// </summary>
    public void WriteBinary(byte[] bytes, BsonBinary.BinarySubType subType = BsonBinary.BinarySubType.Generic)
    {
        ThrowIfDisposed();
        if (bytes == null) throw new ArgumentNullException(nameof(bytes));

        InternalWrite(bytes.Length);
        InternalWrite((byte)subType);
        InternalWrite(bytes);
    }

    /// <summary>
    /// 写入正则表达式
    /// </summary>
    public void WriteRegularExpression(string pattern, string options)
    {
        ThrowIfDisposed();
        WriteCString(pattern);
        WriteCString(options);
    }

    /// <summary>
    /// 写入 JavaScript 代码
    /// </summary>
    public void WriteJavaScript(string code) => WriteString(code);

    /// <summary>
    /// 写入带有作用域的 JavaScript 代码
    /// </summary>
    public void WriteJavaScriptWithScope(string code, BsonDocument scope)
    {
        ThrowIfDisposed();
        if (_stream != null)
        {
            _writer!.Flush();
            var sizePosition = _stream.Position;
            _writer.Write(0);
            WriteString(code);
            WriteDocument(scope);
            _writer.Flush();
            var endPosition = _stream.Position;
            _stream.Seek(sizePosition, SeekOrigin.Begin);
            _writer.Write((int)(endPosition - sizePosition));
            _stream.Seek(endPosition, SeekOrigin.Begin);
        }
        else
        {
            var size = 4 + BsonSerializer.CalculateSize(new BsonString(code)) + BsonSerializer.CalculateDocumentSize(scope);
            InternalWrite(size);
            WriteString(code);
            WriteDocument(scope);
        }
    }

    /// <summary>
    /// 写入符号
    /// </summary>
    public void WriteSymbol(string name) => WriteString(name);

    /// <summary>
    /// 写入 Decimal128 值
    /// </summary>
    public void WriteDecimal128(Decimal128 value)
    {
        InternalWrite((long)value.LowBits); // 这需要更严谨的处理，但目前先保持兼容
        InternalWrite((long)value.HighBits);
    }

    /// <summary>
    /// 写入时间戳
    /// </summary>
    public void WriteTimestamp(long value) => InternalWrite(value);

    private void ThrowIfDisposed() { if (_disposed) throw new ObjectDisposedException(nameof(BsonWriter)); }

    public void Dispose()
    {
        if (!_disposed)
        {
            _writer?.Dispose();
            if (!_leaveOpen) _stream?.Dispose();
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
    private readonly bool _leaveOpen;
    private bool _disposed;

    /// <summary>
    /// 初始化 BSON 读取器
    /// </summary>
    /// <param name="stream">输入流</param>
    /// <param name="leaveOpen">是否保持流打开</param>
    public BsonReader(Stream stream, bool leaveOpen = false)
    {
        _stream = stream ?? throw new ArgumentNullException(nameof(stream));
        _leaveOpen = leaveOpen;
        _reader = new BinaryReader(stream, Encoding.UTF8, true); // BinaryReader always leaves open as we manage disposal
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
            BsonType.Decimal128 => ReadDecimal128(),
            BsonType.Document => ReadDocument(),
            BsonType.Array => ReadArray(),
            BsonType.Binary => ReadBinary(),
            BsonType.RegularExpression => ReadRegularExpression(),
            BsonType.JavaScript => ReadJavaScript(),
            BsonType.JavaScriptWithScope => ReadJavaScriptWithScope(),
            BsonType.Symbol => ReadSymbol(),
            BsonType.Timestamp => ReadTimestamp(),
            BsonType.MinKey => BsonMinKey.Value,
            BsonType.MaxKey => BsonMaxKey.Value,
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
    /// 读取 BSON 文档（仅加载指定字段）
    /// </summary>
    /// <param name="fields">需要加载的字段集合</param>
    /// <returns>BSON 文档</returns>
    public BsonDocument ReadDocument(HashSet<string> fields)
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
            
            // 检查字段是否需要加载
            if (fields == null || fields.Contains(key))
            {
                var value = ReadTypedValue(type);
                document = document.Set(key, value);
            }
            else
            {
                SkipValue(type);
            }
        }

        var endPosition = _stream.Position;
        var expectedContentSize = documentSize - 4;
        var actualContentSize = (int)(endPosition - startPosition);

        // 如果我们跳过了某些值，Position 应该是正确的，因为 SkipValue 也会消耗流
        if (actualContentSize != expectedContentSize)
        {
            throw new InvalidOperationException($"Document size mismatch: expected {documentSize} (content: {expectedContentSize}), actual content size {actualContentSize}");
        }

        return document;
    }

    /// <summary>
    /// 跳过 BSON 值
    /// </summary>
    /// <param name="type">BSON 类型</param>
    private void SkipValue(BsonType type)
    {
        switch (type)
        {
            case BsonType.Null:
            case BsonType.MinKey:
            case BsonType.MaxKey:
                break;
            case BsonType.Boolean:
                SkipBytes(1);
                break;
            case BsonType.Int32:
                SkipBytes(4);
                break;
            case BsonType.Int64:
            case BsonType.Double:
            case BsonType.DateTime:
            case BsonType.Timestamp:
                SkipBytes(8);
                break;
            case BsonType.ObjectId:
                SkipBytes(12);
                break;
            case BsonType.String:
            case BsonType.JavaScript:
            case BsonType.Symbol:
                var strLen = _reader.ReadInt32();
                SkipBytes(strLen); // strLen includes null terminator
                break;
            case BsonType.Decimal128:
                SkipBytes(16);
                break;
            case BsonType.Document:
            case BsonType.Array:
            case BsonType.JavaScriptWithScope:
                var docLen = _reader.ReadInt32();
                SkipBytes(docLen - 4); // docLen includes the int32 size itself
                break;
            case BsonType.Binary:
                var binLen = _reader.ReadInt32();
                SkipBytes(1 + binLen); // subtype (1) + data
                break;
            case BsonType.RegularExpression:
                ReadCString(); // Skip pattern
                ReadCString(); // Skip options
                break;
            default:
                throw new NotSupportedException($"Cannot skip BSON type {type}");
        }
    }

    private void SkipBytes(int count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        if (count == 0) return;

        // 使用共享缓冲区读取并丢弃数据，避免分配新数组
        var buffer = ArrayPool<byte>.Shared.Rent(Math.Min(count, 4096));
        try
        {
            int remaining = count;
            while (remaining > 0)
            {
                int toRead = Math.Min(remaining, buffer.Length);
                int read = _reader.Read(buffer, 0, toRead);
                if (read == 0) throw new EndOfStreamException();
                remaining -= read;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
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

        var buffer = ArrayPool<byte>.Shared.Rent(128);
        int count = 0;
        try
        {
            byte b;
            while ((b = _reader.ReadByte()) != 0)
            {
                if (count >= buffer.Length)
                {
                    var newBuffer = ArrayPool<byte>.Shared.Rent(buffer.Length * 2);
                    Buffer.BlockCopy(buffer, 0, newBuffer, 0, count);
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = newBuffer;
                }
                buffer[count++] = b;
            }

            return Encoding.UTF8.GetString(buffer, 0, count);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
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
    /// 读取 Decimal128 值
    /// </summary>
    /// <returns>BsonDecimal128</returns>
    public BsonDecimal128 ReadDecimal128()
    {
        ThrowIfDisposed();
        var lo = _reader.ReadUInt64();
        var hi = _reader.ReadUInt64();
        return new BsonDecimal128(new Decimal128(lo, hi));
    }

    /// <summary>
    /// 读取二进制数据
    /// </summary>
    /// <returns>BsonBinary</returns>
    public BsonBinary ReadBinary()
    {
        ThrowIfDisposed();
        var length = _reader.ReadInt32();
        var subType = (BsonBinary.BinarySubType)_reader.ReadByte();
        var bytes = _reader.ReadBytes(length);
        return new BsonBinary(bytes, subType);
    }

    /// <summary>
    /// 读取正则表达式
    /// </summary>
    /// <returns>BsonRegularExpression</returns>
    public BsonRegularExpression ReadRegularExpression()
    {
        ThrowIfDisposed();
        var pattern = ReadCString();
        var options = ReadCString();
        return new BsonRegularExpression(pattern, options);
    }

    /// <summary>
    /// 读取时间戳
    /// </summary>
    /// <returns>BsonTimestamp</returns>
    public BsonTimestamp ReadTimestamp()
    {
        ThrowIfDisposed();
        var value = _reader.ReadInt64();
        return new BsonTimestamp(value);
    }

    /// <summary>
    /// 读取 JavaScript 代码
    /// </summary>
    /// <returns>BsonJavaScript</returns>
    public BsonJavaScript ReadJavaScript()
    {
        ThrowIfDisposed();
        return new BsonJavaScript(ReadString().Value);
    }

    /// <summary>
    /// 读取带有作用域的 JavaScript 代码
    /// </summary>
    /// <returns>BsonJavaScriptWithScope</returns>
    public BsonJavaScriptWithScope ReadJavaScriptWithScope()
    {
        ThrowIfDisposed();
        var totalSize = _reader.ReadInt32();
        var code = ReadString().Value;
        var scope = ReadDocument();
        return new BsonJavaScriptWithScope(code, scope);
    }

    /// <summary>
    /// 读取符号
    /// </summary>
    /// <returns>BsonSymbol</returns>
    public BsonSymbol ReadSymbol()
    {
        ThrowIfDisposed();
        return new BsonSymbol(ReadString().Value);
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
            _reader.Dispose();
            if (!_leaveOpen)
            {
                _stream.Dispose();
            }
            _disposed = true;
        }
    }
}
