using System.Buffers;
using System.Collections.Immutable;
using System.Text;
using System.Globalization;
using System.Collections.Frozen;
using TinyDb.Bson;
using Microsoft.IO;
using TinyDb.Utils;

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

    private static readonly SizeCalculator Calculator = new();

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

        var reader = new BsonSpanReader(data);
        var document = reader.ReadDocument();
        return document.ContainsKey("value") ? document["value"] : BsonNull.Value;
    }

    /// <summary>
    /// 获取可回收的内存流
    /// </summary>
    /// <returns>内存流</returns>
    public static Stream GetRecyclableStream()
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
        if (value == null) throw new ArgumentNullException(nameof(value));
        if (writer == null) throw new ArgumentNullException(nameof(writer));

        writer.WriteValueWithType(value);
    }

    /// <summary>
    /// 序列化 BsonValue 到流
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <param name="stream">输出流</param>
    public static void SerializeValue(BsonValue value, Stream stream)
    {
        if (value == null) throw new ArgumentNullException(nameof(value));
        if (stream == null) throw new ArgumentNullException(nameof(stream));

        using var writer = new BsonWriter(stream, true);
        writer.WriteValueWithType(value);
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

        using var buffer = new PooledBufferWriter();
        using var writer = new BsonWriter(buffer);
        writer.WriteDocument(document);
        return buffer.WrittenSpan.ToArray();
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
    /// 反序列化为 BsonDocument（仅加载指定字段）
    /// </summary>
    /// <param name="data">内存块</param>
    /// <param name="fields">需要加载的字段集合</param>
    /// <returns>BSON 文档</returns>
    public static BsonDocument DeserializeDocument(ReadOnlyMemory<byte> data, HashSet<string> fields)
    {
        if (fields == null) throw new ArgumentNullException(nameof(fields));
        if (data.IsEmpty) throw new InvalidDataException("BSON document payload cannot be empty.");

        var reader = new BsonSpanReader(data.Span);
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

        using var buffer = new PooledBufferWriter();
        using var writer = new BsonWriter(buffer);
        writer.WriteArray(array);
        return buffer.WrittenSpan.ToArray();
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

        var reader = new BsonSpanReader(data);
        return reader.ReadArray();
    }

    /// <summary>
    /// 计算序列化后的大小
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <returns>大小（字节）</returns>
    public static int CalculateSize(BsonValue value)
    {
        return Calculator.CalculateSize(value);
    }

    /// <summary>
    /// 计算文档序列化后的大小
    /// </summary>
    /// <param name="document">BSON 文档</param>
    /// <returns>大小（字节）</returns>
    public static int CalculateDocumentSize(BsonDocument document)
    {
        return Calculator.CalculateDocumentSize(document);
    }

    /// <summary>
    /// 计算数组序列化后的大小
    /// </summary>
    /// <param name="array">BSON 数组</param>
    /// <returns>大小（字节）</returns>
    public static int CalculateArraySize(BsonArray array)
    {
        return Calculator.CalculateArraySize(array);
    }
}
