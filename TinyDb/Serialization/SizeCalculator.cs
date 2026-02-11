using System.Text;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// BSON 序列化大小计算器
/// </summary>
internal sealed class SizeCalculator
{
    /// <summary>
    /// 计算 BSON 值的序列化大小
    /// </summary>
    /// <param name="value">BSON 值</param>
    /// <returns>大小（字节）</returns>
    public int CalculateSize(BsonValue value)
    {
        return value switch
        {
            BsonNull => 0,
            BsonMinKey => 0,
            BsonMaxKey => 0,
            BsonString str => CalculateStringSize(str.Value),
            BsonInt32 => 4,
            BsonInt64 => 8,
            BsonDouble => 8,
            BsonBoolean => 1,
            BsonObjectId => 12,
            BsonDateTime dt => 8,
            BsonDocument doc => CalculateDocumentSize(doc),
            BsonArray arr => CalculateArraySize(arr),
            BsonDocumentValue docVal => CalculateDocumentSize(docVal.Value),
            BsonArrayValue arrVal => CalculateArraySize(arrVal.Value),
            BsonBinary b => 4 + 1 + b.Bytes.Length,
            BsonRegularExpression r => CalculateCStringSize(r.Pattern) + CalculateCStringSize(r.Options),
            BsonTimestamp => 8,
            BsonDecimal128 => 16, // Decimal128 is fixed 128-bit (16 bytes)
            BsonJavaScript js => CalculateStringSize(js.Code),
            BsonSymbol sym => CalculateStringSize(sym.Name),
            BsonJavaScriptWithScope jsScope => 4 + CalculateStringSize(jsScope.Code) + CalculateDocumentSize(jsScope.Scope),
            _ => throw new NotSupportedException($"BSON type {value.BsonType} is not supported")
        };
    }

    /// <summary>
    /// 计算 BSON 文档的序列化大小
    /// </summary>
    /// <param name="document">BSON 文档</param>
    /// <returns>大小（字节）</returns>
    public int CalculateDocumentSize(BsonDocument document)
    {
        if (document == null) throw new ArgumentNullException(nameof(document));

        var size = 4; // 文档大小（4字节）

        foreach (var kvp in document._elements)
        {
            size += 1; // 类型字节
            size += CalculateCStringSize(kvp.Key); // 键名
            size += CalculateSize(kvp.Value); // 值
        }

        size += 1; // 结束标记

        return size;
    }

    /// <summary>
    /// 计算 BSON 数组的序列化大小
    /// </summary>
    /// <param name="array">BSON 数组</param>
    /// <returns>大小（字节）</returns>
    public int CalculateArraySize(BsonArray array)
    {
        if (array == null) throw new ArgumentNullException(nameof(array));

        var size = 4; // 数组大小（4字节）

        for (int i = 0; i < array.Count; i++)
        {
            size += 1; // 类型字节
            size += CalculateInt32CStringSize(i); // 索引作为键名
            size += CalculateSize(array[i]); // 值
        }

        size += 1; // 结束标记

        return size;
    }

    /// <summary>
    /// 计算字符串的序列化大小
    /// </summary>
    /// <param name="value">字符串值</param>
    /// <returns>大小（字节）</returns>
    private static int CalculateStringSize(string value)
    {
        if (value == null) return 0;

        var byteCount = Encoding.UTF8.GetByteCount(value);
        return 4 + byteCount + 1; // 长度 + 字节 + null 终止符
    }

    /// <summary>
    /// 计算 C 字符串的序列化大小
    /// </summary>
    /// <param name="value">字符串值</param>
    /// <returns>大小（字节）</returns>
    private static int CalculateCStringSize(string value)
    {
        if (value == null) return 0;

        if (BsonSerializer.CommonKeyCache.TryGetValue(value, out var cachedBytes))
        {
            return cachedBytes.Length + 1; // 字节 + null 终止符
        }

        var byteCount = Encoding.UTF8.GetByteCount(value);
        return byteCount + 1; // 字节 + null 终止符
    }

    private static int CalculateInt32CStringSize(int value)
    {
        if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));

        uint v = (uint)value;
        int digits = 1;

        while (v >= 10)
        {
            v /= 10;
            digits++;
        }

        return digits + 1; // digits + null 终止符
    }
}
