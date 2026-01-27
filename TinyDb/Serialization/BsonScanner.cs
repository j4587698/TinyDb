using System;
using System.Buffers.Binary;
using System.Text;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// BSON 扫描器，提供对 BSON 二进制数据的快速查找和扫描功能。
/// </summary>
public static class BsonScanner
{
    /// <summary>
    /// 尝试从 BSON 文档二进制数据中直接获取指定字段的值。
    /// </summary>
    /// <param name="document">BSON 文档的二进制数据。</param>
    /// <param name="fieldName">要查找的字段名称。</param>
    /// <param name="value">输出获取到的 BSON 值。</param>
    /// <returns>如果找到字段则返回 true；否则返回 false。</returns>
    public static bool TryGetValue(ReadOnlySpan<byte> document, string fieldName, out BsonValue? value)
    {
        value = null;
        if (document.Length < 5) return false; 

        int offset = 4; // 跳过大小字段

        while (offset < document.Length)
        {
            var type = (BsonType)document[offset];
            offset++;

            if (type == BsonType.End) break;

            int nameEnd = offset;
            while (nameEnd < document.Length && document[nameEnd] != 0) nameEnd++;
            if (nameEnd >= document.Length) return false;

            int nameLen = nameEnd - offset;
            var nameSpan = document.Slice(offset, nameLen);
            string currentName = Encoding.UTF8.GetString(nameSpan);
            
            offset = nameEnd + 1;

            if (string.Equals(currentName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                value = ReadValue(type, document, ref offset);
                return true;
            }
            else
            {
                SkipValue(type, document, ref offset);
            }
        }

        return false;
    }

    /// <summary>
    /// 跳过当前 BSON 值。
    /// </summary>
    /// <param name="type">BSON 类型。</param>
    /// <param name="data">数据跨度。</param>
    /// <param name="offset">当前偏移量（引用）。</param>
    private static void SkipValue(BsonType type, ReadOnlySpan<byte> data, ref int offset)
    {
        switch (type)
        {
            case BsonType.Double: offset += 8; break;
            case BsonType.String:
            case BsonType.JavaScript:
            case BsonType.Symbol:
                int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset));
                offset += 4 + len; 
                break;
            case BsonType.Document:
            case BsonType.Array:
            case BsonType.JavaScriptWithScope:
                int size = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset));
                offset += size;
                break;
            case BsonType.Binary:
                int binLen = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset));
                offset += 4 + 1 + binLen;
                break;
            case BsonType.ObjectId: offset += 12; break;
            case BsonType.Boolean: offset += 1; break;
            case BsonType.DateTime:
            case BsonType.Timestamp:
            case BsonType.Int64: offset += 8; break;
            case BsonType.Decimal128: offset += 16; break;
            case BsonType.Null:
            case BsonType.Undefined:
            case BsonType.MinKey:
            case BsonType.MaxKey:
                break;
            case BsonType.RegularExpression:
                while (data[offset++] != 0) ;
                while (data[offset++] != 0) ;
                break;
            case BsonType.Int32: offset += 4; break;
            default: throw new NotSupportedException($"Unknown BSON type {type} at offset {offset}");
        }
    }

    /// <summary>
    /// 读取当前 BSON 值。
    /// </summary>
    /// <param name="type">BSON 类型。</param>
    /// <param name="data">数据跨度。</param>
    /// <param name="offset">当前偏移量（引用）。</param>
    /// <returns>读取的 BsonValue。</returns>
    private static BsonValue ReadValue(BsonType type, ReadOnlySpan<byte> data, ref int offset)
    {
        switch (type)
        {
            case BsonType.Double:
                var d = BitConverter.ToDouble(data.Slice(offset, 8));
                offset += 8;
                return new BsonDouble(d);
            case BsonType.String:
                int len = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset));
                var s = Encoding.UTF8.GetString(data.Slice(offset + 4, len - 1));
                offset += 4 + len;
                return new BsonString(s);
            case BsonType.Int32:
                var i = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset));
                offset += 4;
                return new BsonInt32(i);
            case BsonType.Int64:
                var l = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(offset));
                offset += 8;
                return new BsonInt64(l);
            case BsonType.Boolean:
                var b = data[offset] != 0;
                offset += 1;
                return new BsonBoolean(b);
            case BsonType.DateTime:
                var ms = BinaryPrimitives.ReadInt64LittleEndian(data.Slice(offset));
                offset += 8;
                var dt = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(ms);
                return new BsonDateTime(dt);
            case BsonType.Decimal128:
                 var lo = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(offset));
                 var hi = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(offset + 8));
                 offset += 16;
                 return new BsonDecimal128(new Decimal128(lo, hi));
            case BsonType.Null:
                return BsonNull.Value;
            case BsonType.ObjectId:
                var oidBytes = data.Slice(offset, 12).ToArray();
                offset += 12;
                return new BsonObjectId(new ObjectId(oidBytes));
            default:
                 // Fallback for complex types or others
                 int start = offset;
                 SkipValue(type, data, ref offset);
                 var valueData = data.Slice(start, offset - start).ToArray();
                 return type switch {
                     BsonType.Binary => new BsonBinary(valueData.Skip(5).ToArray(), (BsonBinary.BinarySubType)valueData[4]),
                     _ => BsonNull.Value 
                 };
        }
    }
}