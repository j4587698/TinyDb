using System;
using System.Buffers.Binary;
using System.Text;
using TinyDb.Bson;

namespace TinyDb.Serialization;

public static class BsonScanner
{
    /// <summary>
    /// 在原始 BSON 中定位字段。使用 UTF8 字节直接比较，零分配。
    /// </summary>
    public static bool TryLocateField(ReadOnlySpan<byte> document, ReadOnlySpan<byte> fieldNameBytes, out int valueOffset, out BsonType type)
    {
        valueOffset = 0;
        type = BsonType.Null;
        
        if (document.Length < 5) return false; 

        int offset = 4; // 跳过大小字段

        while (offset < document.Length)
        {
            type = (BsonType)document[offset];
            offset++;

            if (type == BsonType.End) break;

            int nameEnd = offset;
            while (nameEnd < document.Length && document[nameEnd] != 0) nameEnd++;
            if (nameEnd >= document.Length) return false;

            int nameLen = nameEnd - offset;
            var nameSpan = document.Slice(offset, nameLen);

            offset = nameEnd + 1;

            if (nameSpan.SequenceEqual(fieldNameBytes))
            {
                valueOffset = offset;
                return true;
            }

            if (!TrySkipValue(type, document, ref offset)) return false;
        }
        return false;
    }


    // 保留旧接口兼容性
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
                if (!TrySkipValue(type, document, ref offset))
                {
                    if (!IsKnownType(type)) throw new NotSupportedException($"Unknown BSON type {type} at offset {offset}");
                    return false;
                }
            }
        }

        return false;
    }

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
                var oidBytes = data.Slice(offset, 12);
                offset += 12;
                return new BsonObjectId(new ObjectId(oidBytes));
            default:
                // Fallback for complex types or others
                int start = offset;
                if (!TrySkipValue(type, data, ref offset))
                {
                    if (!IsKnownType(type)) throw new NotSupportedException($"Unknown BSON type {type} at offset {offset}");
                    return BsonNull.Value;
                }
                var valueData = data.Slice(start, offset - start).ToArray();
                return type switch
                {
                    BsonType.Binary => new BsonBinary(valueData.Skip(5).ToArray(), (BsonBinary.BinarySubType)valueData[4]),
                    _ => BsonNull.Value
                };
        }
    }

    internal static bool TrySkipValue(BsonType type, ReadOnlySpan<byte> data, ref int offset)
    {
        switch (type)
        {
            case BsonType.Double:
                return TryAdvance(data.Length, ref offset, 8);

            case BsonType.String:
            case BsonType.JavaScript:
            case BsonType.Symbol:
                if (!TryReadInt32(data, offset, out int len) || len < 0) return false;
                return TryAdvance(data.Length, ref offset, 4 + len);

            case BsonType.Document:
            case BsonType.Array:
            case BsonType.JavaScriptWithScope:
                if (!TryReadInt32(data, offset, out int size) || size < 0) return false;
                return TryAdvance(data.Length, ref offset, size);

            case BsonType.Binary:
                if (!TryReadInt32(data, offset, out int binLen) || binLen < 0) return false;
                return TryAdvance(data.Length, ref offset, 4 + 1 + binLen);

            case BsonType.ObjectId:
                return TryAdvance(data.Length, ref offset, 12);

            case BsonType.Boolean:
                return TryAdvance(data.Length, ref offset, 1);

            case BsonType.DateTime:
            case BsonType.Timestamp:
            case BsonType.Int64:
                return TryAdvance(data.Length, ref offset, 8);

            case BsonType.Decimal128:
                return TryAdvance(data.Length, ref offset, 16);

            case BsonType.Null:
            case BsonType.Undefined:
            case BsonType.MinKey:
            case BsonType.MaxKey:
                return true;

            case BsonType.RegularExpression:
                return TrySkipCString(data, ref offset) && TrySkipCString(data, ref offset);

            case BsonType.Int32:
                return TryAdvance(data.Length, ref offset, 4);

            default:
                return false;
        }
    }

    private static bool TryAdvance(int length, ref int offset, int count)
    {
        if (offset < 0) return false;
        int next = offset + count;
        if (next < offset) return false;
        if (next > length) return false;
        offset = next;
        return true;
    }

    private static bool TryReadInt32(ReadOnlySpan<byte> data, int offset, out int value)
    {
        value = default;
        if (offset < 0 || offset + 4 > data.Length) return false;
        value = BinaryPrimitives.ReadInt32LittleEndian(data.Slice(offset, 4));
        return true;
    }

    private static bool TrySkipCString(ReadOnlySpan<byte> data, ref int offset)
    {
        if (offset < 0 || offset >= data.Length) return false;

        int i = offset;
        while (i < data.Length && data[i] != 0) i++;
        if (i >= data.Length) return false;

        offset = i + 1;
        return true;
    }

    private static bool IsKnownType(BsonType type)
    {
        return type switch
        {
            BsonType.Double => true,
            BsonType.String => true,
            BsonType.Document => true,
            BsonType.Array => true,
            BsonType.Binary => true,
            BsonType.Undefined => true,
            BsonType.ObjectId => true,
            BsonType.Boolean => true,
            BsonType.DateTime => true,
            BsonType.Null => true,
            BsonType.RegularExpression => true,
            BsonType.JavaScript => true,
            BsonType.Symbol => true,
            BsonType.JavaScriptWithScope => true,
            BsonType.Int32 => true,
            BsonType.Timestamp => true,
            BsonType.Int64 => true,
            BsonType.Decimal128 => true,
            BsonType.MinKey => true,
            BsonType.MaxKey => true,
            BsonType.End => true,
            _ => false
        };
    }
}
