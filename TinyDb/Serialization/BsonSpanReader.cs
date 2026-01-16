using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using TinyDb.Bson;

namespace TinyDb.Serialization;

/// <summary>
/// 高性能 BSON 读取器，直接在 Span 上操作，避免流分配和内存拷贝
/// </summary>
public ref struct BsonSpanReader
{
    private ReadOnlySpan<byte> _data;
    private int _position;

    public BsonSpanReader(ReadOnlySpan<byte> data)
    {
        _data = data;
        _position = 0;
    }

    public BsonDocument ReadDocument(HashSet<string>? fields = null)
    {
        // 检查剩余长度是否至少包含大小(4) + 结束符(1)
        if (_data.Length < 5) throw new EndOfStreamException();

        int startPos = _position;
        int docSize = ReadInt32();
        
        if (docSize < 5 || docSize > _data.Length - startPos)
            throw new InvalidOperationException($"Invalid document size: {docSize}");

        var document = new BsonDocument();

        // 循环读取元素直到遇到结束符
        while (true)
        {
            var type = (BsonType)ReadByte();
            if (type == BsonType.End) break;

            // 优化：直接扫描 CString 长度
            var key = ReadCString();

            // 投影过滤
            if (fields == null || fields.Contains(key))
            {
                var value = ReadValue(type);
                document = document.Set(key, value);
            }
            else
            {
                SkipValue(type);
            }
        }

        int actualSize = _position - startPos;
        if (actualSize != docSize)
        {
            throw new InvalidOperationException($"Document size mismatch. Expected {docSize}, read {actualSize}");
        }

        return document;
    }

    public BsonArray ReadArray()
    {
        int startPos = _position;
        int arrSize = ReadInt32();
        var array = new BsonArray();

        while (true)
        {
            var type = (BsonType)ReadByte();
            if (type == BsonType.End) break;

            ReadCString(); // 忽略数组索引键
            var value = ReadValue(type);
            array = array.AddValue(value);
        }

        return array;
    }

    public BsonValue ReadValue(BsonType type)
    {
        return type switch
        {
            BsonType.Double => new BsonDouble(ReadDouble()),
            BsonType.String => new BsonString(ReadString()),
            BsonType.Document => ReadDocument(),
            BsonType.Array => ReadArray(),
            BsonType.Binary => ReadBinary(),
            BsonType.ObjectId => new BsonObjectId(ReadObjectId()),
            BsonType.Boolean => new BsonBoolean(ReadBoolean()),
            BsonType.DateTime => new BsonDateTime(ReadDateTime()),
            BsonType.Null => BsonNull.Value,
            BsonType.RegularExpression => ReadRegularExpression(),
            BsonType.Int32 => new BsonInt32(ReadInt32()),
            BsonType.Timestamp => new BsonTimestamp(ReadInt64()),
            BsonType.Int64 => new BsonInt64(ReadInt64()),
            BsonType.Decimal128 => ReadDecimal128(),
            BsonType.MinKey => BsonMinKey.Value,
            BsonType.MaxKey => BsonMaxKey.Value,
            _ => throw new NotSupportedException($"Unsupported BSON type: {type}")
        };
    }

    private void SkipValue(BsonType type)
    {
        switch (type)
        {
            case BsonType.Double: _position += 8; break;
            case BsonType.String:
            case BsonType.JavaScript:
            case BsonType.Symbol:
                int len = ReadInt32();
                _position += len; 
                break;
            case BsonType.Document:
            case BsonType.Array:
                int docLen = ReadInt32();
                _position += (docLen - 4); // 减去刚刚读取的长度字节
                break;
            case BsonType.Binary:
                int binLen = ReadInt32();
                _position += (binLen + 1); // +1 subtype
                break;
            case BsonType.ObjectId: _position += 12; break;
            case BsonType.Boolean: _position += 1; break;
            case BsonType.DateTime:
            case BsonType.Timestamp:
            case BsonType.Int64: _position += 8; break;
            case BsonType.Null:
            case BsonType.MinKey:
            case BsonType.MaxKey: break;
            case BsonType.RegularExpression:
                SkipCString();
                SkipCString();
                break;
            case BsonType.Int32: _position += 4; break;
            case BsonType.Decimal128: 
                // Decimal128 stored as string in this implementation? Or raw bytes?
                // Standard BSON is 128-bit. But current BsonReader implementation reads it as String (int32 length + bytes).
                // "ReadDecimal128 reads int32 length then bytes."
                int decLen = ReadInt32();
                _position += decLen;
                break;
            default: throw new NotSupportedException($"Cannot skip type {type}");
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public byte ReadByte()
    {
        if (_position >= _data.Length) throw new EndOfStreamException();
        return _data[_position++];
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public int ReadInt32()
    {
        if (_position + 4 > _data.Length) throw new EndOfStreamException();
        var val = BinaryPrimitives.ReadInt32LittleEndian(_data.Slice(_position));
        _position += 4;
        return val;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public long ReadInt64()
    {
        if (_position + 8 > _data.Length) throw new EndOfStreamException();
        var val = BinaryPrimitives.ReadInt64LittleEndian(_data.Slice(_position));
        _position += 8;
        return val;
    }

    public double ReadDouble()
    {
        if (_position + 8 > _data.Length) throw new EndOfStreamException();
        var val = BinaryPrimitives.ReadDoubleLittleEndian(_data.Slice(_position));
        _position += 8;
        return val;
    }

    public bool ReadBoolean()
    {
        return ReadByte() != 0;
    }

    public ObjectId ReadObjectId()
    {
        if (_position + 12 > _data.Length) throw new EndOfStreamException();
        // 直接从切片创建 ObjectId，避免数组分配
        // ObjectId 构造函数接受 byte[]，这里我们需要切片
        // 现有的 ObjectId(byte[]) 会复制。
        // 为了极致性能，ObjectId 应该支持 Span 构造函数。
        // 目前先分配数组：
        var bytes = _data.Slice(_position, 12).ToArray();
        _position += 12;
        return new ObjectId(bytes);
    }

    public DateTime ReadDateTime()
    {
        long ms = ReadInt64();
        return new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddMilliseconds(ms);
    }

    public string ReadCString()
    {
        var span = _data.Slice(_position);
        int idx = span.IndexOf((byte)0);
        if (idx < 0) throw new EndOfStreamException("CString null terminator not found");

        string s = Encoding.UTF8.GetString(span.Slice(0, idx));
        _position += idx + 1;
        return s;
    }

    private void SkipCString()
    {
        var span = _data.Slice(_position);
        int idx = span.IndexOf((byte)0);
        if (idx < 0) throw new EndOfStreamException();
        _position += idx + 1;
    }

    public string ReadString()
    {
        int len = ReadInt32(); // 包含 null
        if (len < 1) throw new InvalidOperationException($"Invalid string length: {len}");
        if (_position + len > _data.Length) throw new EndOfStreamException();

        // 除去最后的 null
        string s = Encoding.UTF8.GetString(_data.Slice(_position, len - 1));
        _position += len;
        return s;
    }

    public BsonBinary ReadBinary()
    {
        int len = ReadInt32();
        byte subType = ReadByte();
        if (_position + len > _data.Length) throw new EndOfStreamException();
        
        var bytes = _data.Slice(_position, len).ToArray();
        _position += len;
        return new BsonBinary(bytes, (BsonBinary.BinarySubType)subType);
    }

    public BsonRegularExpression ReadRegularExpression()
    {
        var pattern = ReadCString();
        var options = ReadCString();
        return new BsonRegularExpression(pattern, options);
    }

    public BsonDecimal128 ReadDecimal128()
    {
        // 兼容现有实现：作为字符串存储
        int len = ReadInt32();
        if (_position + len > _data.Length) throw new EndOfStreamException();
        
        string s = Encoding.UTF8.GetString(_data.Slice(_position, len - 1));
        _position += len;
        
        if (decimal.TryParse(s, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var d))
        {
            return new BsonDecimal128(d);
        }
        throw new InvalidOperationException($"Invalid decimal: {s}");
    }
}
