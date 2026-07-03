using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Immutable;
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
    private int _depth;
    private const int MaxBsonDepth = 128;
    private const int MaxBsonValueLength = 64 * 1024 * 1024;

    public int Remaining => _data.Length - _position;

    public BsonSpanReader(ReadOnlySpan<byte> data)
    {
        _data = data;
        _position = 0;
        _depth = 0;
    }

    public BsonDocument ReadDocument(HashSet<string>? fields = null)
    {
        EnterContainer();
        try
        {
            // 检查剩余长度是否至少包含大小(4) + 结束符(1)
            if (_data.Length < 5) throw new EndOfStreamException();

            int startPos = _position;
            int docSize = ReadInt32();

            if (docSize < 5 || docSize > _data.Length - startPos)
                throw new InvalidOperationException($"Invalid document size: {docSize}");

            var builder = ImmutableDictionary.CreateBuilder<string, BsonValue>();

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
                    builder[key] = value;
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

            return new BsonDocument(builder);
        }
        finally
        {
            ExitContainer();
        }
    }

    public BsonArray ReadArray()
    {
        EnterContainer();
        try
        {
            int startPos = _position;
            int arrSize = ReadInt32();
            if (arrSize < 5 || arrSize > _data.Length - startPos)
                throw new InvalidOperationException($"Invalid array size: {arrSize}");

            var builder = ImmutableList.CreateBuilder<BsonValue>();

            while (true)
            {
                var type = (BsonType)ReadByte();
                if (type == BsonType.End) break;

                ReadCString(); // 忽略数组索引键
                var value = ReadValue(type);
                builder.Add(value);
            }

            int actualSize = _position - startPos;
            if (actualSize != arrSize)
            {
                throw new InvalidOperationException($"Array size mismatch. Expected {arrSize}, read {actualSize}");
            }

            return new BsonArray(builder);
        }
        finally
        {
            ExitContainer();
        }
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
            BsonType.JavaScript => new BsonJavaScript(ReadString()),
            BsonType.Symbol => new BsonSymbol(ReadString()),
            BsonType.JavaScriptWithScope => ReadJavaScriptWithScope(),
            BsonType.MinKey => BsonMinKey.Value,
            BsonType.MaxKey => BsonMaxKey.Value,
            _ => throw new NotSupportedException($"Unsupported BSON type: {type}")
        };
    }

    private void SkipValue(BsonType type)
    {
        switch (type)
        {
            case BsonType.Double: EnsureAvailable(8); _position += 8; break;
            case BsonType.String:
            case BsonType.JavaScript:
            case BsonType.Symbol:
                int len = ReadInt32();
                ValidateLength(len, 1, "string");
                EnsureAvailableLength(len);
                _position += len; 
                break;
            case BsonType.JavaScriptWithScope:
                int totalSize = ReadInt32();
                ValidateLength(totalSize, 5, "JavaScriptWithScope");
                EnsureAvailableLength(totalSize - 4);
                _position += (totalSize - 4);
                break;
            case BsonType.Document:
            case BsonType.Array:
                int docLen = ReadInt32();
                ValidateLength(docLen, 5, "document");
                EnsureAvailableLength(docLen - 4);
                _position += (docLen - 4); // 减去刚刚读取的长度字节
                break;
            case BsonType.Binary:
                int binLen = ReadInt32();
                ValidateLength(binLen, 0, "binary");
                EnsureAvailable(1);
                _position += 1; // subtype
                EnsureAvailableLength(binLen);
                _position += binLen;
                break;
            case BsonType.ObjectId: EnsureAvailable(12); _position += 12; break;
            case BsonType.Boolean: EnsureAvailable(1); _position += 1; break;
            case BsonType.DateTime:
            case BsonType.Timestamp:
            case BsonType.Int64: EnsureAvailable(8); _position += 8; break;
            case BsonType.Null:
            case BsonType.MinKey:
            case BsonType.MaxKey: break;
            case BsonType.RegularExpression:
                SkipCString();
                SkipCString();
                break;
            case BsonType.Int32: EnsureAvailable(4); _position += 4; break;
            case BsonType.Decimal128: 
                EnsureAvailable(16);
                _position += 16;
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
        EnsureAvailable(4);
        var val = BinaryPrimitives.ReadInt32LittleEndian(_data.Slice(_position));
        _position += 4;
        return val;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public long ReadInt64()
    {
        EnsureAvailable(8);
        var val = BinaryPrimitives.ReadInt64LittleEndian(_data.Slice(_position));
        _position += 8;
        return val;
    }

    public double ReadDouble()
    {
        EnsureAvailable(8);
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
        EnsureAvailable(12);
        var bytes = _data.Slice(_position, 12);
        _position += 12;
        return new ObjectId(bytes);
    }

    public DateTime ReadDateTime()
    {
        return BsonDateTime.DecodeStoredValue(ReadInt64());
    }

    public string ReadCString()
    {
        var span = _data.Slice(_position);
        int idx = span.IndexOf((byte)0);
        if (idx < 0) throw new EndOfStreamException("CString null terminator not found");

        string s = BsonFieldName.Decode(span.Slice(0, idx));
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
        ValidateLength(len, 1, "string");
        EnsureAvailableLength(len);

        // 除去最后的 null
        string s = Encoding.UTF8.GetString(_data.Slice(_position, len - 1));
        _position += len;
        return s;
    }

    public BsonBinary ReadBinary()
    {
        int len = ReadInt32();
        ValidateLength(len, 0, "binary");
        byte subType = ReadByte();
        EnsureAvailableLength(len);
        
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
        EnsureAvailable(16);
        
        ulong lo = BinaryPrimitives.ReadUInt64LittleEndian(_data.Slice(_position));
        ulong hi = BinaryPrimitives.ReadUInt64LittleEndian(_data.Slice(_position + 8));
        _position += 16;
        
        return new BsonDecimal128(new Decimal128(lo, hi));
    }

    public BsonJavaScriptWithScope ReadJavaScriptWithScope()
    {
        int totalSize = ReadInt32();
        ValidateLength(totalSize, 5, "JavaScriptWithScope");
        int startPosition = _position - sizeof(int);
        string code = ReadString();
        var scope = ReadDocument();
        int actualSize = _position - startPosition;
        if (actualSize != totalSize)
        {
            throw new InvalidOperationException($"JavaScriptWithScope size mismatch. Expected {totalSize}, read {actualSize}");
        }
        return new BsonJavaScriptWithScope(code, scope);
    }

    private void EnsureAvailable(int count)
    {
        if (_position < 0 ||
            _position > _data.Length ||
            count < 0 ||
            (uint)count > (uint)(_data.Length - _position))
        {
            throw new EndOfStreamException();
        }
    }

    private void EnsureAvailableLength(int length)
    {
        ValidateLength(length, 0, "BSON value");
        EnsureAvailable(length);
    }

    private void ValidateLength(int length, int minimum, string valueName)
    {
        if (length < minimum)
        {
            throw new InvalidOperationException($"Invalid {valueName} length: {length}");
        }

        if (length > MaxBsonValueLength)
        {
            throw new InvalidOperationException($"{valueName} length exceeds {MaxBsonValueLength} bytes: {length}");
        }
    }

    private void EnterContainer()
    {
        if (++_depth > MaxBsonDepth)
        {
            throw new InvalidOperationException($"BSON nesting depth exceeds {MaxBsonDepth}.");
        }
    }

    private void ExitContainer()
    {
        if (_depth > 0)
        {
            _depth--;
        }
    }
}
