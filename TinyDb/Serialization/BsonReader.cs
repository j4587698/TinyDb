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
/// BSON 读取器，负责从流中读取 BSON 数据
/// </summary>
public sealed class BsonReader : IDisposable
{
    private readonly Stream _stream;
    private readonly BinaryReader _reader;
    private readonly bool _leaveOpen;
    private int _depth;
    private bool _disposed;
    private const int MaxBsonDepth = 128;
    private const int MaxBsonValueLength = 64 * 1024 * 1024;

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
        EnterContainer();

        try
        {
            var documentSize = ReadContainerLength("document");
            var startPosition = _stream.CanSeek ? _stream.Position : 0;
            var builder = new BsonDocumentBuilder();

            while (true)
            {
                var type = (BsonType)_reader.ReadByte();

                if (type == BsonType.End)
                    break;

                var key = ReadCString();
                var value = ReadTypedValue(type);

                builder.Set(key, value);
            }

            if (_stream.CanSeek)
            {
                var endPosition = _stream.Position;
                var expectedContentSize = documentSize - 4;
                var actualContentSize = (int)(endPosition - startPosition);

                if (actualContentSize != expectedContentSize)
                {
                    throw new InvalidOperationException($"Document size mismatch: expected {documentSize} (content: {expectedContentSize}), actual content size {actualContentSize}");
                }
            }

            return builder.Build();
        }
        finally
        {
            ExitContainer();
        }
    }

    /// <summary>
    /// 读取 BSON 文档（仅加载指定字段）
    /// </summary>
    /// <param name="fields">需要加载的字段集合</param>
    /// <returns>BSON 文档</returns>
    public BsonDocument ReadDocument(HashSet<string> fields)
    {
        ThrowIfDisposed();
        EnterContainer();

        try
        {
            var documentSize = ReadContainerLength("document");
            var startPosition = _stream.CanSeek ? _stream.Position : 0;
            var builder = new BsonDocumentBuilder();

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
                    builder.Set(key, value);
                }
                else
                {
                    SkipValue(type);
                }
            }

            if (_stream.CanSeek)
            {
                var endPosition = _stream.Position;
                var expectedContentSize = documentSize - 4;
                var actualContentSize = (int)(endPosition - startPosition);

                // 如果我们跳过了某些值，Position 应该是正确的，因为 SkipValue 也会消耗流
                if (actualContentSize != expectedContentSize)
                {
                    throw new InvalidOperationException($"Document size mismatch: expected {documentSize} (content: {expectedContentSize}), actual content size {actualContentSize}");
                }
            }

            return builder.Build();
        }
        finally
        {
            ExitContainer();
        }
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
                ValidateLength(strLen, 1, "string");
                SkipBytes(strLen); // strLen includes null terminator
                break;
            case BsonType.Decimal128:
                SkipBytes(16);
                break;
            case BsonType.Document:
            case BsonType.Array:
            case BsonType.JavaScriptWithScope:
                var docLen = _reader.ReadInt32();
                ValidateLength(docLen, 5, "document");
                SkipBytes(docLen - 4); // docLen includes the int32 size itself
                break;
            case BsonType.Binary:
                var binLen = _reader.ReadInt32();
                ValidateLength(binLen, 0, "binary");
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

        if (_stream.CanSeek)
        {
            try
            {
                if (_stream.Length - _stream.Position < count)
                {
                    throw new EndOfStreamException();
                }
            }
            catch (NotSupportedException)
            {
            }

            _stream.Seek(count, SeekOrigin.Current);
            return;
        }

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

    private int ReadContainerLength(string containerName)
    {
        var length = _reader.ReadInt32();
        ValidateLength(length, 5, containerName);
        return length;
    }

    private void ValidateLength(int length, int minimum, string valueName)
    {
        if (length < minimum || length > MaxBsonValueLength)
        {
            throw new InvalidDataException($"Invalid BSON {valueName} length: {length}.");
        }

    }

    private void EnterContainer()
    {
        if (++_depth > MaxBsonDepth)
        {
            _depth--;
            throw new InvalidDataException($"BSON nesting depth exceeds {MaxBsonDepth}.");
        }
    }

    private void ExitContainer()
    {
        _depth--;
    }

    /// <summary>
    /// 读取 BSON 数组
    /// </summary>
    /// <returns>BSON 数组</returns>
    public BsonArray ReadArray()
    {
        ThrowIfDisposed();
        EnterContainer();

        try
        {
            var arraySize = ReadContainerLength("array");
            var startPosition = _stream.CanSeek ? _stream.Position : 0;
            var values = new List<BsonValue>();

            while (true)
            {
                var type = (BsonType)_reader.ReadByte();

                if (type == BsonType.End)
                    break;

                var key = ReadCString(); // 数组索引
                var value = ReadTypedValue(type);

                values.Add(value);
            }

            if (_stream.CanSeek)
            {
                var endPosition = _stream.Position;
                var expectedContentSize = arraySize - 4;
                var actualContentSize = (int)(endPosition - startPosition);

                if (actualContentSize != expectedContentSize)
                {
                    throw new InvalidOperationException($"Array size mismatch: expected {arraySize} (content: {expectedContentSize}), actual content size {actualContentSize}");
                }
            }

            return new BsonArray(values);
        }
        finally
        {
            ExitContainer();
        }
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
                if (count >= MaxBsonValueLength)
                {
                    throw new InvalidDataException($"BSON CString length exceeds {MaxBsonValueLength} bytes.");
                }

                if (count >= buffer.Length)
                {
                    var newLength = Math.Min(buffer.Length * 2, MaxBsonValueLength);
                    var newBuffer = ArrayPool<byte>.Shared.Rent(newLength);
                    Buffer.BlockCopy(buffer, 0, newBuffer, 0, count);
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = newBuffer;
                }
                buffer[count++] = b;
            }

            return BsonFieldName.Decode(buffer.AsSpan(0, count));
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
        ValidateLength(length, 1, "string");
        var bytes = _reader.ReadBytes(length - 1); // 不包含 null 终止符
        if (bytes.Length != length - 1)
            throw new EndOfStreamException("Unexpected end of stream while reading BSON string.");
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
        return new BsonDateTime(BsonDateTime.DecodeStoredValue(_reader.ReadInt64()));
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
        ValidateLength(length, 0, "binary");
        var subType = (BsonBinary.BinarySubType)_reader.ReadByte();
        var bytes = _reader.ReadBytes(length);
        if (bytes.Length != length)
            throw new EndOfStreamException("Unexpected end of stream while reading BSON binary data.");
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
        var startPosition = _stream.CanSeek ? _stream.Position : 0;
        var totalSize = _reader.ReadInt32();
        ValidateLength(totalSize, 5, "JavaScriptWithScope");
        var code = ReadString().Value;
        var scope = ReadDocument();
        if (_stream.CanSeek)
        {
            var actualSize = (int)(_stream.Position - startPosition);
            if (actualSize != totalSize)
            {
                throw new InvalidOperationException($"JavaScriptWithScope size mismatch: expected {totalSize}, actual {actualSize}.");
            }
        }

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
