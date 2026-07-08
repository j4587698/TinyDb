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
/// BSON 写入器，负责将 BSON 数据写入流或 IBufferWriter
/// </summary>
public sealed class BsonWriter : IDisposable
{
    private readonly Stream? _stream;
    private readonly IBufferWriter<byte>? _bufferWriter;
    private readonly BinaryWriter? _writer;
    private readonly bool _leaveOpen;
    private const int MaxBsonDepth = 128;
    private int _depth;
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

    private void PatchStreamInt32LittleEndian(long position, int value)
    {
        var currentPosition = _stream!.Position;
        _stream.Position = position;
        _writer!.Write(value);
        _stream.Position = currentPosition;
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

    private void InternalWrite(ulong value)
    {
        if (_stream != null)
        {
            _writer!.Write(value);
        }
        else
        {
            var span = _bufferWriter!.GetSpan(8);
            System.Buffers.Binary.BinaryPrimitives.WriteUInt64LittleEndian(span, value);
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
            if (_stream != null && _stream.CanSeek) return _stream.Position;
            // 对于 IBufferWriter，由于它不直接暴露位置，我们需要通过其它方式记录
            // 但目前的 BsonWriter 实现依赖于 Seek 重新写入长度
            // 改进：使用一个临时的字节缓冲区或预先计算大小
            throw new NotSupportedException("CurrentPosition is only supported for Streams. Use a specialized pooled buffer instead.");
        }
    }

    /// <summary>
    /// 写入包含 BSON 类型字节的值。
    /// </summary>
    public void WriteValueWithType(BsonValue value)
    {
        ThrowIfDisposed();
        if (value == null) throw new ArgumentNullException(nameof(value));

        InternalWrite((byte)value.BsonType);
        WriteValue(value);
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
            case BsonDocumentValue docVal:
                WriteDocument(docVal.Value);
                break;
            case BsonArrayValue arrVal:
                WriteArray(arrVal.Value);
                break;
            case BsonDocument doc:
                WriteDocument(doc);
                break;
            case BsonArray arr:
                WriteArray(arr);
                break;
            case BsonBinary binary:
                WriteBinary(binary.BytesSpan, binary.SubType);
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
        EnterContainer();

        try
        {
        if (_stream != null)
        {
            if (_stream.CanSeek)
            {
                var sizePosition = _stream.Position;
                InternalWrite(0);
                foreach (var kvp in document.Entries) WriteElement(kvp.Key, kvp.Value);
                InternalWrite((byte)BsonType.End);

                var endPosition = _stream.Position;
                PatchStreamInt32LittleEndian(sizePosition, checked((int)(endPosition - sizePosition)));
                return;
            }

            using var buffer = new PooledBufferWriter();
            using (var bufferedWriter = new BsonWriter(buffer))
            {
                bufferedWriter.WriteDocument(document);
            }

            InternalWrite(buffer.WrittenSpan);
        }
        else
        {
            if (_bufferWriter is IPatchableBufferWriter patchable)
            {
                int sizePosition = patchable.WrittenCount;
                InternalWrite(0); // 占位符

                foreach (var kvp in document.Entries) WriteElement(kvp.Key, kvp.Value);
                InternalWrite((byte)BsonType.End);

                int endPosition = patchable.WrittenCount;
                patchable.WriteInt32LittleEndianAt(sizePosition, endPosition - sizePosition);
                return;
            }

            // 通用 IBufferWriter：先写入可回填缓冲区，再一次性写出。
            using var buffer = new PooledBufferWriter();
            using (var bufferedWriter = new BsonWriter(buffer))
            {
                bufferedWriter.WriteDocument(document);
            }

            InternalWrite(buffer.WrittenSpan);
        }
        }
        finally
        {
            ExitContainer();
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
        EnterContainer();

        try
        {
        if (_stream != null)
        {
            if (_stream.CanSeek)
            {
                var sizePosition = _stream.Position;
                InternalWrite(0);
                for (int i = 0; i < array.Count; i++) WriteArrayElement(i, array[i]);
                InternalWrite((byte)BsonType.End);

                var endPosition = _stream.Position;
                PatchStreamInt32LittleEndian(sizePosition, checked((int)(endPosition - sizePosition)));
                return;
            }

            using var buffer = new PooledBufferWriter();
            using (var bufferedWriter = new BsonWriter(buffer))
            {
                bufferedWriter.WriteArray(array);
            }

            InternalWrite(buffer.WrittenSpan);
        }
        else
        {
            if (_bufferWriter is IPatchableBufferWriter patchable)
            {
                int sizePosition = patchable.WrittenCount;
                InternalWrite(0); // 占位符

                for (int i = 0; i < array.Count; i++) WriteArrayElement(i, array[i]);
                InternalWrite((byte)BsonType.End);

                int endPosition = patchable.WrittenCount;
                patchable.WriteInt32LittleEndianAt(sizePosition, endPosition - sizePosition);
                return;
            }

            using var buffer = new PooledBufferWriter();
            using (var bufferedWriter = new BsonWriter(buffer))
            {
                bufferedWriter.WriteArray(array);
            }

            InternalWrite(buffer.WrittenSpan);
        }
        }
        finally
        {
            ExitContainer();
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

    private void WriteArrayElement(int index, BsonValue value)
    {
        InternalWrite((byte)value.BsonType);
        WriteCString(index);
        WriteValue(value);
    }

    private const int Utf8StackAllocThreshold = 256;

    private void WriteUtf8Bytes(string value, int byteCount)
    {
        if (byteCount <= 0) return;

        if (byteCount <= Utf8StackAllocThreshold)
        {
            Span<byte> buffer = stackalloc byte[byteCount];
            int written = Encoding.UTF8.GetBytes(value.AsSpan(), buffer);
            InternalWrite(buffer.Slice(0, written));
            return;
        }

        var rented = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            int written = Encoding.UTF8.GetBytes(value.AsSpan(), rented.AsSpan(0, byteCount));
            InternalWrite(rented.AsSpan(0, written));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// 写入 C 字符串（以 null 结尾的字符串）
    /// </summary>
    /// <param name="value">字符串值</param>
    public void WriteCString(string value)
    {
        ThrowIfDisposed();
        if (value == null) throw new ArgumentNullException(nameof(value));
        if (value.IndexOf('\0') >= 0)
        {
            throw new ArgumentException("BSON CString values cannot contain null characters.", nameof(value));
        }

        if (BsonSerializer.CommonKeyCache.TryGetValue(value, out var cachedBytes))
        {
            InternalWrite(cachedBytes);
        }
        else
        {
            var byteCount = Encoding.UTF8.GetByteCount(value);
            WriteUtf8Bytes(value, byteCount);
        }
        InternalWrite((byte)0);
    }

    private void WriteCString(int value)
    {
        ThrowIfDisposed();
        if (value < 0) throw new ArgumentOutOfRangeException(nameof(value));

        Span<byte> buffer = stackalloc byte[11]; // int.MaxValue=2147483647 (10 digits) + safety
        int pos = buffer.Length;

        uint v = (uint)value;
        do
        {
            uint q = v / 10;
            buffer[--pos] = (byte)('0' + (v - (q * 10)));
            v = q;
        } while (v != 0);

        InternalWrite(buffer.Slice(pos));
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

        var byteCount = Encoding.UTF8.GetByteCount(value);
        InternalWrite(byteCount + 1);
        WriteUtf8Bytes(value, byteCount);
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
    public void WriteObjectId(ObjectId value)
    {
        Span<byte> bytes = stackalloc byte[12];
        value.CopyTo(bytes);
        InternalWrite(bytes);
    }

    /// <summary>
    /// 写入 DateTime
    /// </summary>
    public void WriteDateTime(DateTime value)
    {
        InternalWrite(BsonDateTime.EncodeStoredValue(value));
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

        WriteBinary(bytes.AsSpan(), subType);
    }

    internal void WriteBinary(ReadOnlySpan<byte> bytes, BsonBinary.BinarySubType subType = BsonBinary.BinarySubType.Generic)
    {
        ThrowIfDisposed();

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
        if (_stream != null && _stream.CanSeek)
        {
            var sizePosition = _stream.Position;
            InternalWrite(0);
            WriteString(code);
            WriteDocument(scope);
            var endPosition = _stream.Position;
            PatchStreamInt32LittleEndian(sizePosition, checked((int)(endPosition - sizePosition)));
            return;
        }

        if (_bufferWriter is IPatchableBufferWriter patchable)
        {
            int sizePosition = patchable.WrittenCount;
            InternalWrite(0);
            WriteString(code);
            WriteDocument(scope);
            int endPosition = patchable.WrittenCount;
            patchable.WriteInt32LittleEndianAt(sizePosition, endPosition - sizePosition);
            return;
        }

        using var buffer = new PooledBufferWriter();
        using (var bufferedWriter = new BsonWriter(buffer))
        {
            bufferedWriter.WriteJavaScriptWithScope(code, scope);
        }

        InternalWrite(buffer.WrittenSpan);
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
        InternalWrite(value.LowBits);
        InternalWrite(value.HighBits);
    }

    /// <summary>
    /// 写入时间戳
    /// </summary>
    public void WriteTimestamp(long value) => InternalWrite(value);

    private void ThrowIfDisposed() { if (_disposed) throw new ObjectDisposedException(nameof(BsonWriter)); }

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
