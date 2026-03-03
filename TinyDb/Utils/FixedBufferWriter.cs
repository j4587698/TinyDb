using System.Buffers;
using System.Buffers.Binary;

namespace TinyDb.Utils;

/// <summary>
/// 固定容量的 <see cref="IBufferWriter{T}"/>，用于在已知长度时避免二次拷贝。
/// </summary>
internal sealed class FixedBufferWriter : IPatchableBufferWriter
{
    private readonly byte[] _buffer;
    private int _index;

    public FixedBufferWriter(int length)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length));
        _buffer = new byte[length];
    }

    public int WrittenCount => _index;

    public byte[] WrittenArray
    {
        get
        {
            if (_index != _buffer.Length)
            {
                throw new InvalidOperationException("Buffer not fully written.");
            }

            return _buffer;
        }
    }

    public void Advance(int count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        if (_index + count > _buffer.Length)
        {
            throw new InvalidOperationException("Buffer overflow.");
        }

        _index += count;
    }

    public void WriteInt32LittleEndianAt(int offset, int value)
    {
        if ((uint)offset > (uint)(_index - 4))
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }

        BinaryPrimitives.WriteInt32LittleEndian(_buffer.AsSpan(offset, 4), value);
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_index);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_index);
    }

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint < 0) throw new ArgumentOutOfRangeException(nameof(sizeHint));

        var required = sizeHint == 0 ? 1 : sizeHint;
        if (_index + required > _buffer.Length)
        {
            throw new InvalidOperationException("Insufficient buffer capacity.");
        }
    }
}
