using System;
using System.Buffers;

namespace TinyDb.Utils;

/// <summary>
/// 基于 <see cref="ArrayPool{T}"/> 的 <see cref="IBufferWriter{T}"/> 实现，用于减少序列化过程中的内存分配。
/// </summary>
internal sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer;
    private int _index;

    public PooledBufferWriter(int initialCapacity = 4096)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
    }

    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _index);
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _index);
    public int WrittenCount => _index;

    public void Reset()
    {
        _index = 0;
    }

    public void Advance(int count)
    {
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        if (_index + count > _buffer.Length) throw new ArgumentOutOfRangeException(nameof(count));
        _index += count;
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
        int required = sizeHint > 0 ? sizeHint : 256;
        if (_index + required <= _buffer.Length) return;

        int newSize = Math.Max(_buffer.Length * 2, _index + required);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
        Buffer.BlockCopy(_buffer, 0, newBuffer, 0, _index);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = newBuffer;
    }

    public void Dispose()
    {
        if (_buffer.Length == 0) return;

        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = Array.Empty<byte>();
        _index = 0;
    }
}
