using System;
using System.Buffers;
using System.IO;

namespace Microsoft.IO;

/// <summary>
/// TinyDb 内置的最小兼容实现，仅覆盖当前项目使用到的 API。
/// </summary>
internal sealed class RecyclableMemoryStreamManager
{
    internal sealed class Options
    {
        public int BlockSize { get; set; } = 4096;
        public int LargeBufferMultiple { get; set; } = 1024 * 1024;
        public long MaximumSmallPoolFreeBytes { get; set; } = 1024 * 1024 * 100;
    }

    private readonly Options _options;

    public RecyclableMemoryStreamManager() : this(new Options())
    {
    }

    public RecyclableMemoryStreamManager(Options options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public RecyclableMemoryStream GetStream()
    {
        var initialCapacity = _options.BlockSize > 0 ? _options.BlockSize : 4096;
        return new RecyclableMemoryStream(initialCapacity);
    }
}

/// <summary>
/// TinyDb 内置的最小兼容内存流类型。
/// </summary>
internal sealed class RecyclableMemoryStream : Stream
{
    private byte[] _buffer;
    private int _length;
    private int _position;
    private bool _disposed;

    public RecyclableMemoryStream() : this(0)
    {
    }

    public RecyclableMemoryStream(int capacity)
    {
        if (capacity < 0) throw new ArgumentOutOfRangeException(nameof(capacity));
        _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(capacity, 1));
    }

    public override bool CanRead => !_disposed;
    public override bool CanSeek => !_disposed;
    public override bool CanWrite => !_disposed;

    public override long Length
    {
        get
        {
            ThrowIfDisposed();
            return _length;
        }
    }

    public override long Position
    {
        get
        {
            ThrowIfDisposed();
            return _position;
        }
        set
        {
            ThrowIfDisposed();
            if (value < 0 || value > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(value));
            _position = (int)value;
        }
    }

    public override void Flush()
    {
        ThrowIfDisposed();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        ValidateBufferArguments(buffer, offset, count);
        return Read(buffer.AsSpan(offset, count));
    }

    public override int Read(Span<byte> buffer)
    {
        ThrowIfDisposed();
        var available = _length - _position;
        if (available <= 0) return 0;

        var bytesToCopy = Math.Min(available, buffer.Length);
        _buffer.AsSpan(_position, bytesToCopy).CopyTo(buffer);
        _position += bytesToCopy;
        return bytesToCopy;
    }

    public override int ReadByte()
    {
        ThrowIfDisposed();
        if (_position >= _length) return -1;
        return _buffer[_position++];
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        ThrowIfDisposed();
        var next = origin switch
        {
            SeekOrigin.Begin => offset,
            SeekOrigin.Current => _position + offset,
            SeekOrigin.End => _length + offset,
            _ => throw new ArgumentOutOfRangeException(nameof(origin))
        };

        if (next < 0 || next > int.MaxValue) throw new IOException("Seek position is outside the supported range.");
        _position = (int)next;
        return _position;
    }

    public override void SetLength(long value)
    {
        ThrowIfDisposed();
        if (value < 0 || value > int.MaxValue) throw new ArgumentOutOfRangeException(nameof(value));

        var newLength = (int)value;
        EnsureCapacity(newLength);
        if (newLength > _length)
        {
            Array.Clear(_buffer, _length, newLength - _length);
        }

        _length = newLength;
        if (_position > _length)
        {
            _position = _length;
        }
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        ValidateBufferArguments(buffer, offset, count);
        Write(buffer.AsSpan(offset, count));
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        ThrowIfDisposed();
        if (buffer.Length == 0) return;

        var endPosition = checked(_position + buffer.Length);
        EnsureCapacity(endPosition);
        if (_position > _length)
        {
            Array.Clear(_buffer, _length, _position - _length);
        }

        buffer.CopyTo(_buffer.AsSpan(_position));
        _position = endPosition;
        if (_position > _length)
        {
            _length = _position;
        }
    }

    public override void WriteByte(byte value)
    {
        ThrowIfDisposed();
        EnsureCapacity(_position + 1);
        if (_position > _length)
        {
            Array.Clear(_buffer, _length, _position - _length);
        }

        _buffer[_position++] = value;
        if (_position > _length)
        {
            _length = _position;
        }
    }

    public byte[] ToArray()
    {
        ThrowIfDisposed();
        var result = new byte[_length];
        _buffer.AsSpan(0, _length).CopyTo(result);
        return result;
    }

    protected override void Dispose(bool disposing)
    {
        if (!_disposed)
        {
            _disposed = true;
            var buffer = _buffer;
            _buffer = Array.Empty<byte>();
            _length = 0;
            _position = 0;
            if (buffer.Length > 0)
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }
        }

        base.Dispose(disposing);
    }

    private void EnsureCapacity(int requiredCapacity)
    {
        if (requiredCapacity <= _buffer.Length) return;

        var newCapacity = Math.Max(requiredCapacity, _buffer.Length * 2);
        var newBuffer = ArrayPool<byte>.Shared.Rent(newCapacity);
        _buffer.AsSpan(0, _length).CopyTo(newBuffer);
        ArrayPool<byte>.Shared.Return(_buffer, clearArray: true);
        _buffer = newBuffer;
    }

    private void ThrowIfDisposed()
    {
        if (_disposed) throw new ObjectDisposedException(nameof(RecyclableMemoryStream));
    }
}
