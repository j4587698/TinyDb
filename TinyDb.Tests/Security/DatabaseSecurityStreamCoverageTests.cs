using System;
using System.IO;
using TinyDb.Core;
using TinyDb.Security;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Security;

public sealed class DatabaseSecurityStreamCoverageTests
{
    [Test]
    public async Task TryReadSecurityMetadata_StreamShortRead_ShouldReturnFalse()
    {
        var headerBytes = new byte[DatabaseHeader.Size];
        using var stream = new ShortReadStream(headerBytes, maxReadBytes: 8, zeroAfterBytes: 32);

        var ok = DatabaseSecurity.TryReadSecurityMetadata(stream, out _);

        await Assert.That(ok).IsFalse();
    }

    private sealed class ShortReadStream : Stream
    {
        private readonly byte[] _data;
        private readonly int _maxReadBytes;
        private readonly int _zeroAfterBytes;
        private int _position;

        public ShortReadStream(byte[] data, int maxReadBytes, int zeroAfterBytes)
        {
            _data = data ?? throw new ArgumentNullException(nameof(data));
            _maxReadBytes = maxReadBytes;
            _zeroAfterBytes = zeroAfterBytes;
        }

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => _data.Length;
        public override long Position { get => _position; set => _position = (int)value; }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_position >= _zeroAfterBytes)
                return 0;

            if (_position >= _data.Length)
                return 0;

            var toRead = Math.Min(count, _maxReadBytes);
            toRead = Math.Min(toRead, _data.Length - _position);

            Array.Copy(_data, _position, buffer, offset, toRead);
            _position += toRead;
            return toRead;
        }

        public override void Flush() { }

        public override long Seek(long offset, SeekOrigin origin)
        {
            int newPosition = origin switch
            {
                SeekOrigin.Begin => (int)offset,
                SeekOrigin.Current => _position + (int)offset,
                SeekOrigin.End => _data.Length + (int)offset,
                _ => _position
            };

            _position = Math.Clamp(newPosition, 0, _data.Length);
            return _position;
        }

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}

