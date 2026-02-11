using System;
using System.Buffers.Binary;
using TinyDb.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Utils;

public class PooledBufferWriterTests
{
    [Test]
    public async Task WrittenMemory_ShouldMatch_WrittenSpan()
    {
        using var writer = new PooledBufferWriter(64);

        var span = writer.GetSpan(4);
        span[0] = 1;
        span[1] = 2;
        span[2] = 3;
        span[3] = 4;
        writer.Advance(4);

        var memory = writer.WrittenMemory;

        await Assert.That(memory.Length).IsEqualTo(4);
        await Assert.That(writer.WrittenSpan.SequenceEqual(memory.Span)).IsTrue();
    }

    [Test]
    public async Task WriteInt32LittleEndianAt_ShouldSupportValidOffset_AndRejectInvalidOffset()
    {
        using var writer = new PooledBufferWriter(64);

        writer.GetSpan(4);
        writer.Advance(4);

        writer.WriteInt32LittleEndianAt(0, 123456);
        var value = BinaryPrimitives.ReadInt32LittleEndian(writer.WrittenSpan[..4]);
        await Assert.That(value).IsEqualTo(123456);

        await Assert.That(() => writer.WriteInt32LittleEndianAt(1, 0))
            .Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task Advance_ShouldThrow_ForNegative_AndBeyondBufferLength()
    {
        using var writer = new PooledBufferWriter(1);

        await Assert.That(() => writer.Advance(-1)).Throws<ArgumentOutOfRangeException>();

        var span = writer.GetSpan(0);
        var tooBig = span.Length + 1;

        await Assert.That(() => writer.Advance(tooBig)).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task GetMemory_ShouldEnsureCapacity_AndReturnMemory()
    {
        using var writer = new PooledBufferWriter(16);
        var memory = writer.GetMemory(512);

        await Assert.That(memory.Length).IsGreaterThan(0);
    }

    [Test]
    public async Task Dispose_Twice_ShouldReturnEarly()
    {
        var writer = new PooledBufferWriter(16);
        writer.Dispose();

        await Assert.That(() => writer.Dispose()).ThrowsNothing();
    }
}

