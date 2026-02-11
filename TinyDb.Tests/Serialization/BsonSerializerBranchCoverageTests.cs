using System;
using System.Buffers;
using System.IO;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonSerializerBranchCoverageTests
{
    [Test]
    public async Task Deserialize_WhenValueKeyMissing_ShouldReturnBsonNull()
    {
        var bytes = BsonSerializer.SerializeDocument(new BsonDocument());
        var value = BsonSerializer.Deserialize(bytes);

        await Assert.That(value).IsEqualTo(BsonNull.Value);
    }

    [Test]
    public async Task Deserialize_WhenValueKeyPresent_ShouldReturnStoredValue()
    {
        var bytes = BsonSerializer.Serialize(new BsonInt32(123));
        var value = BsonSerializer.Deserialize(bytes);

        await Assert.That(value).IsTypeOf<BsonInt32>();
        await Assert.That(((BsonInt32)value).Value).IsEqualTo(123);
    }

    [Test]
    public async Task BsonWriter_And_BsonReader_Constructors_WithNullStream_ShouldThrow()
    {
        await Assert.That(() => new BsonWriter(null!)).Throws<ArgumentNullException>();
        await Assert.That(() => new BsonReader(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_Dispose_ShouldRespectLeaveOpen()
    {
        var leaveOpenStream = new MemoryStream();
        using (var writer = new BsonWriter(leaveOpenStream, leaveOpen: true))
        {
            writer.WriteValue(new BsonInt32(1));
        }

        leaveOpenStream.WriteByte(0xFF);
        await Assert.That(leaveOpenStream.Length).IsGreaterThan(0L);

        var closedStream = new MemoryStream();
        var writer2 = new BsonWriter(closedStream, leaveOpen: false);
        writer2.Dispose();

        await Assert.That(() => closedStream.WriteByte(1)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonWriter_Dispose_WhenStreamIsNullAndLeaveOpenFalse_ShouldNotThrow()
    {
        var writer = new BsonWriter(new ArrayBufferWriter<byte>());

        var leaveOpenField = typeof(BsonWriter).GetField("_leaveOpen", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(leaveOpenField).IsNotNull();
        leaveOpenField!.SetValue(writer, false);

        await Assert.That(() => writer.Dispose()).ThrowsNothing();
    }

    [Test]
    public async Task SerializeDocument_ToStream_ShouldWriteAndLeaveStreamOpen()
    {
        var doc = new BsonDocument().Set("a", 1);
        using var ms = new MemoryStream();

        BsonSerializer.SerializeDocument(doc, ms);

        await Assert.That(ms.Length).IsGreaterThan(0L);

        // The overload uses BsonWriter(stream, leaveOpen: true)
        ms.WriteByte(0xFF);
        await Assert.That(ms.Length).IsGreaterThan(1L);
    }

    [Test]
    public async Task SerializeDocument_ToStream_WhenDocumentNull_ShouldThrow()
    {
        using var ms = new MemoryStream();
        await Assert.That(() => BsonSerializer.SerializeDocument(null!, ms)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task SerializeDocumentToBuffer_WhenDocumentNull_ShouldThrow()
    {
        var writer = new ArrayBufferWriter<byte>();
        await Assert.That(() => BsonSerializer.SerializeDocumentToBuffer(null!, writer)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_StreamCtor_WithNullStream_ShouldThrow()
    {
        await Assert.That(() => new BsonWriter((Stream)null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task BsonWriter_CurrentPosition_ShouldReturnForStream_AndThrowForBufferWriter()
    {
        using var ms = new MemoryStream();
        using var streamWriter = new BsonWriter(ms, leaveOpen: true);

        var currentPosition = GetCurrentPosition(streamWriter);
        await Assert.That(currentPosition).IsEqualTo(0L);

        using var pooled = new PooledBufferWriter(64);
        using var bufferWriter = new BsonWriter(pooled);

        await Assert.That(() => GetCurrentPosition(bufferWriter)).Throws<NotSupportedException>();
    }

    [Test]
    public async Task BsonWriter_WriteCString_Int32_ShouldRejectNegativeValues()
    {
        using var ms = new MemoryStream();
        using var writer = new BsonWriter(ms, leaveOpen: true);

        await Assert.That(() => InvokeWriteCStringInt32(writer, -1)).Throws<ArgumentOutOfRangeException>();

        InvokeWriteCStringInt32(writer, 123);
        await Assert.That(ms.Length).IsGreaterThan(0L);
    }

    [Test]
    public async Task BsonReader_Dispose_ShouldRespectLeaveOpen()
    {
        var leaveOpenStream = new MemoryStream();
        leaveOpenStream.WriteByte((byte)BsonType.Int32);
        using (var writer = new BsonWriter(leaveOpenStream, leaveOpen: true))
        {
            writer.WriteInt32(42);
        }
        leaveOpenStream.Position = 0;

        using (var reader = new BsonReader(leaveOpenStream, leaveOpen: true))
        {
            var value = reader.ReadValue();
            await Assert.That(value).IsTypeOf<BsonInt32>();
            await Assert.That(((BsonInt32)value).Value).IsEqualTo(42);
        }

        leaveOpenStream.Position = 0;
        await Assert.That(leaveOpenStream.Position).IsEqualTo(0L);

        var closedStream = new MemoryStream(BsonSerializer.Serialize(new BsonInt32(1)));
        var reader2 = new BsonReader(closedStream, leaveOpen: false);
        reader2.Dispose();

        await Assert.That(() => _ = closedStream.Position).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BsonReader_SkipBytes_ShouldHandleNegativeZeroAndEndOfStream()
    {
        using var stream = new MemoryStream();
        using var reader = new BsonReader(stream, leaveOpen: true);

        await Assert.That(() => InvokeSkipBytes(reader, -1)).Throws<ArgumentOutOfRangeException>();
        InvokeSkipBytes(reader, 0);
        await Assert.That(() => InvokeSkipBytes(reader, 1)).Throws<EndOfStreamException>();
    }

    private static void InvokeSkipBytes(BsonReader reader, int count)
    {
        var method = typeof(BsonReader).GetMethod("SkipBytes", BindingFlags.NonPublic | BindingFlags.Instance);
        if (method == null) throw new InvalidOperationException("SkipBytes method not found.");

        try
        {
            method.Invoke(reader, new object[] { count });
        }
        catch (TargetInvocationException tie) when (tie.InnerException != null)
        {
            throw tie.InnerException;
        }
    }

    private static long GetCurrentPosition(BsonWriter writer)
    {
        var prop = typeof(BsonWriter).GetProperty("CurrentPosition", BindingFlags.NonPublic | BindingFlags.Instance);
        if (prop == null) throw new InvalidOperationException("CurrentPosition property not found.");

        try
        {
            return (long)prop.GetValue(writer)!;
        }
        catch (TargetInvocationException tie) when (tie.InnerException != null)
        {
            throw tie.InnerException;
        }
    }

    private static void InvokeWriteCStringInt32(BsonWriter writer, int value)
    {
        var method = typeof(BsonWriter).GetMethod(
            "WriteCString",
            BindingFlags.NonPublic | BindingFlags.Instance,
            binder: null,
            types: new[] { typeof(int) },
            modifiers: null);
        if (method == null) throw new InvalidOperationException("WriteCString(int) method not found.");

        try
        {
            method.Invoke(writer, new object[] { value });
        }
        catch (TargetInvocationException tie) when (tie.InnerException != null)
        {
            throw tie.InnerException;
        }
    }
}
