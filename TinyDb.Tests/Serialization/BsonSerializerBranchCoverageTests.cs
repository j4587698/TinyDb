using System;
using System.IO;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Serialization;
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
}
