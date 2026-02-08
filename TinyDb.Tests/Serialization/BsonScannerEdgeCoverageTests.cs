using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Text;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public sealed class BsonScannerEdgeCoverageTests
{
    [Test]
    public async Task TryGetValue_ShouldSkipEmbeddedDocumentAndRegex_InSerializedOrder()
    {
        var bytes = BuildDocumentWithEmbeddedDocRegexAndTarget();

        await Assert.That(BsonScanner.TryGetValue(bytes, "target", out var val)).IsTrue();
        await Assert.That(val).IsTypeOf<BsonInt32>();
        await Assert.That(((BsonInt32)val!).Value).IsEqualTo(123);

        await Assert.That(BsonScanner.TryGetValue(bytes, "skipDoc", out var docVal)).IsTrue();
        await Assert.That(docVal).IsTypeOf<BsonNull>();
    }

    [Test]
    public async Task TryGetValue_MalformedNameWithoutTerminator_ShouldReturnFalse()
    {
        var bytes = new byte[] { 0, 0, 0, 0, (byte)BsonType.Int32, (byte)'a' };
        await Assert.That(BsonScanner.TryGetValue(bytes, "a", out _)).IsFalse();
    }

    [Test]
    public async Task TryGetValue_UnknownType_ShouldThrow()
    {
        var bytes = BuildDocumentWithUnknownType();

        await Assert.That(() => BsonScanner.TryGetValue(bytes, "x", out _))
            .ThrowsExactly<NotSupportedException>();
    }

    private static byte[] BuildDocumentWithEmbeddedDocRegexAndTarget()
    {
        var buffer = new List<byte>(64);
        buffer.AddRange(new byte[4]); // length placeholder

        buffer.Add((byte)BsonType.Document);
        buffer.AddRange(Encoding.UTF8.GetBytes("skipDoc"));
        buffer.Add(0);
        buffer.AddRange(GetInt32Bytes(5));
        buffer.Add(0); // empty embedded doc

        buffer.Add((byte)BsonType.RegularExpression);
        buffer.AddRange(Encoding.UTF8.GetBytes("regex"));
        buffer.Add(0);
        buffer.Add((byte)'p');
        buffer.Add(0);
        buffer.Add((byte)'i');
        buffer.Add(0);

        buffer.Add((byte)BsonType.Int32);
        buffer.AddRange(Encoding.UTF8.GetBytes("target"));
        buffer.Add(0);
        buffer.AddRange(GetInt32Bytes(123));

        buffer.Add(0); // end

        var bytes = buffer.ToArray();
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0, 4), bytes.Length);
        return bytes;
    }

    private static byte[] BuildDocumentWithUnknownType()
    {
        var buffer = new List<byte>(16);
        buffer.AddRange(new byte[4]); // length placeholder

        buffer.Add(0x0C); // unknown type (not defined in TinyDb's BsonType enum)
        buffer.Add((byte)'x');
        buffer.Add(0);

        buffer.Add(0); // end

        var bytes = buffer.ToArray();
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(0, 4), bytes.Length);
        return bytes;
    }

    private static byte[] GetInt32Bytes(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, value);
        return bytes;
    }
}
