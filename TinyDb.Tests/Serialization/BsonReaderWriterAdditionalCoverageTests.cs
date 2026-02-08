using System.Text;
using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class BsonReaderWriterAdditionalCoverageTests
{
    [Test]
    public async Task BsonWriter_NoPayloadTypes_ShouldExecute()
    {
        using var stream = new MemoryStream();
        using var writer = new BsonWriter(stream, true);

        writer.WriteUndefined();
        writer.WriteMinKey();
        writer.WriteMaxKey();
        writer.WriteDecimal128(123m);

        await Assert.That(stream.Length).IsGreaterThan(0);
    }

    [Test]
    public async Task BsonReader_ReadCString_LongValue_ShouldGrowBuffer()
    {
        var value = new string('a', 200);
        var bytes = Encoding.UTF8.GetBytes(value + "\0");

        using var stream = new MemoryStream(bytes);
        using var reader = new BsonReader(stream, leaveOpen: true);

        var result = reader.ReadCString();
        await Assert.That(result.Length).IsEqualTo(200);
    }

    [Test]
    public async Task BsonReader_ReadDocument_WithFields_ShouldSkip_Int32_And_Decimal128()
    {
        var document = new BsonDocument()
            .Set("keep", "x")
            .Set("i", 1)
            .Set("d", new BsonDecimal128(new Decimal128(123m)));

        var bytes = BsonSerializer.SerializeDocument(document);
        using var stream = new MemoryStream(bytes);
        using var reader = new BsonReader(stream, leaveOpen: true);

        var fields = new HashSet<string> { "keep" };
        var result = reader.ReadDocument(fields);

        await Assert.That(result.Count).IsEqualTo(1);
        await Assert.That(((BsonString)result["keep"]).Value).IsEqualTo("x");
    }

    [Test]
    public async Task BsonReader_ReadDocument_SizeMismatch_WithFields_ShouldThrow()
    {
        var bytes = BsonSerializer.SerializeDocument(new BsonDocument().Set("a", 1));

        var size = BitConverter.ToInt32(bytes, 0);
        BitConverter.GetBytes(size + 1).CopyTo(bytes, 0);

        using var stream = new MemoryStream(bytes);
        using var reader = new BsonReader(stream, leaveOpen: true);

        await Assert.That(() => reader.ReadDocument(new HashSet<string>())).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task BsonReader_SkipValue_UnsupportedType_ShouldThrow()
    {
        var bytes = new byte[]
        {
            8, 0, 0, 0,
            250,
            (byte)'a', 0,
            0
        };

        using var stream = new MemoryStream(bytes);
        using var reader = new BsonReader(stream, leaveOpen: true);

        await Assert.That(() => reader.ReadDocument(new HashSet<string>())).Throws<NotSupportedException>();
    }
}

