using TinyDb.Bson;
using TinyDb.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Core;

public class PageDocumentEntryTests
{
    [Test]
    public async Task Id_WithValue_ShouldReturnId()
    {
        var doc = new BsonDocument().Set("_id", 123);
        var entry = new PageDocumentEntry(doc, Array.Empty<byte>());

        await Assert.That(((BsonInt32)entry.Id).Value).IsEqualTo(123);
    }

    [Test]
    public async Task Id_WithoutValue_ShouldReturnBsonNull()
    {
        var doc = new BsonDocument();
        var entry = new PageDocumentEntry(doc, Array.Empty<byte>());

        await Assert.That(entry.Id).IsEqualTo(BsonNull.Value);
    }

    [Test]
    public async Task RawBytes_WhenBackedByMemory_ShouldReturnStableCopy()
    {
        var doc = new BsonDocument().Set("_id", 1);
        var source = new byte[] { 1, 2, 3 };
        var entry = new PageDocumentEntry(doc, source.AsMemory());

        await Assert.That(entry.RawLength).IsEqualTo(3);
        await Assert.That(entry.RawMemory.Span[1]).IsEqualTo((byte)2);

        var copied = entry.RawBytes;
        copied[1] = 9;
        await Assert.That(source[1]).IsEqualTo((byte)2);

        var stable = entry.ToStableRawBytes();
        source[1] = 8;
        await Assert.That(stable.RawMemory.Span[1]).IsEqualTo((byte)2);
    }
}
