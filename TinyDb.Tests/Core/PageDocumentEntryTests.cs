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
}
