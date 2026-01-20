using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Bson;

public class BsonDocumentBuilderTests
{
    [Test]
    public async Task Builder_Should_Create_Document()
    {
        var builder = new BsonDocumentBuilder();
        var doc = builder
            .Set("a", 1)
            .Set("b", "test")
            .Build();
            
        await Assert.That(doc.Count).IsEqualTo(2);
        await Assert.That(((BsonInt32)doc["a"]).Value).IsEqualTo(1);
        await Assert.That(((BsonString)doc["b"]).Value).IsEqualTo("test");
    }

    [Test]
    public async Task Builder_From_Existing_Should_Work()
    {
        var original = new BsonDocument().Set("a", 1);
        var builder = new BsonDocumentBuilder(original);
        var doc = builder.Set("b", 2).Build();
        
        await Assert.That(doc.Count).IsEqualTo(2);
        await Assert.That(((BsonInt32)doc["a"]).Value).IsEqualTo(1);
        await Assert.That(((BsonInt32)doc["b"]).Value).IsEqualTo(2);
    }

    [Test]
    public async Task Builder_Remove_Should_Work()
    {
        var builder = new BsonDocumentBuilder();
        var doc = builder.Set("a", 1).Remove("a").Build();
        
        await Assert.That(doc.Count).IsEqualTo(0);
    }
}
