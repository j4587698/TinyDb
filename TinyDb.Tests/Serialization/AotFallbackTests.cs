using TinyDb.Serialization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotFallbackTests
{
    // A class NOT registered with Source Generator
    public class UnregisteredEntity
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
        public Dictionary<string, int> Scores { get; set; } = new();
        public List<string> Tags { get; set; } = new();
    }

    [Test]
    public async Task Fallback_Mapping_Should_Work()
    {
        var entity = new UnregisteredEntity
        {
            Name = "Fallback Test",
            Value = 123,
            Scores = new Dictionary<string, int> { { "math", 90 }, { "science", 85 } },
            Tags = new List<string> { "tag1", "tag2" }
        };

        // Act
        var doc = AotBsonMapper.ToDocument(entity);
        var replayed = AotBsonMapper.FromDocument<UnregisteredEntity>(doc);

        // Assert
        await Assert.That(replayed.Name).IsEqualTo(entity.Name);
        await Assert.That(replayed.Value).IsEqualTo(entity.Value);
        await Assert.That(replayed.Scores["math"]).IsEqualTo(90);
        await Assert.That(replayed.Tags.Count).IsEqualTo(2);
    }

    [Test]
    public async Task Fallback_Circular_Reference_Should_Handle_Gracefully()
    {
        var parent = new CircularEntity { Name = "Parent" };
        var child = new CircularEntity { Name = "Child", Parent = parent };
        parent.Child = child;

        // Act
        var doc = AotBsonMapper.ToDocument(parent);
        
        // Assert - Should not stack overflow
        await Assert.That(doc.ContainsKey("child")).IsTrue();
        var childDoc = (BsonDocument)doc["child"];
        await Assert.That(childDoc.ContainsKey("parent")).IsTrue();
        // Circular reference is handled by returning a doc without the back-reference (empty since no _id)
        await Assert.That(((BsonDocument)childDoc["parent"]).Count).IsEqualTo(0);
    }

    public class CircularEntity
    {
        public string Name { get; set; } = "";
        public CircularEntity? Parent { get; set; }
        public CircularEntity? Child { get; set; }
    }
}
