using TinyDb.Serialization;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class AotMapperReflectionTests
{
    public class ComplexDoc
    {
        public Guid Id { get; set; }
        public NestedDoc Nested { get; set; } = new();
        public List<SubItem> List { get; set; } = new();
        public Dictionary<string, int> Dict { get; set; } = new();
    }

    public class NestedDoc
    {
        public string Name { get; set; } = "";
        public int Value { get; set; }
    }

    public struct SubItem
    {
        public bool Flag { get; set; }
    }

    [Test]
    public async Task Reflection_Fallback_Should_Handle_Complex_Graph()
    {
        if (!System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeCompiled) return;

        var doc = new ComplexDoc
        {
            Id = Guid.NewGuid(),
            Nested = new NestedDoc { Name = "N", Value = 1 },
            List = new List<SubItem> { new SubItem { Flag = true }, new SubItem { Flag = false } },
            Dict = new Dictionary<string, int> { { "k1", 10 }, { "k2", 20 } }
        };

        var bson = AotBsonMapper.ToDocument(doc);
        var restored = AotBsonMapper.FromDocument<ComplexDoc>(bson);

        await Assert.That(restored.Id).IsEqualTo(doc.Id);
        await Assert.That(restored.Nested.Name).IsEqualTo("N");
        await Assert.That(restored.List.Count).IsEqualTo(2);
        await Assert.That(restored.List[0].Flag).IsTrue();
        await Assert.That(restored.Dict["k1"]).IsEqualTo(10);
    }
}
