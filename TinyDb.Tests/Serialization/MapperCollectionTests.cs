using TinyDb.Serialization;
using TinyDb.Bson;
using TinyDb.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using System.Collections.Generic;

namespace TinyDb.Tests.Serialization;

public class MapperCollectionTests
{
    [Entity]
    public class CollectionContainer
    {
        public List<int> IntList { get; set; } = new();
        public int[] IntArray { get; set; } = Array.Empty<int>();
        public Dictionary<string, int> Dict { get; set; } = new();
        public IEnumerable<string> EnumerableStrings { get; set; } = new List<string>();
        public IList<double> DoubleList { get; set; } = new List<double>();
    }

    [Test]
    public async Task Mapper_Should_Handle_Various_Collections()
    {
        var container = new CollectionContainer
        {
            IntList = new List<int> { 1, 2 },
            IntArray = new[] { 3, 4 },
            Dict = new Dictionary<string, int> { { "a", 1 } },
            EnumerableStrings = new[] { "s1", "s2" },
            DoubleList = new List<double> { 1.1, 2.2 }
        };

        var doc = AotBsonMapper.ToDocument(container);
        var restored = AotBsonMapper.FromDocument<CollectionContainer>(doc);

        await Assert.That(restored.IntList.Count).IsEqualTo(2);
        await Assert.That(restored.IntArray.Length).IsEqualTo(2);
        await Assert.That(restored.Dict["a"]).IsEqualTo(1);
        await Assert.That(restored.EnumerableStrings.Count()).IsEqualTo(2);
        await Assert.That(restored.DoubleList.Count).IsEqualTo(2);
    }

    [Entity]
    public class NestedCollections
    {
        public List<List<int>> Matrix { get; set; } = new();
    }

    [Test]
    public async Task Mapper_Should_Handle_Nested_Collections()
    {
        var nested = new NestedCollections();
        nested.Matrix.Add(new List<int> { 1, 2 });
        nested.Matrix.Add(new List<int> { 3, 4 });

        var doc = AotBsonMapper.ToDocument(nested);
        var restored = AotBsonMapper.FromDocument<NestedCollections>(doc);

        await Assert.That(restored.Matrix.Count).IsEqualTo(2);
        await Assert.That(restored.Matrix[0][0]).IsEqualTo(1);
    }
}
