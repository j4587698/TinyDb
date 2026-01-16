using TinyDb.Bson;
using TinyDb.Serialization;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Serialization;

public class NestedStructureTests
{
    public class Level1
    {
        public string Name { get; set; } = "";
        public Level2 Child { get; set; } = new();
        public List<Level2> ChildrenList { get; set; } = new();
        public Dictionary<string, Level2> ChildrenMap { get; set; } = new();
    }

    public class Level2
    {
        public int Value { get; set; }
        public Level3 DeepChild { get; set; } = new();
    }

    public class Level3
    {
        public bool Flag { get; set; }
        public double Score { get; set; }
    }

    [Test]
    public async Task Serialize_DeeplyNestedObject_ShouldWork()
    {
        if (!System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeCompiled) return;

        var obj = new Level1
        {
            Name = "Root",
            Child = new Level2
            {
                Value = 42,
                DeepChild = new Level3
                {
                    Flag = true,
                    Score = 99.9
                }
            }
        };

        var doc = BsonMapper.ToDocument(obj);

        await Assert.That(doc["name"].ToString()).IsEqualTo("Root");
        await Assert.That(((BsonDocument)doc["child"])["value"].ToInt32()).IsEqualTo(42);
        await Assert.That(((BsonDocument)((BsonDocument)doc["child"])["deepChild"])["flag"].ToBoolean()).IsTrue();
        await Assert.That(((BsonDocument)((BsonDocument)doc["child"])["deepChild"])["score"].ToDouble()).IsEqualTo(99.9);

        var restored = BsonMapper.ToObject<Level1>(doc);
        await Assert.That(restored!.Name).IsEqualTo("Root");
        await Assert.That(restored.Child.Value).IsEqualTo(42);
        await Assert.That(restored.Child.DeepChild.Flag).IsTrue();
        await Assert.That(restored.Child.DeepChild.Score).IsEqualTo(99.9);
    }

    [Test]
    public async Task Serialize_ListNestedObject_ShouldWork()
    {
        if (!System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeCompiled) return;

        var obj = new Level1
        {
            Name = "ListTest",
            ChildrenList = new List<Level2>
            {
                new Level2 { Value = 1, DeepChild = new Level3 { Flag = false } },
                new Level2 { Value = 2, DeepChild = new Level3 { Flag = true } }
            }
        };

        var doc = BsonMapper.ToDocument(obj);
        var list = (BsonArray)doc["childrenList"];
        
        await Assert.That(list.Count).IsEqualTo(2);
        await Assert.That(((BsonDocument)list[0])["value"].ToInt32()).IsEqualTo(1);
        await Assert.That(((BsonDocument)((BsonDocument)list[1])["deepChild"])["flag"].ToBoolean()).IsTrue();

        var restored = BsonMapper.ToObject<Level1>(doc);
        await Assert.That(restored!.ChildrenList.Count).IsEqualTo(2);
        await Assert.That(restored.ChildrenList[0].Value).IsEqualTo(1);
        await Assert.That(restored.ChildrenList[1].DeepChild.Flag).IsTrue();
    }

    [Test]
    public async Task Serialize_DictionaryNestedObject_ShouldWork()
    {
        if (!System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeCompiled) return;

        var obj = new Level1
        {
            Name = "MapTest",
            ChildrenMap = new Dictionary<string, Level2>
            {
                { "first", new Level2 { Value = 10 } },
                { "second", new Level2 { Value = 20, DeepChild = new Level3 { Score = 5.5 } } }
            }
        };

        var doc = BsonMapper.ToDocument(obj);
        var map = (BsonDocument)doc["childrenMap"];

        await Assert.That(((BsonDocument)map["first"])["value"].ToInt32()).IsEqualTo(10);
        await Assert.That(((BsonDocument)((BsonDocument)map["second"])["deepChild"])["score"].ToDouble()).IsEqualTo(5.5);

        var restored = BsonMapper.ToObject<Level1>(doc);
        await Assert.That(restored!.ChildrenMap.Count).IsEqualTo(2);
        await Assert.That(restored.ChildrenMap["first"].Value).IsEqualTo(10);
        await Assert.That(restored.ChildrenMap["second"].DeepChild.Score).IsEqualTo(5.5);
    }

    public class RecursiveNode
    {
        public string Name { get; set; } = "";
        public RecursiveNode? Next { get; set; }
    }

    [Test]
    public async Task Serialize_RecursiveReference_ShouldHandleGracefully()
    {
        if (!System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeCompiled) return;

        var node1 = new RecursiveNode { Name = "Node1" };
        var node2 = new RecursiveNode { Name = "Node2" };
        node1.Next = node2;
        node2.Next = node1; // Cycle!

        // BsonMapper should detect cycle and probably cut it off or return null/empty for the recursion
        
        var doc = BsonMapper.ToDocument(node1);
        
        await Assert.That(doc["name"].ToString()).IsEqualTo("Node1");
        await Assert.That(((BsonDocument)doc["next"])["name"].ToString()).IsEqualTo("Node2");
        
        var node2Doc = (BsonDocument)doc["next"];
        await Assert.That(node2Doc.ContainsKey("next")).IsTrue();
        
        // Should be empty doc or similar because no ID property
        var cycleDoc = (BsonDocument)node2Doc["next"];
        await Assert.That(cycleDoc.Count).IsEqualTo(0);
    }
}
