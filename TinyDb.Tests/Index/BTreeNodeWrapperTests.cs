using System;
using System.Reflection;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Bson;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class BTreeNodeWrapperTests : IDisposable
{
    private readonly TinyDbEngine _engine;
    private readonly string _dbPath;

    public BTreeNodeWrapperTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), $"btree_node_tests_{Guid.NewGuid()}.db");
        _engine = new TinyDbEngine(_dbPath);
    }

    public void Dispose()
    {
        _engine.Dispose();
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Test]
    public async Task BTreeNode_Wrapper_Methods_Coverage()
    {
        // 1. Create a BTreeIndex via IndexManager
        var col = _engine.GetCollection<object>("test");
        var idxManager = _engine.GetIndexManager("test");
        idxManager.CreateIndex("idx_a", new[] { "A" });
        
        var index = idxManager.GetIndex("idx_a") as BTreeIndex;
        await Assert.That(index).IsNotNull();
        
        // 2. Insert data to trigger splits (so we have internal nodes)
        // Max keys is default 200. Insert 500 items.
        for (int i = 0; i < 500; i++)
        {
            col.Insert(new BsonDocument().Set("_id", i).Set("A", i));
        }
        
        // 3. Access _root field via reflection
        var rootField = typeof(BTreeIndex).GetField("_root", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(rootField).IsNotNull();
        
        var rootNode = rootField!.GetValue(index) as BTreeNode;
        await Assert.That(rootNode).IsNotNull();
        
        // 4. Test BTreeNode methods
        await Assert.That(rootNode!.IsDiskMode).IsTrue();
        await Assert.That(rootNode.ToString()).Contains("Internal");
        
        // GetChild coverage
        if (!rootNode.IsLeaf)
        {
            var child = rootNode.GetChild(0);
            await Assert.That(child).IsNotNull();
            await Assert.That(child.Parent).IsNull(); 
        }
        
        // FindKeyPosition
        var key = new IndexKey(new BsonValue[] { new BsonInt32(50) });
        var pos = rootNode.FindKeyPosition(key);
        await Assert.That(pos).IsGreaterThanOrEqualTo(0);
        
        // GetStatistics
        var stats = rootNode.GetStatistics();
        await Assert.That(stats).IsNotNull();
        
        // NotSupported methods
        await Assert.That(() => rootNode.SetKey(0, key)).Throws<NotSupportedException>();
        await Assert.That(() => rootNode.SetChild(0, rootNode)).Throws<NotSupportedException>();
        await Assert.That(() => rootNode.RemoveKeyAt(0)).Throws<NotSupportedException>();
        await Assert.That(() => rootNode.RemoveChildAt(0)).Throws<NotSupportedException>();
        
        // Properties
        rootNode.Parent = null;
        rootNode.NextSibling = null;
        rootNode.PreviousSibling = null;
    }
}