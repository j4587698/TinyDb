using System;
using System.Reflection;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Core;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class BTreeNodeCoverageTests
{
    [Test]
    public async Task BTreeNode_Wrapper_Methods_Coverage()
    {
        var index = new BTreeIndex("test", new[] { "id" }, false, 4);
        for (int i = 0; i < 10; i++)
        {
            index.Insert(new IndexKey(new BsonInt32(i)), new BsonString($"doc_{i}"));
        }

        // Get _root field via reflection
        var rootField = typeof(BTreeIndex).GetField("_root", BindingFlags.NonPublic | BindingFlags.Instance);
        var root = rootField!.GetValue(index) as BTreeNode;
        
        await Assert.That(root).IsNotNull();
        await Assert.That(root!.IsDiskMode).IsTrue();
        
        // Test properties
        await Assert.That(root.KeyCount).IsGreaterThan(0);
        // root should be internal node (not leaf) since we inserted 10 items with maxKeys=4
        await Assert.That(root.IsLeaf).IsFalse();
        await Assert.That(root.ChildCount).IsGreaterThan(0);
        await Assert.That(root.DocumentCount).IsEqualTo(0); // Internal node doc count is 0 in disk mode wrapper
        
        // Test GetKey
        var key = root.GetKey(0);
        await Assert.That(key).IsNotNull();
        
        // Test GetChild
        var child = root.GetChild(0);
        await Assert.That(child).IsNotNull();
        await Assert.That(child.IsLeaf).IsTrue(); // First child likely leaf
        
        // Test FindKeyPosition
        var pos = root.FindKeyPosition(key);
        await Assert.That(pos).IsEqualTo(0);
        
        // Test GetDocumentIds (on leaf)
        await Assert.That(child.DocumentCount).IsGreaterThan(0);
        var docs = child.GetDocumentIds(0);
        await Assert.That(docs).IsNotNull();
        await Assert.That(docs.Count).IsEqualTo(1);
        
        // Test ToString
        await Assert.That(root.ToString()).IsNotEmpty();
        
        // Test Statistics
        var stats = root.GetStatistics();
        await Assert.That(stats).IsNotNull();
        await Assert.That(stats.IsLeaf).IsFalse();
        
        // Test NotSupported methods
        try { root.SetKey(0, key); Assert.Fail("SetKey"); } catch (NotSupportedException) {}
        try { root.SetChild(0, child); Assert.Fail("SetChild"); } catch (NotSupportedException) {}
        try { root.RemoveKeyAt(0); Assert.Fail("RemoveKeyAt"); } catch (NotSupportedException) {}
        try { root.RemoveChildAt(0); Assert.Fail("RemoveChildAt"); } catch (NotSupportedException) {}
        
        index.Dispose();
    }
}
