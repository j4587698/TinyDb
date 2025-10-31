using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Index;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public class BTreeNodeTests
{
    [Test]
    public async Task Constructor_Should_Initialize_Leaf_Node()
    {
        // Arrange
        var maxKeys = 100;

        // Act
        var node = new BTreeNode(maxKeys, true);

        // Assert
        await Assert.That(node.IsLeaf).IsTrue();
        await Assert.That(node.KeyCount).IsEqualTo(0);
        await Assert.That(node.ChildCount).IsEqualTo(0);
        await Assert.That(node.DocumentCount).IsEqualTo(0);
        await Assert.That(node.MaxKeys).IsEqualTo(maxKeys);
        await Assert.That(node.MinKeys).IsEqualTo(maxKeys / 2);
    }

    [Test]
    public async Task Constructor_Should_Initialize_Internal_Node()
    {
        // Arrange
        var maxKeys = 100;

        // Act
        var node = new BTreeNode(maxKeys, false);

        // Assert
        await Assert.That(node.IsLeaf).IsFalse();
        await Assert.That(node.KeyCount).IsEqualTo(0);
        await Assert.That(node.ChildCount).IsEqualTo(0);
        await Assert.That(node.DocumentCount).IsEqualTo(0);
    }

    [Test]
    public async Task FindKeyPosition_Should_Return_Correct_Position_For_Existing_Key()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var key1 = new IndexKey(new BsonString("apple"));
        var key2 = new IndexKey(new BsonString("banana"));
        var key3 = new IndexKey(new BsonString("cherry"));

        node.Insert(key1, new BsonInt32(1));
        node.Insert(key2, new BsonInt32(2));
        node.Insert(key3, new BsonInt32(3));

        // Act
        var position = node.FindKeyPosition(key2);

        // Assert
        await Assert.That(position).IsEqualTo(1);
    }

    [Test]
    public async Task FindKeyPosition_Should_Return_Insert_Position_For_Non_Existing_Key()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var key1 = new IndexKey(new BsonString("apple"));
        var key2 = new IndexKey(new BsonString("cherry"));

        node.Insert(key1, new BsonInt32(1));
        node.Insert(key2, new BsonInt32(3));

        var searchKey = new IndexKey(new BsonString("banana"));

        // Act
        var position = node.FindKeyPosition(searchKey);

        // Assert
        await Assert.That(position).IsEqualTo(1); // Should be inserted between apple and cherry
    }

    [Test]
    public async Task FindDocumentPosition_Should_Return_Correct_Position()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var key = new IndexKey(new BsonString("test"));
        var docId1 = new BsonInt32(1);
        var docId2 = new BsonInt32(2);

        node.Insert(key, docId1);
        node.Insert(key, docId2); // Same key, different document

        // Act
        var position = node.FindDocumentPosition(docId2);

        // Assert
        await Assert.That(position).IsEqualTo(0); // First position
    }

    [Test]
    public async Task FindDocumentPosition_Should_Return_Minus_One_For_Non_Existing_Document()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var key = new IndexKey(new BsonString("test"));
        var docId = new BsonInt32(1);

        node.Insert(key, docId);

        var searchDocId = new BsonInt32(99);

        // Act
        var position = node.FindDocumentPosition(searchDocId);

        // Assert
        await Assert.That(position).IsEqualTo(-1);
    }

    [Test]
    public async Task Insert_Into_Leaf_Node_Should_Work()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var key = new IndexKey(new BsonString("test"));
        var docId = new BsonInt32(1);

        // Act
        var needSplit = node.Insert(key, docId);

        // Assert
        await Assert.That(needSplit).IsFalse();
        await Assert.That(node.KeyCount).IsEqualTo(1);
        await Assert.That(node.GetKey(0)).IsEqualTo(key);
        await Assert.That(node.GetDocumentId(0)).IsEqualTo(docId);
    }

    [Test]
    public async Task Insert_Into_Internal_Node_Should_Throw_Exception()
    {
        // Arrange
        var node = new BTreeNode(100, false);
        var key = new IndexKey(new BsonString("test"));
        var docId = new BsonInt32(1);

        // Act & Assert
        await Assert.That(() => node.Insert(key, docId)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Insert_Same_Key_Should_Add_Document_At_Beginning()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var key = new IndexKey(new BsonString("test"));
        var docId1 = new BsonInt32(1);
        var docId2 = new BsonInt32(2);

        node.Insert(key, docId1);

        // Act
        var needSplit = node.Insert(key, docId2);

        // Assert
        await Assert.That(needSplit).IsFalse();
        await Assert.That(node.KeyCount).IsEqualTo(1); // Still one key
        await Assert.That(node.GetDocumentId(0)).IsEqualTo(docId2); // New doc at front
    }

    [Test]
    public async Task InsertChild_Into_Internal_Node_Should_Work()
    {
        // Arrange
        var node = new BTreeNode(100, false);
        var child = new BTreeNode(100, true);
        var key = new IndexKey(new BsonString("separator"));

        // Act
        var needSplit = node.InsertChild(key, child);

        // Assert
        await Assert.That(needSplit).IsFalse();
        await Assert.That(node.KeyCount).IsEqualTo(1);
        await Assert.That(node.ChildCount).IsEqualTo(1);
        await Assert.That(node.GetKey(0)).IsEqualTo(key);
        await Assert.That(ReferenceEquals(node.GetChild(0), child)).IsTrue();
    }

    [Test]
    public async Task InsertChild_Into_Leaf_Node_Should_Throw_Exception()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var child = new BTreeNode(100, true);
        var key = new IndexKey(new BsonString("separator"));

        // Act & Assert
        await Assert.That(() => node.InsertChild(key, child)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task Delete_From_Leaf_Node_Should_Work()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var key = new IndexKey(new BsonString("test"));
        var docId = new BsonInt32(1);
        node.Insert(key, docId);

        // Act
        var deleted = node.Delete(key, docId);

        // Assert
        await Assert.That(deleted).IsTrue();
        await Assert.That(node.KeyCount).IsEqualTo(0);
        await Assert.That(node.DocumentCount).IsEqualTo(0);
    }

    [Test]
    public async Task Delete_Non_Existing_Key_Should_Return_False()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var key1 = new IndexKey(new BsonString("test"));
        var key2 = new IndexKey(new BsonString("different"));
        var docId = new BsonInt32(1);
        node.Insert(key1, docId);

        // Act
        var deleted = node.Delete(key2, docId);

        // Assert
        await Assert.That(deleted).IsFalse();
    }

    [Test]
    public async Task Delete_Non_Existing_Document_Should_Return_False()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var key = new IndexKey(new BsonString("test"));
        var docId1 = new BsonInt32(1);
        var docId2 = new BsonInt32(99);
        node.Insert(key, docId1);

        // Act
        var deleted = node.Delete(key, docId2);

        // Assert
        await Assert.That(deleted).IsFalse();
    }

    [Test]
    public async Task Split_Leaf_Node_Should_Create_Two_Nodes()
    {
        // Arrange
        var node = new BTreeNode(4, true); // Small max keys for easy splitting
        for (int i = 0; i < 6; i++)
        {
            node.Insert(new IndexKey(new BsonInt32(i)), new BsonInt32(i));
        }

        // Act
        var newNode = node.Split();

        // Assert
        await Assert.That(node.KeyCount).IsEqualTo(3); // Original has first half
        await Assert.That(newNode.KeyCount).IsEqualTo(3); // New node has second half
        await Assert.That(node.GetKey(2)).IsNotEqualTo(newNode.GetKey(0)); // Verify split point
    }

    [Test]
    public async Task Split_Internal_Node_Should_Handle_Children_Correctly()
    {
        // Arrange
        var node = new BTreeNode(4, false);
        var child1 = new BTreeNode(4, true);
        var child2 = new BTreeNode(4, true);
        var child3 = new BTreeNode(4, true);
        var child4 = new BTreeNode(4, true);

        node.InsertChild(new IndexKey(new BsonInt32(1)), child1);
        node.InsertChild(new IndexKey(new BsonInt32(2)), child2);
        node.InsertChild(new IndexKey(new BsonInt32(3)), child3);

        // Act
        var newNode = node.Split();

        // Assert
        await Assert.That(node.KeyCount).IsGreaterThan(0);
        await Assert.That(newNode.KeyCount).IsGreaterThan(0);
        await Assert.That(node.ChildCount).IsGreaterThan(0);
        await Assert.That(newNode.ChildCount).IsGreaterThan(0);
    }

    [Test]
    public async Task Merge_Leaf_Nodes_Should_Combine_Keys()
    {
        // Arrange
        var node1 = new BTreeNode(100, true);
        var node2 = new BTreeNode(100, true);
        var separatorKey = new IndexKey(new BsonString("separator"));

        node1.Insert(new IndexKey(new BsonString("a")), new BsonInt32(1));
        node2.Insert(new IndexKey(new BsonString("b")), new BsonInt32(2));

        // Act
        node1.Merge(node2, separatorKey);

        // Assert
        await Assert.That(node1.KeyCount).IsEqualTo(2);
        await Assert.That(node1.GetKey(0)).IsEqualTo(new IndexKey(new BsonString("a")));
        await Assert.That(node1.GetKey(1)).IsEqualTo(new IndexKey(new BsonString("b")));
    }

    [Test]
    public async Task Merge_Different_Node_Types_Should_Throw_Exception()
    {
        // Arrange
        var leafNode = new BTreeNode(100, true);
        var internalNode = new BTreeNode(100, false);
        var separatorKey = new IndexKey(new BsonString("separator"));

        // Act & Assert
        await Assert.That(() => leafNode.Merge(internalNode, separatorKey)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task BorrowFromSibling_Leaf_Should_Work()
    {
        // Arrange
        var node1 = new BTreeNode(100, true);
        var node2 = new BTreeNode(100, true);
        var separatorKey = new IndexKey(new BsonString("separator"));

        node1.Insert(new IndexKey(new BsonString("a")), new BsonInt32(1));
        node2.Insert(new IndexKey(new BsonString("b")), new BsonInt32(2));
        node2.Insert(new IndexKey(new BsonString("c")), new BsonInt32(3));

        // Act
        var newSeparator = node1.BorrowFromSibling(node2, false, separatorKey);

        // Assert
        await Assert.That(node1.KeyCount).IsEqualTo(2);
        await Assert.That(node2.KeyCount).IsEqualTo(1);
        await Assert.That(newSeparator).IsEqualTo(new IndexKey(new BsonString("c")));
    }

    [Test]
    public async Task BorrowFromSibling_Leaf_WithLeftSibling_Should_Work()
    {
        // Arrange
        var target = new BTreeNode(4, true);
        var leftSibling = new BTreeNode(4, true);
        var separator = new IndexKey(new BsonString("separator"));

        leftSibling.Insert(new IndexKey(new BsonString("a")), new BsonInt32(1));
        leftSibling.Insert(new IndexKey(new BsonString("b")), new BsonInt32(2));
        leftSibling.Insert(new IndexKey(new BsonString("c")), new BsonInt32(3));
        target.Insert(new IndexKey(new BsonString("d")), new BsonInt32(4));

        // Act
        var newSeparator = target.BorrowFromSibling(leftSibling, true, separator);

        // Assert
        await Assert.That(target.KeyCount).IsEqualTo(2);
        await Assert.That(leftSibling.KeyCount).IsEqualTo(2);
        await Assert.That(((BsonString)target.GetKey(0).Values.First()).Value).IsEqualTo("c");
        await Assert.That(newSeparator).IsEqualTo(new IndexKey(new BsonString("c")));
    }

    [Test]
    public async Task BorrowFromSibling_Internal_WithLeftSibling_Should_AdoptBorrowedKey()
    {
        // Arrange
        var target = new BTreeNode(4, false);
        var leftSibling = new BTreeNode(4, false);
        var borrowedChild = new BTreeNode(4, true);
        var leftChild0 = new BTreeNode(4, true);
        var leftChild1 = new BTreeNode(4, true);
        var targetChild0 = new BTreeNode(4, true);
        var targetChild1 = new BTreeNode(4, true);
        var separator = new IndexKey(new BsonInt32(4));

        SetChildren(leftSibling, leftChild0, leftChild1, borrowedChild);
        SetKeys(leftSibling, new IndexKey(new BsonInt32(2)), new IndexKey(new BsonInt32(3)));
        leftChild0.Parent = leftSibling;
        leftChild1.Parent = leftSibling;
        borrowedChild.Parent = leftSibling;

        SetChildren(target, targetChild0, targetChild1);
        SetKeys(target, new IndexKey(new BsonInt32(5)));
        targetChild0.Parent = target;
        targetChild1.Parent = target;

        // Act
        var newSeparator = target.BorrowFromSibling(leftSibling, true, separator);

        // Assert
        var leftValues = GetKeyValues(leftSibling);
        await Assert.That(leftValues.Length).IsEqualTo(1);
        await Assert.That(leftValues[0]).IsEqualTo(2);

        var targetValues = GetKeyValues(target);
        await Assert.That(targetValues.Length).IsEqualTo(2);
        await Assert.That(targetValues[0]).IsEqualTo(4);
        await Assert.That(targetValues[1]).IsEqualTo(5);
        await Assert.That(newSeparator).IsEqualTo(new IndexKey(new BsonInt32(3)));
        await Assert.That(target.ChildCount).IsEqualTo(3);
        await Assert.That(target.GetChild(0)).IsSameReferenceAs(borrowedChild);
        await Assert.That(target.GetChild(0).Parent).IsSameReferenceAs(target);
    }

    [Test]
    public async Task BorrowFromSibling_Internal_WithRightSibling_Should_UpdateSeparator()
    {
        // Arrange
        var target = new BTreeNode(4, false);
        var rightSibling = new BTreeNode(4, false);
        var separator = new IndexKey(new BsonInt32(4));

        var targetChild0 = new BTreeNode(4, true);
        var targetChild1 = new BTreeNode(4, true);
        SetChildren(target, targetChild0, targetChild1);
        SetKeys(target, new IndexKey(new BsonInt32(2)));
        targetChild0.Parent = target;
        targetChild1.Parent = target;

        var borrowedChild = new BTreeNode(4, true);
        var rightChild1 = new BTreeNode(4, true);
        SetChildren(rightSibling, borrowedChild, rightChild1);
        SetKeys(rightSibling, new IndexKey(new BsonInt32(6)));
        borrowedChild.Parent = rightSibling;
        rightChild1.Parent = rightSibling;

        // Act
        var newSeparator = target.BorrowFromSibling(rightSibling, false, separator);

        // Assert
        var targetValues = GetKeyValues(target);
        await Assert.That(targetValues.Length).IsEqualTo(2);
        await Assert.That(targetValues[0]).IsEqualTo(2);
        await Assert.That(targetValues[1]).IsEqualTo(4);

        await Assert.That(rightSibling.KeyCount).IsEqualTo(0);
        await Assert.That(newSeparator).IsEqualTo(new IndexKey(new BsonInt32(6)));
        await Assert.That(target.ChildCount).IsEqualTo(3);
        await Assert.That(target.GetChild(target.ChildCount - 1)).IsSameReferenceAs(borrowedChild);
        await Assert.That(borrowedChild.Parent).IsSameReferenceAs(target);
    }


    [Test]
    public async Task IsFull_Should_Return_True_When_MaxKeys_Reached()
    {
        // Arrange
        var node = new BTreeNode(2, true); // Very small max keys
        node.Insert(new IndexKey(new BsonString("a")), new BsonInt32(1));
        node.Insert(new IndexKey(new BsonString("b")), new BsonInt32(2));

        // Act & Assert
        await Assert.That(node.IsFull()).IsTrue();
    }

    [Test]
    public async Task NeedsMerge_Should_Return_True_When_Below_MinKeys()
    {
        // Arrange
        var node = new BTreeNode(4, true); // Min keys = 2
        node.Insert(new IndexKey(new BsonString("a")), new BsonInt32(1)); // Only 1 key

        // Act & Assert
        await Assert.That(node.NeedsMerge()).IsTrue();
    }

    [Test]
    public async Task GetStatistics_Should_Return_Correct_Information()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        node.Insert(new IndexKey(new BsonString("test")), new BsonInt32(1));

        // Act
        var stats = node.GetStatistics();

        // Assert
        await Assert.That(stats.IsLeaf).IsTrue();
        await Assert.That(stats.KeyCount).IsEqualTo(1);
        await Assert.That(stats.DocumentCount).IsEqualTo(1);
        await Assert.That(stats.MaxKeys).IsEqualTo(100);
        await Assert.That(stats.MinKeys).IsEqualTo(50);
        await Assert.That(stats.IsFull).IsFalse();
        await Assert.That(stats.NeedsMerge).IsTrue(); // 1 < 50
    }

    [Test]
    public async Task ToString_Should_Return_Correct_Format()
    {
        // Arrange
        var leafNode = new BTreeNode(100, true);
        var internalNode = new BTreeNode(100, false);

        // Act
        var leafString = leafNode.ToString();
        var internalString = internalNode.ToString();

        // Assert
        await Assert.That(leafString).Contains("Leaf");
        await Assert.That(internalString).Contains("Internal");
    }

    [Test]
    public async Task SetKey_Should_Update_Key_Value()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var oldKey = new IndexKey(new BsonString("old"));
        var newKey = new IndexKey(new BsonString("new"));
        node.Insert(oldKey, new BsonInt32(1));

        // Act
        node.SetKey(0, newKey);

        // Assert
        await Assert.That(node.GetKey(0)).IsEqualTo(newKey);
    }

    [Test]
    public async Task RemoveKeyAt_Should_Remove_Key()
    {
        // Arrange
        var node = new BTreeNode(100, true);
        var key1 = new IndexKey(new BsonString("a"));
        var key2 = new IndexKey(new BsonString("b"));
        node.Insert(key1, new BsonInt32(1));
        node.Insert(key2, new BsonInt32(2));

        // Act
        node.RemoveKeyAt(0);

        // Assert
        await Assert.That(node.KeyCount).IsEqualTo(1);
        await Assert.That(node.GetKey(0)).IsEqualTo(key2);
    }

    private static void SetKeys(BTreeNode node, params IndexKey[] keys)
    {
        var keysField = typeof(BTreeNode).GetField("_keys", BindingFlags.NonPublic | BindingFlags.Instance);
        var list = (List<IndexKey>)(keysField?.GetValue(node) ?? throw new InvalidOperationException("Keys list not found"));
        list.Clear();
        list.AddRange(keys);
    }

    private static void SetChildren(BTreeNode node, params BTreeNode[] children)
    {
        var childrenField = typeof(BTreeNode).GetField("_children", BindingFlags.NonPublic | BindingFlags.Instance);
        var list = (List<BTreeNode>)(childrenField?.GetValue(node) ?? throw new InvalidOperationException("Children list not found"));
        list.Clear();
        list.AddRange(children);
    }

    private static int[] GetKeyValues(BTreeNode node)
    {
        return Enumerable.Range(0, node.KeyCount)
            .Select(i => node.GetKey(i))
            .Select(k => (BsonInt32)k.Values.First())
            .Select(v => v.Value)
            .ToArray();
    }
}
