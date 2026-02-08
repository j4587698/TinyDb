using System;
using System.IO;
using System.Linq;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Index;
using TinyDb.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;

namespace TinyDb.Tests.Index;

public sealed class DiskBTreeValidateAdditionalCoverageTests
{
    [Test]
    public async Task Validate_WhenNodeKeysNotStrictlyIncreasing_ShouldReturnFalse()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_validate_sort_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            var tree = CreatePopulatedTree(pm, count: 20);

            var root = tree.RootNode;
            await Assert.That(root.KeyCount).IsGreaterThanOrEqualTo(2);

            // Corrupt ordering: make first key >= second key
            (root.Keys[0], root.Keys[1]) = (root.Keys[1], root.Keys[0]);
            root.MarkDirty();
            root.Save(pm);

            await Assert.That(tree.Validate()).IsFalse();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Validate_WhenChildParentPointerMismatch_ShouldReturnFalse()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_validate_parent_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            var tree = CreatePopulatedTree(pm, count: 40);

            var root = tree.RootNode;
            await Assert.That(root.IsLeaf).IsFalse();
            await Assert.That(root.ChildrenIds.Count).IsGreaterThanOrEqualTo(1);

            var child = LoadNode(tree, root.ChildrenIds[0]);
            child.SetParent(0);
            child.Save(pm);

            await Assert.That(tree.Validate()).IsFalse();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Validate_WhenChildFirstKeyBelowMinKey_ShouldReturnFalse()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_validate_minkey_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            var tree = CreatePopulatedTree(pm, count: 60);

            var root = tree.RootNode;
            await Assert.That(root.IsLeaf).IsFalse();
            await Assert.That(root.ChildrenIds.Count).IsGreaterThanOrEqualTo(2);
            await Assert.That(root.KeyCount).IsGreaterThanOrEqualTo(1);

            var minKey = root.Keys[0];
            var child = LoadNode(tree, root.ChildrenIds[1]);
            await Assert.That(child.KeyCount).IsGreaterThanOrEqualTo(1);

            // Ensure the child's first key is still sorted within the node but violates the parent's minKey constraint
            child.Keys[0] = new IndexKey(new BsonInt32(((BsonInt32)minKey.Values[0]).Value - 1));
            child.MarkDirty();
            child.Save(pm);

            await Assert.That(tree.Validate()).IsFalse();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    [Test]
    public async Task Validate_WhenChildLastKeyAboveMaxKey_ShouldReturnFalse()
    {
        var path = Path.Combine(Path.GetTempPath(), $"btree_validate_maxkey_{Guid.NewGuid():N}.db");

        try
        {
            using var ds = new DiskStream(path);
            using var pm = new PageManager(ds, 4096);
            var tree = CreatePopulatedTree(pm, count: 60);

            var root = tree.RootNode;
            await Assert.That(root.IsLeaf).IsFalse();
            await Assert.That(root.ChildrenIds.Count).IsGreaterThanOrEqualTo(1);
            await Assert.That(root.KeyCount).IsGreaterThanOrEqualTo(1);

            var maxKey = root.Keys[0];
            var child = LoadNode(tree, root.ChildrenIds[0]);
            await Assert.That(child.KeyCount).IsGreaterThanOrEqualTo(1);

            var lastIndex = child.KeyCount - 1;
            child.Keys[lastIndex] = new IndexKey(new BsonInt32(((BsonInt32)maxKey.Values[0]).Value + 1));
            child.MarkDirty();
            child.Save(pm);

            await Assert.That(tree.Validate()).IsFalse();
        }
        finally
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    private static DiskBTree CreatePopulatedTree(PageManager pm, int count)
    {
        var tree = DiskBTree.Create(pm, maxKeys: 3);
        for (int i = 0; i < count; i++)
        {
            tree.Insert(new IndexKey(new BsonInt32(i)), new BsonInt32(i));
        }
        return tree;
    }

    private static DiskBTreeNode LoadNode(DiskBTree tree, uint pageId)
    {
        var method = typeof(DiskBTree).GetMethod("LoadNode", BindingFlags.NonPublic | BindingFlags.Instance);
        return (DiskBTreeNode)method!.Invoke(tree, new object[] { pageId })!;
    }
}

