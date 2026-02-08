using System;
using TinyDb.Bson;
using TinyDb.Index;
using TinyDb.Storage;
using TinyDb.Tests.Storage;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

public class BTreeNodeBranchCoverageTests
{
    [Test]
    public async Task Ctor_NullArguments_ShouldThrow()
    {
        using var pm = new PageManager(new MockDiskStream());
        using var tree = DiskBTree.Create(pm, maxKeys: 2);

        var page = pm.NewPage(PageType.Index);
        var diskNode = new DiskBTreeNode(page, pm);

        await Assert.That(() => new BTreeNode(null!, tree)).Throws<ArgumentNullException>();
        await Assert.That(() => new BTreeNode(diskNode, null!)).Throws<ArgumentNullException>();
    }

    [Test]
    public async Task ChildCount_And_ToString_ShouldCoverLeafAndInternalBranches()
    {
        using var pm = new PageManager(new MockDiskStream());
        using var tree = DiskBTree.Create(pm, maxKeys: 2);

        var page = pm.NewPage(PageType.Index);
        var diskNode = new DiskBTreeNode(page, pm);

        var leaf = new BTreeNode(diskNode, tree);
        await Assert.That(leaf.ChildCount).IsEqualTo(0);
        await Assert.That(leaf.ToString()).Contains("Leaf");

        diskNode.SetLeaf(false);
        diskNode.ChildrenIds.Add(1);
        diskNode.ChildrenIds.Add(2);

        await Assert.That(leaf.ChildCount).IsEqualTo(2);
        await Assert.That(leaf.ToString()).Contains("Internal");
    }

    [Test]
    public async Task FindKeyPosition_ShouldCoverLessThanEqualAndDefaultBranches()
    {
        using var pm = new PageManager(new MockDiskStream());
        using var tree = DiskBTree.Create(pm, maxKeys: 2);

        var page = pm.NewPage(PageType.Index);
        var diskNode = new DiskBTreeNode(page, pm);
        diskNode.Keys.Add(new IndexKey(new BsonInt32(10)));
        diskNode.Keys.Add(new IndexKey(new BsonInt32(20)));

        var node = new BTreeNode(diskNode, tree);

        await Assert.That(node.FindKeyPosition(new IndexKey(new BsonInt32(5)))).IsEqualTo(0);
        await Assert.That(node.FindKeyPosition(new IndexKey(new BsonInt32(10)))).IsEqualTo(0);
        await Assert.That(node.FindKeyPosition(new IndexKey(new BsonInt32(30)))).IsEqualTo(2);
    }

    [Test]
    public async Task GetChild_WhenLeaf_ShouldThrow()
    {
        using var pm = new PageManager(new MockDiskStream());
        using var tree = DiskBTree.Create(pm, maxKeys: 2);

        var leaf = new BTreeNode(tree.RootNode, tree);
        await Assert.That(() => leaf.GetChild(0)).Throws<InvalidOperationException>();
    }

    [Test]
    public async Task GetChild_WhenInternal_ShouldReturnChild()
    {
        using var pm = new PageManager(new MockDiskStream());
        using var tree = DiskBTree.Create(pm, maxKeys: 2);

        tree.Insert(new IndexKey(new BsonInt32(1)), new BsonInt32(1));
        tree.Insert(new IndexKey(new BsonInt32(2)), new BsonInt32(2));
        tree.Insert(new IndexKey(new BsonInt32(3)), new BsonInt32(3));

        var root = tree.RootNode;
        await Assert.That(root.IsLeaf).IsFalse();

        var wrapper = new BTreeNode(root, tree);
        var child = wrapper.GetChild(0);
        await Assert.That(child).IsNotNull();
    }
}

