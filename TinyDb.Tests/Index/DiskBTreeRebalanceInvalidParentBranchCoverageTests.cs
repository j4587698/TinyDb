using System;
using System.Reflection;
using TinyDb.Index;
using TinyDb.Storage;
using TinyDb.Tests.Storage;
using TUnit.Assertions;
using TUnit.Core;

namespace TinyDb.Tests.Index;

public class DiskBTreeRebalanceInvalidParentBranchCoverageTests
{
    [Test]
    public async Task Rebalance_WhenChildNotFoundInParent_ShouldReturn()
    {
        using var pm = new PageManager(new MockDiskStream());
        using var tree = DiskBTree.Create(pm, maxKeys: 4);

        var parentPage = pm.NewPage(PageType.Index);
        var parent = new DiskBTreeNode(parentPage, pm);
        parent.SetLeaf(false);
        parent.ChildrenIds.Add(12345); // does NOT include child's PageId
        parent.Save(pm);

        var childPage = pm.NewPage(PageType.Index);
        var child = new DiskBTreeNode(childPage, pm);
        child.SetParent(parent.PageId);

        var rebalance = typeof(DiskBTree).GetMethod("Rebalance", BindingFlags.NonPublic | BindingFlags.Instance);
        await Assert.That(rebalance).IsNotNull();

        // Should not throw, should just return (childIndex == -1)
        rebalance!.Invoke(tree, new object[] { child });
    }
}

