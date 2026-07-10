using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Storage;

namespace TinyDb.Index;

public sealed partial class DiskBTree
{

    /// <summary>
    /// 向树中插入一个键值对。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    public void Insert(IndexKey key, BsonValue value)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var root = LoadNode(_rootPageId);
        if (root.IsFull((int)_pm.PageSize) || root.KeyCount >= _maxKeys)
        {
            SplitRoot(root);
            InsertNonFull(root, key, value);
        }
        else
        {
            if (root.IsLeaf)
            {
                int pos = LowerBound(root.Keys, key);
                root.Keys.Insert(pos, key);
                root.Values.Insert(pos, value);
                root.SetTreeEntryCount(root.TreeEntryCount + 1);
                root.MarkDirty();
                root.Save(_pm);
                return;
            }

            InsertNonFull(root, key, value);
        }

        root.SetTreeEntryCount(root.TreeEntryCount + 1);
        root.Save(_pm);
    }

    public bool TryInsertUnique(IndexKey key, BsonValue value)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();

        var path = new List<(DiskBTreeNode Parent, int ChildIndex)>();
        var root = LoadNode(_rootPageId);
        var node = root;
        while (!node.IsLeaf)
        {
            int childIndex = UpperBound(node.Keys, key);
            path.Add((node, childIndex));
            node = LoadNode(node.ChildrenIds[childIndex]);
        }

        int position = LowerBound(node.Keys, key);
        if (position < node.KeyCount && node.Keys[position].CompareTo(key) == 0)
        {
            return false;
        }

        node.Keys.Insert(position, key);
        node.Values.Insert(position, value);
        node.MarkDirty();

        var shouldSplit = ShouldSplit(node);
        if (node.PageId == _rootPageId && !shouldSplit)
        {
            node.SetTreeEntryCount(node.TreeEntryCount + 1);
            node.Save(_pm);
            return true;
        }

        if (shouldSplit)
        {
            SplitOverflowNode(node, path);
        }
        else
        {
            node.Save(_pm);
        }

        var rootForCount = shouldSplit ? LoadNode(_rootPageId) : root;
        rootForCount.SetTreeEntryCount(rootForCount.TreeEntryCount + 1);
        rootForCount.Save(_pm);
        return true;
    }

    private bool ShouldSplit(DiskBTreeNode node)
    {
        return node.IsFull((int)_pm.PageSize) || node.KeyCount > _maxKeys;
    }

    private void SplitOverflowNode(DiskBTreeNode node, List<(DiskBTreeNode Parent, int ChildIndex)> path)
    {
        while (ShouldSplit(node))
        {
            if (node.PageId == _rootPageId || path.Count == 0)
            {
                SplitRoot(node);
                return;
            }

            var parentRef = path[^1];
            path.RemoveAt(path.Count - 1);
            SplitChild(parentRef.Parent, parentRef.ChildIndex, node);
            node = parentRef.Parent;
        }
    }

    private void SplitRoot(DiskBTreeNode root)
    {
        var newChildPage = NewIndexPage();
        var newChild = new DiskBTreeNode(newChildPage, _pm);
        newChild.Keys.Clear();
        newChild.ChildrenIds.Clear();
        newChild.Values.Clear();

        newChild.SetLeaf(root.IsLeaf);
        newChild.Keys.AddRange(root.Keys);
        newChild.ChildrenIds.AddRange(root.ChildrenIds);
        newChild.Values.AddRange(root.Values);
        newChild.SetNext(root.NextSiblingId);
        newChild.SetParent(root.PageId);

        if (!newChild.IsLeaf)
        {
            foreach (var childId in newChild.ChildrenIds)
            {
                var child = LoadNode(childId);
                child.SetParent(newChild.PageId);
                child.Save(_pm);
            }
        }

        long currentCount = root.TreeEntryCount;
        root.InitAsRoot();
        root.SetLeaf(false);
        root.SetTreeEntryCount(currentCount);
        root.ChildrenIds.Add(newChild.PageId);
        root.MarkDirty();
        root.Save(_pm);

        SplitChild(root, 0, newChild);
    }

    private void InsertNonFull(DiskBTreeNode node, IndexKey key, BsonValue value)
    {
        if (node.IsLeaf)
        {
            int pos = LowerBound(node.Keys, key);
            node.Keys.Insert(pos, key);
            node.Values.Insert(pos, value);
            node.MarkDirty();
            node.Save(_pm);
        }
        else
        {
            int i = UpperBound(node.Keys, key);
            var child = LoadNode(node.ChildrenIds[i]);
            if (child.IsFull((int)_pm.PageSize) || child.KeyCount >= _maxKeys)
            {
                SplitChild(node, i, child);
                if (node.Keys[i].CompareTo(key) < 0) i++;
                child = LoadNode(node.ChildrenIds[i]);
            }
            InsertNonFull(child, key, value);
        }
    }

    private static int LowerBound(List<IndexKey> keys, IndexKey key)
    {
        int low = 0;
        int high = keys.Count;

        while (low < high)
        {
            int mid = low + ((high - low) >> 1);
            if (keys[mid].CompareTo(key) < 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        return low;
    }

    private static int UpperBound(List<IndexKey> keys, IndexKey key)
    {
        int low = 0;
        int high = keys.Count;

        while (low < high)
        {
            int mid = low + ((high - low) >> 1);
            if (keys[mid].CompareTo(key) <= 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }

        return low;
    }

    private void SplitChild(DiskBTreeNode parent, int index, DiskBTreeNode child)
    {
        var newNode = new DiskBTreeNode(NewIndexPage(), _pm);
        // Ensure garbage from recycled page is cleared
        newNode.Keys.Clear(); newNode.ChildrenIds.Clear(); newNode.Values.Clear();

        newNode.SetLeaf(child.IsLeaf);
        newNode.SetParent(parent.PageId);

        int mid = child.KeyCount / 2;
        IndexKey upKey = child.Keys[mid];

        if (child.IsLeaf)
        {
            var moveCount = child.KeyCount - mid;
            if (newNode.Keys.Capacity < moveCount) newNode.Keys.Capacity = moveCount;
            if (newNode.Values.Capacity < moveCount) newNode.Values.Capacity = moveCount;

            for(int j = mid; j < child.KeyCount; j++)
            {
                newNode.Keys.Add(child.Keys[j]);
                newNode.Values.Add(child.Values[j]);
            }
            child.Keys.RemoveRange(mid, moveCount);
            child.Values.RemoveRange(mid, moveCount);
            child.MarkDirty();

            newNode.SetNext(child.NextSiblingId);
            newNode.SetPrev(child.PageId);
            child.SetNext(newNode.PageId);

            if (newNode.NextSiblingId != 0)
            {
                var nextSib = LoadNode(newNode.NextSiblingId);
                nextSib.SetPrev(newNode.PageId);
                nextSib.Save(_pm);
            }
        }
        else
        {
            var moveKeyCount = child.KeyCount - (mid + 1);
            var moveChildCount = child.ChildrenIds.Count - (mid + 1);
            if (newNode.Keys.Capacity < moveKeyCount) newNode.Keys.Capacity = moveKeyCount;
            if (newNode.ChildrenIds.Capacity < moveChildCount) newNode.ChildrenIds.Capacity = moveChildCount;

            for(int j = mid + 1; j < child.KeyCount; j++)
                newNode.Keys.Add(child.Keys[j]);
            for(int j = mid + 1; j < child.ChildrenIds.Count; j++)
                newNode.ChildrenIds.Add(child.ChildrenIds[j]);

            foreach(var movedChildId in newNode.ChildrenIds)
            {
                var movedChild = LoadNode(movedChildId);
                movedChild.SetParent(newNode.PageId);
                movedChild.Save(_pm);
            }

            child.Keys.RemoveRange(mid, child.KeyCount - mid);
            child.ChildrenIds.RemoveRange(mid + 1, child.ChildrenIds.Count - (mid + 1));
            child.MarkDirty();
        }
        newNode.Save(_pm);
        child.Save(_pm);

        parent.Keys.Insert(index, upKey);
        parent.ChildrenIds.Insert(index + 1, newNode.PageId);
        parent.MarkDirty();
        parent.Save(_pm);
    }
}
