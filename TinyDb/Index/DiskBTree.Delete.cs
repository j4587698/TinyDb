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
    /// 从树中删除一个键值对。
    /// </summary>
    /// <param name="key">键。</param>
    /// <param name="value">值。</param>
    /// <returns>如果找到并删除成功则为 true。</returns>
    public bool Delete(IndexKey key, BsonValue value)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(key);

        bool deleted = false;
        DiskBTreeNode? deletionNode = null;

        while (node != null)
        {
            bool passed = false;
            for (int i = 0; i < node.Keys.Count; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0 && ValuesEqual(node.Values[i], value))
                {
                    node.Keys.RemoveAt(i);
                    node.Values.RemoveAt(i);
                    node.MarkDirty();
                    node.Save(_pm);
                    deleted = true;
                    deletionNode = node;
                    break;
                }
                else if (cmp > 0)
                {
                    passed = true;
                    break;
                }
            }

            if (deleted) break;
            if (passed) break;
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }

        if (deleted)
        {
            var root = LoadNode(_rootPageId);
            root.SetTreeEntryCount(root.TreeEntryCount - 1);
            root.Save(_pm);

            if (deletionNode != null && deletionNode.PageId != _rootPageId)
            {
                Rebalance(deletionNode);
            }
        }
        return deleted;
    }

    private void Rebalance(DiskBTreeNode node)
    {
        if (node.PageId == _rootPageId)
        {
            if (!node.IsLeaf && node.KeyCount == 0 && node.ChildrenIds.Count > 0)
            {
                var child = LoadNode(node.ChildrenIds[0]);
                node.SetLeaf(child.IsLeaf);
                node.Keys.Clear(); node.Keys.AddRange(child.Keys);
                node.Values.Clear(); node.Values.AddRange(child.Values);
                node.ChildrenIds.Clear(); node.ChildrenIds.AddRange(child.ChildrenIds);
                node.SetNext(child.NextSiblingId);

                if (!node.IsLeaf)
                {
                    foreach(var gcId in node.ChildrenIds)
                    {
                        var gc = LoadNode(gcId);
                        gc.SetParent(node.PageId);
                        gc.Save(_pm);
                    }
                }

                node.MarkDirty();
                node.Save(_pm);
                FreePage(child.PageId);
            }
            return;
        }

        int minKeys = GetMinimumKeyCount(node);
        if (node.KeyCount >= minKeys) return;

        if (!TryResolveParent(node, out var parent, out var childIndex))
            throw new InvalidDataException($"Unable to locate parent for B+Tree node {node.PageId}.");

        if (childIndex > 0)
        {
            var leftSibling = LoadNode(parent.ChildrenIds[childIndex - 1]);
            if (leftSibling.KeyCount > minKeys)
            {
                BorrowFromLeft(node, leftSibling, parent, childIndex - 1);
                return;
            }
        }

        if (childIndex < parent.ChildrenIds.Count - 1)
        {
            var rightSibling = LoadNode(parent.ChildrenIds[childIndex + 1]);
            if (rightSibling.KeyCount > minKeys)
            {
                BorrowFromRight(node, rightSibling, parent, childIndex);
                return;
            }
        }

        if (childIndex > 0)
        {
            var leftSibling = LoadNode(parent.ChildrenIds[childIndex - 1]);
            if (CanMerge(leftSibling, node))
            {
                MergeNodes(parent, leftSibling, node, childIndex - 1);
                Rebalance(parent);
                return;
            }
        }

        if (childIndex < parent.ChildrenIds.Count - 1)
        {
            var rightSibling = LoadNode(parent.ChildrenIds[childIndex + 1]);
            if (CanMerge(node, rightSibling))
            {
                MergeNodes(parent, node, rightSibling, childIndex);
                Rebalance(parent);
                return;
            }
        }
    }

    private bool TryResolveParent(DiskBTreeNode node, [NotNullWhen(true)] out DiskBTreeNode? parent, out int childIndex)
    {
        parent = null;
        childIndex = -1;

        if (node.ParentId != 0)
        {
            var candidate = LoadNode(node.ParentId);
            childIndex = candidate.ChildrenIds.IndexOf(node.PageId);
            if (childIndex >= 0)
            {
                parent = candidate;
                return true;
            }
        }

        if (TryFindParent(_rootPageId, node.PageId, new HashSet<uint>(), depth: 0, out parent, out childIndex))
        {
            node.SetParent(parent.PageId);
            node.Save(_pm);
            return true;
        }

        return false;
    }

    private bool TryFindParent(
        uint currentPageId,
        uint childPageId,
        HashSet<uint> visited,
        int depth,
        [NotNullWhen(true)] out DiskBTreeNode? parent,
        out int childIndex)
    {
        parent = null;
        childIndex = -1;
        if (depth > _maxKeys + 64 || !visited.Add(currentPageId))
        {
            return false;
        }

        var current = LoadNode(currentPageId);
        if (current.IsLeaf) return false;

        childIndex = current.ChildrenIds.IndexOf(childPageId);
        if (childIndex >= 0)
        {
            parent = current;
            return true;
        }

        foreach (var nextChildId in current.ChildrenIds)
        {
            if (TryFindParent(nextChildId, childPageId, visited, depth + 1, out parent, out childIndex))
            {
                return true;
            }
        }

        return false;
    }

    private static bool ValuesEqual(BsonValue left, BsonValue right)
    {
        return BsonValueComparer.Compare(left, right) == 0;
    }

    private bool CanMerge(DiskBTreeNode left, DiskBTreeNode right)
    {
        // DiskBTreeNode.Save stores oversized node payloads in an overflow chain, so
        // single-page byte capacity must not block a logically valid merge.
        var mergedKeyCount = left.KeyCount + right.KeyCount + (left.IsLeaf ? 0 : 1);
        return mergedKeyCount <= _maxKeys;
    }

    private int GetMinimumKeyCount(DiskBTreeNode node)
    {
        return Math.Max(1, node.IsLeaf ? _maxKeys / 2 : (_maxKeys - 1) / 2);
    }

    private void BorrowFromLeft(DiskBTreeNode node, DiskBTreeNode leftSibling, DiskBTreeNode parent, int leftSiblingIndex)
    {
        if (node.IsLeaf)
        {
            var key = leftSibling.Keys[leftSibling.KeyCount - 1];
            var val = leftSibling.Values[leftSibling.Values.Count - 1];
            leftSibling.Keys.RemoveAt(leftSibling.KeyCount - 1);
            leftSibling.Values.RemoveAt(leftSibling.Values.Count - 1);

            node.Keys.Insert(0, key);
            node.Values.Insert(0, val);

            // 更新父节点的分隔符为当前节点新的第一个键
            parent.Keys[leftSiblingIndex] = node.Keys[0];
        }
        else
        {
            var key = leftSibling.Keys[leftSibling.KeyCount - 1];
            var childId = leftSibling.ChildrenIds[leftSibling.ChildrenIds.Count - 1];
            leftSibling.Keys.RemoveAt(leftSibling.KeyCount - 1);
            leftSibling.ChildrenIds.RemoveAt(leftSibling.ChildrenIds.Count - 1);

            // 旋转操作：父节点分隔符移入子节点，左兄弟最后一个键移入父节点
            var separator = parent.Keys[leftSiblingIndex];
            node.Keys.Insert(0, separator);
            node.ChildrenIds.Insert(0, childId);

            parent.Keys[leftSiblingIndex] = key;

            var child = LoadNode(childId);
            child.SetParent(node.PageId);
            child.Save(_pm);
        }

        leftSibling.MarkDirty(); leftSibling.Save(_pm);
        node.MarkDirty(); node.Save(_pm);
        parent.MarkDirty(); parent.Save(_pm);
    }

    private void BorrowFromRight(DiskBTreeNode node, DiskBTreeNode rightSibling, DiskBTreeNode parent, int nodeIndex)
    {
        if (node.IsLeaf)
        {
            var key = rightSibling.Keys[0];
            var val = rightSibling.Values[0];
            rightSibling.Keys.RemoveAt(0);
            rightSibling.Values.RemoveAt(0);

            node.Keys.Add(key);
            node.Values.Add(val);

            // 更新父节点的分隔符为右兄弟新的第一个键
            parent.Keys[nodeIndex] = rightSibling.KeyCount > 0 ? rightSibling.Keys[0] : key;
        }
        else
        {
            var key = rightSibling.Keys[0];
            var childId = rightSibling.ChildrenIds[0];
            rightSibling.Keys.RemoveAt(0);
            rightSibling.ChildrenIds.RemoveAt(0);

            // 旋转操作：父节点分隔符移入子节点，右兄弟第一个键移入父节点
            var separator = parent.Keys[nodeIndex];
            node.Keys.Add(separator);
            node.ChildrenIds.Add(childId);

            parent.Keys[nodeIndex] = key;

            var child = LoadNode(childId);
            child.SetParent(node.PageId);
            child.Save(_pm);
        }

        rightSibling.MarkDirty(); rightSibling.Save(_pm);
        node.MarkDirty(); node.Save(_pm);
        parent.MarkDirty(); parent.Save(_pm);
    }

    private void MergeNodes(DiskBTreeNode parent, DiskBTreeNode left, DiskBTreeNode right, int leftIndex)
    {
        if (left.IsLeaf)
        {
            left.Keys.AddRange(right.Keys);
            left.Values.AddRange(right.Values);

            left.SetNext(right.NextSiblingId);
            if (right.NextSiblingId != 0)
            {
                var next = LoadNode(right.NextSiblingId);
                next.SetPrev(left.PageId);
                next.Save(_pm);
            }

            parent.Keys.RemoveAt(leftIndex);
            parent.ChildrenIds.RemoveAt(leftIndex + 1);
        }
        else
        {
            var separator = parent.Keys[leftIndex];
            left.Keys.Add(separator);

            left.Keys.AddRange(right.Keys);
            left.ChildrenIds.AddRange(right.ChildrenIds);

            foreach(var childId in right.ChildrenIds)
            {
                var c = LoadNode(childId);
                c.SetParent(left.PageId);
                c.Save(_pm);
            }

            parent.Keys.RemoveAt(leftIndex);
            parent.ChildrenIds.RemoveAt(leftIndex + 1);
        }

        left.MarkDirty(); left.Save(_pm);
        FreePage(right.PageId);
        parent.MarkDirty(); parent.Save(_pm);
    }
}
