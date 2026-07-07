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

    private DiskBTreeNode FindLeafNode(IndexKey key)
    {
        var node = LoadNode(_rootPageId);
        while (!node.IsLeaf)
        {
            int childIdx = UpperBound(node.Keys, key);
            node = LoadNode(node.ChildrenIds[childIdx]);
        }
        return node;
    }

    private DiskBTreeNode FindFirstCandidateLeafNode(IndexKey key)
    {
        var node = FindLeafNode(key);

        while (node.PrevSiblingId != 0)
        {
            var prev = LoadNode(node.PrevSiblingId);
            if (prev.KeyCount == 0 || prev.Keys[prev.KeyCount - 1].CompareTo(key) < 0)
            {
                break;
            }

            node = prev;
        }

        return node;
    }

    private DiskBTreeNode FindLastCandidateLeafNode(IndexKey key)
    {
        var node = FindLeafNode(key);

        while (node.NextSiblingId != 0)
        {
            var next = LoadNode(node.NextSiblingId);
            if (next.KeyCount == 0 || next.Keys[0].CompareTo(key) > 0)
            {
                break;
            }

            node = next;
        }

        return node;
    }

    private async Task<DiskBTreeNode> FindLeafNodeAsync(IndexKey key, CancellationToken cancellationToken)
    {
        var node = await LoadNodeAsync(_rootPageId, cancellationToken).ConfigureAwait(false);
        while (!node.IsLeaf)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int childIdx = UpperBound(node.Keys, key);
            node = await LoadNodeAsync(node.ChildrenIds[childIdx], cancellationToken).ConfigureAwait(false);
        }
        return node;
    }

    private async Task<DiskBTreeNode> FindFirstCandidateLeafNodeAsync(IndexKey key, CancellationToken cancellationToken)
    {
        var node = await FindLeafNodeAsync(key, cancellationToken).ConfigureAwait(false);

        while (node.PrevSiblingId != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var prev = await LoadNodeAsync(node.PrevSiblingId, cancellationToken).ConfigureAwait(false);
            if (prev.KeyCount == 0 || prev.Keys[prev.KeyCount - 1].CompareTo(key) < 0)
            {
                break;
            }

            node = prev;
        }

        return node;
    }

    private async Task<DiskBTreeNode> FindLastCandidateLeafNodeAsync(IndexKey key, CancellationToken cancellationToken)
    {
        var node = await FindLeafNodeAsync(key, cancellationToken).ConfigureAwait(false);

        while (node.NextSiblingId != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var next = await LoadNodeAsync(node.NextSiblingId, cancellationToken).ConfigureAwait(false);
            if (next.KeyCount == 0 || next.Keys[0].CompareTo(key) > 0)
            {
                break;
            }

            node = next;
        }

        return node;
    }
}
