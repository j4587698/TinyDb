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
    /// 检查树是否包含某个键。
    /// </summary>
    public bool Contains(IndexKey key)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(key);

        while (node != null)
        {
            for (int i = 0; i < node.KeyCount; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0) return true;
                if (cmp > 0) return false;
            }

            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }

        return false;
    }

    /// <summary>
    /// 检查树是否包含特定的键值对。
    /// </summary>
    public bool Contains(IndexKey key, BsonValue value)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(key);

        while (node != null)
        {
            for (int i = 0; i < node.KeyCount; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0 && ValuesEqual(node.Values[i], value)) return true;
                if (cmp > 0) return false;
            }

            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }

        return false;
    }

    /// <summary>
    /// 获取树中的所有值。
    /// </summary>
    public IEnumerable<BsonValue> GetAll()
    {
        ThrowIfDisposed();
        var results = new List<BsonValue>();
        using var lease = BeginPageLease();
        var node = LoadNode(_rootPageId);
        while (!node.IsLeaf) node = LoadNode(node.ChildrenIds[0]);
        while (node != null)
        {
            results.AddRange(node.Values);
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }

        return results;
    }

    /// <summary>
    /// 获取树中的所有值（按降序返回）。
    /// </summary>
    public IEnumerable<BsonValue> GetAllReverse()
    {
        ThrowIfDisposed();
        var results = new List<BsonValue>();
        using var lease = BeginPageLease();
        var node = LoadNode(_rootPageId);
        while (!node.IsLeaf) node = LoadNode(node.ChildrenIds[^1]);

        while (node != null)
        {
            for (int i = node.Values.Count - 1; i >= 0; i--)
            {
                results.Add(node.Values[i]);
            }

            if (node.PrevSiblingId == 0) break;
            node = LoadNode(node.PrevSiblingId);
        }

        return results;
    }

    /// <summary>
    /// 清空树。
    /// </summary>
    public void Clear()
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var root = LoadNode(_rootPageId);
        var visited = new HashSet<uint>();

        foreach (var childId in root.ChildrenIds.ToArray())
        {
            FreeSubtree(childId, visited);
        }

        root.InitAsRoot();
        root.Save(_pm);
    }

    internal void DropPages()
    {
        if (_disposed) return;

        using var lease = BeginPageLease();
        var visited = new HashSet<uint>();
        FreeSubtree(_rootPageId, visited);
        _disposed = true;
    }

    private void FreeSubtree(uint pageId, HashSet<uint> visited)
    {
        if (pageId == 0 || !visited.Add(pageId)) return;

        Page? page = null;
        DiskBTreeNode? node = null;
        uint overflowPageId = 0;

        try
        {
            page = GetPage(pageId);
            overflowPageId = page.Header.NextPageID;

            if (page.PageType == PageType.Index && page.Header.ItemCount > 0)
            {
                node = LoadNode(pageId);
            }
        }
        catch
        {
            // 损坏的索引页仍应释放；具体损坏会在校验或读取路径暴露。
        }

        if (node != null && !node.IsLeaf)
        {
            foreach (var childId in node.ChildrenIds.ToArray())
            {
                FreeSubtree(childId, visited);
            }
        }

        FreeOverflowChain(overflowPageId, visited);
        FreePage(pageId);
    }

    private void FreeOverflowChain(uint pageId, HashSet<uint> visited)
    {
        while (pageId != 0 && visited.Add(pageId))
        {
            var page = GetPage(pageId);
            var nextPageId = page.Header.NextPageID;
            FreePage(pageId);
            pageId = nextPageId;
        }
    }

    /// <summary>
    /// 验证树结构。
    /// </summary>
    /// <returns>如果有效则为 true。</returns>
    public bool Validate()
    {
        try
        {
            ThrowIfDisposed();
            using var lease = BeginPageLease();
            var root = LoadNode(_rootPageId);
            return ValidateNode(root, null, null);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return false;
        }
    }

    private bool ValidateNode(DiskBTreeNode node, IndexKey? minKey, IndexKey? maxKey)
    {
        if (node.PageId != _rootPageId)
        {
            int minKeys = GetMinimumKeyCount(node);
            if (node.KeyCount < minKeys) return false;
        }

        if (node.KeyCount > _maxKeys) return false;

        // 1. Validate keys are sorted
        for (int i = 0; i < node.KeyCount - 1; i++)
        {
            if (node.Keys[i].CompareTo(node.Keys[i + 1]) > 0) return false;
        }

        // 2. Validate keys are within range [minKey, maxKey]
        if (node.KeyCount > 0)
        {
            if (minKey != null && node.Keys[0].CompareTo(minKey) < 0) return false;
            if (maxKey != null && node.Keys[node.KeyCount - 1].CompareTo(maxKey) > 0) return false;
        }

        // 3. Recursively validate children
        if (!node.IsLeaf)
        {
            if (node.ChildrenIds.Count != node.KeyCount + 1) return false;

            for (int i = 0; i < node.ChildrenIds.Count; i++)
            {
                var child = LoadNode(node.ChildrenIds[i]);
                if (child.ParentId != node.PageId) return false; // Parent pointer mismatch

                IndexKey? childMin = (i == 0) ? minKey : node.Keys[i - 1];
                IndexKey? childMax = (i == node.KeyCount) ? maxKey : node.Keys[i];

                if (!ValidateNode(child, childMin, childMax)) return false;
            }
        }

        return true;
    }
}
