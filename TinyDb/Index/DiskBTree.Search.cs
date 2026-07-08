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
    /// 查找与键关联的所有值。
    /// </summary>
    /// <param name="key">键。</param>
    /// <returns>值列表。</returns>
    public List<BsonValue> Find(IndexKey key)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(key);

        var res = new List<BsonValue>();
        bool passed = false;

        while (node != null)
        {
            for(int i=0; i<node.KeyCount; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0)
                {
                    res.Add(node.Values[i]);
                }
                else if (cmp > 0)
                {
                    passed = true;
                    break;
                }
            }

            if (passed) break;
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }
        return res;
    }

    /// <summary>
    /// 查找键的单个值（第一个匹配项）。
    /// 优化版本：直接在叶子节点中查找，无需创建 List。
    /// </summary>
    /// <param name="key">键。</param>
    /// <returns>值，如果未找到则为 null。</returns>
    public BsonValue? FindExact(IndexKey key)
    {
        ThrowIfDisposed();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(key);

        // 查找精确匹配的第一个值
        bool passed = false;
        for (int i = 0; i < node.KeyCount; i++)
        {
            int cmp = node.Keys[i].CompareTo(key);
            if (cmp == 0)
            {
                return node.Values[i];
            }
            if (cmp > 0)
            {
                passed = true;
                break; // 已经超过目标键，不可能找到了
            }
        }
        if (passed) return null;

        while (node.NextSiblingId != 0)
        {
            node = LoadNode(node.NextSiblingId);
            for (int i = 0; i < node.KeyCount; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0)
                {
                    return node.Values[i];
                }

                if (cmp > 0)
                {
                    return null;
                }
            }
        }

        return null;
    }

    internal async Task<BsonValue?> FindExactAsync(IndexKey key, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        using var lease = BeginPageLease();
        var node = await FindFirstCandidateLeafNodeAsync(key, cancellationToken).ConfigureAwait(false);

        bool passed = false;
        for (int i = 0; i < node.KeyCount; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            int cmp = node.Keys[i].CompareTo(key);
            if (cmp == 0)
            {
                return node.Values[i];
            }
            if (cmp > 0)
            {
                passed = true;
                break;
            }
        }
        if (passed) return null;

        while (node.NextSiblingId != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            node = await LoadNodeAsync(node.NextSiblingId, cancellationToken).ConfigureAwait(false);
            for (int i = 0; i < node.KeyCount; i++)
            {
                int cmp = node.Keys[i].CompareTo(key);
                if (cmp == 0)
                {
                    return node.Values[i];
                }

                if (cmp > 0)
                {
                    return null;
                }
            }
        }

        return null;
    }














}
