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
    /// 在键范围内查找值（按降序返回）。
    /// </summary>
    /// <param name="startKey">开始键。</param>
    /// <param name="endKey">结束键。</param>
    /// <param name="includeStart">是否包含开始键。</param>
    /// <param name="includeEnd">是否包含结束键。</param>
    /// <returns>值的集合（降序）。</returns>
    public IEnumerable<BsonValue> FindRangeReverse(IndexKey startKey, IndexKey endKey, bool includeStart, bool includeEnd)
    {
        ThrowIfDisposed();
        var results = new List<BsonValue>();
        using var lease = BeginPageLease();
        var node = FindLastCandidateLeafNode(endKey);

        while (node != null)
        {
            bool pastStart = false;

            for (int i = node.KeyCount - 1; i >= 0; i--)
            {
                var key = node.Keys[i];
                int cmpStart = key.CompareTo(startKey);
                int cmpEnd = key.CompareTo(endKey);

                if (cmpStart < 0 || (cmpStart == 0 && !includeStart))
                {
                    pastStart = true;
                    break;
                }

                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    continue;
                }

                results.Add(node.Values[i]);
            }

            if (pastStart) break;
            if (node.PrevSiblingId == 0) break;
            node = LoadNode(node.PrevSiblingId);
        }

        return results;
    }

    internal IndexScanBatch FindRangeReverseBatch(
        IndexKey startKey,
        IndexKey endKey,
        bool includeStart,
        bool includeEnd,
        IndexKey? continuationKey,
        BsonValue? continuationValue,
        uint continuationPageId,
        int continuationIndex,
        int batchSize)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));

        ThrowIfDisposed();
        var results = new List<BsonValue>(batchSize);
        IndexKey? lastKey = null;
        BsonValue? lastValue = null;
        uint lastPageId = 0;
        int lastIndex = -1;

        using var lease = BeginPageLease();
        DiskBTreeNode node;
        var startIndex = -1;
        var waitingForContinuation = continuationKey != null;
        if (TryResumeFromLeafPosition(continuationPageId, continuationIndex, continuationKey, continuationValue, forward: false, out var resumedNode, out startIndex))
        {
            node = resumedNode;
            waitingForContinuation = false;
        }
        else
        {
            node = continuationKey != null
                ? FindLastCandidateLeafNode(continuationKey)
                : FindLastCandidateLeafNode(endKey);
            startIndex = node.KeyCount - 1;
        }

        while (node != null)
        {
            if (startIndex < 0)
            {
                if (node.PrevSiblingId == 0) break;
                node = LoadNode(node.PrevSiblingId);
                startIndex = node.KeyCount - 1;
                continue;
            }

            bool pastStart = false;

            for (int i = startIndex; i >= 0; i--)
            {
                var key = node.Keys[i];
                var value = node.Values[i];
                int cmpStart = key.CompareTo(startKey);

                if (cmpStart < 0 || (cmpStart == 0 && !includeStart))
                {
                    pastStart = true;
                    break;
                }

                if (waitingForContinuation)
                {
                    var cmpContinuation = key.CompareTo(continuationKey!);
                    if (cmpContinuation > 0)
                    {
                        continue;
                    }

                    if (cmpContinuation == 0)
                    {
                        if (continuationValue != null && ValuesEqual(value, continuationValue))
                        {
                            waitingForContinuation = false;
                        }

                        continue;
                    }

                    waitingForContinuation = false;
                }

                int cmpEnd = key.CompareTo(endKey);
                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    continue;
                }

                results.Add(value);
                lastKey = key;
                lastValue = value;
                lastPageId = node.PageId;
                lastIndex = i;

                if (results.Count >= batchSize)
                {
                    return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: true);
                }
            }

            if (pastStart) break;
            if (node.PrevSiblingId == 0) break;
            node = LoadNode(node.PrevSiblingId);
            startIndex = node.KeyCount - 1;
        }

        return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: false);
    }

    internal async Task<IndexScanBatch> FindRangeReverseBatchAsync(
        IndexKey startKey,
        IndexKey endKey,
        bool includeStart,
        bool includeEnd,
        IndexKey? continuationKey,
        BsonValue? continuationValue,
        uint continuationPageId,
        int continuationIndex,
        int batchSize,
        CancellationToken cancellationToken = default)
    {
        if (batchSize <= 0) throw new ArgumentOutOfRangeException(nameof(batchSize));

        ThrowIfDisposed();
        cancellationToken.ThrowIfCancellationRequested();

        var results = new List<BsonValue>(batchSize);
        IndexKey? lastKey = null;
        BsonValue? lastValue = null;
        uint lastPageId = 0;
        int lastIndex = -1;

        DiskBTreeNode node;
        var startIndex = -1;
        var waitingForContinuation = continuationKey != null;
        var resume = await TryResumeFromLeafPositionAsync(
            continuationPageId,
            continuationIndex,
            continuationKey,
            continuationValue,
            forward: false,
            cancellationToken).ConfigureAwait(false);

        if (resume.Node != null)
        {
            node = resume.Node;
            startIndex = resume.StartIndex;
            waitingForContinuation = false;
        }
        else
        {
            node = continuationKey != null
                ? await FindLastCandidateLeafNodeAsync(continuationKey, cancellationToken).ConfigureAwait(false)
                : await FindLastCandidateLeafNodeAsync(endKey, cancellationToken).ConfigureAwait(false);
            startIndex = node.KeyCount - 1;
        }

        while (node != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (startIndex < 0)
            {
                if (node.PrevSiblingId == 0) break;
                node = await LoadNodeAsync(node.PrevSiblingId, cancellationToken).ConfigureAwait(false);
                startIndex = node.KeyCount - 1;
                continue;
            }

            bool pastStart = false;

            for (int i = startIndex; i >= 0; i--)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var key = node.Keys[i];
                var value = node.Values[i];
                int cmpStart = key.CompareTo(startKey);

                if (cmpStart < 0 || (cmpStart == 0 && !includeStart))
                {
                    pastStart = true;
                    break;
                }

                if (waitingForContinuation)
                {
                    var cmpContinuation = key.CompareTo(continuationKey!);
                    if (cmpContinuation > 0)
                    {
                        continue;
                    }

                    if (cmpContinuation == 0)
                    {
                        if (continuationValue != null && ValuesEqual(value, continuationValue))
                        {
                            waitingForContinuation = false;
                        }

                        continue;
                    }

                    waitingForContinuation = false;
                }

                int cmpEnd = key.CompareTo(endKey);
                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    continue;
                }

                results.Add(value);
                lastKey = key;
                lastValue = value;
                lastPageId = node.PageId;
                lastIndex = i;

                if (results.Count >= batchSize)
                {
                    return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: true);
                }
            }

            if (pastStart) break;
            if (node.PrevSiblingId == 0) break;
            node = await LoadNodeAsync(node.PrevSiblingId, cancellationToken).ConfigureAwait(false);
            startIndex = node.KeyCount - 1;
        }

        return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: false);
    }
}
