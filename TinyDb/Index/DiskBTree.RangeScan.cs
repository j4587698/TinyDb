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
    /// 在键范围内查找值。
    /// </summary>
    /// <param name="startKey">开始键。</param>
    /// <param name="endKey">结束键。</param>
    /// <param name="includeStart">是否包含开始键。</param>
    /// <param name="includeEnd">是否包含结束键。</param>
    /// <returns>值的集合。</returns>
    public IEnumerable<BsonValue> FindRange(IndexKey startKey, IndexKey endKey, bool includeStart, bool includeEnd)
    {
        ThrowIfDisposed();
        var results = new List<BsonValue>();
        using var lease = BeginPageLease();
        var node = FindFirstCandidateLeafNode(startKey);

        while (node != null)
        {
            bool pastEnd = false;
            for (int i = 0; i < node.KeyCount; i++)
            {
                var key = node.Keys[i];
                int cmpStart = key.CompareTo(startKey);
                int cmpEnd = key.CompareTo(endKey);

                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    pastEnd = true;
                    break;
                }

                if (cmpStart > 0 || (cmpStart == 0 && includeStart))
                {
                    results.Add(node.Values[i]);
                }
            }

            if (pastEnd) break;
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
        }

        return results;
    }

    internal IndexScanBatch FindRangeBatch(
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
        var startIndex = 0;
        var waitingForContinuation = continuationKey != null;
        if (TryResumeFromLeafPosition(continuationPageId, continuationIndex, continuationKey, continuationValue, forward: true, out var resumedNode, out startIndex))
        {
            node = resumedNode;
            waitingForContinuation = false;
        }
        else
        {
            node = FindFirstCandidateLeafNode(continuationKey ?? startKey);
        }

        while (node != null)
        {
            if (startIndex >= node.KeyCount)
            {
                if (node.NextSiblingId == 0) break;
                node = LoadNode(node.NextSiblingId);
                startIndex = 0;
                continue;
            }

            bool pastEnd = false;
            for (int i = startIndex; i < node.KeyCount; i++)
            {
                var key = node.Keys[i];
                var value = node.Values[i];
                int cmpEnd = key.CompareTo(endKey);

                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    pastEnd = true;
                    break;
                }

                if (waitingForContinuation)
                {
                    var cmpContinuation = key.CompareTo(continuationKey!);
                    if (cmpContinuation < 0)
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

                int cmpStart = key.CompareTo(startKey);
                if (cmpStart > 0 || (cmpStart == 0 && includeStart))
                {
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
            }

            if (pastEnd) break;
            if (node.NextSiblingId == 0) break;
            node = LoadNode(node.NextSiblingId);
            startIndex = 0;
        }

        return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: false);
    }

    internal async Task<IndexScanBatch> FindRangeBatchAsync(
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
        var startIndex = 0;
        var waitingForContinuation = continuationKey != null;
        var resume = await TryResumeFromLeafPositionAsync(
            continuationPageId,
            continuationIndex,
            continuationKey,
            continuationValue,
            forward: true,
            cancellationToken).ConfigureAwait(false);

        if (resume.Node != null)
        {
            node = resume.Node;
            startIndex = resume.StartIndex;
            waitingForContinuation = false;
        }
        else
        {
            node = await FindFirstCandidateLeafNodeAsync(continuationKey ?? startKey, cancellationToken).ConfigureAwait(false);
        }

        while (node != null)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (startIndex >= node.KeyCount)
            {
                if (node.NextSiblingId == 0) break;
                node = await LoadNodeAsync(node.NextSiblingId, cancellationToken).ConfigureAwait(false);
                startIndex = 0;
                continue;
            }

            bool pastEnd = false;
            for (int i = startIndex; i < node.KeyCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var key = node.Keys[i];
                var value = node.Values[i];
                int cmpEnd = key.CompareTo(endKey);

                if (cmpEnd > 0 || (cmpEnd == 0 && !includeEnd))
                {
                    pastEnd = true;
                    break;
                }

                if (waitingForContinuation)
                {
                    var cmpContinuation = key.CompareTo(continuationKey!);
                    if (cmpContinuation < 0)
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

                int cmpStart = key.CompareTo(startKey);
                if (cmpStart > 0 || (cmpStart == 0 && includeStart))
                {
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
            }

            if (pastEnd) break;
            if (node.NextSiblingId == 0) break;
            node = await LoadNodeAsync(node.NextSiblingId, cancellationToken).ConfigureAwait(false);
            startIndex = 0;
        }

        return new IndexScanBatch(results, lastKey, lastValue, lastPageId, lastIndex, hasMore: false);
    }

    private bool TryResumeFromLeafPosition(
        uint pageId,
        int index,
        IndexKey? key,
        BsonValue? value,
        bool forward,
        [NotNullWhen(true)] out DiskBTreeNode? node,
        out int startIndex)
    {
        node = null;
        startIndex = forward ? 0 : -1;

        if (pageId == 0 || index < 0 || key == null || value == null)
        {
            return false;
        }

        try
        {
            var candidate = LoadNode(pageId);
            if (!candidate.IsLeaf ||
                index >= candidate.KeyCount ||
                candidate.Keys[index].CompareTo(key) != 0 ||
                !ValuesEqual(candidate.Values[index], value))
            {
                return false;
            }

            node = candidate;
            startIndex = forward ? index + 1 : index - 1;
            return true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            return false;
        }
    }

    private async Task<(DiskBTreeNode? Node, int StartIndex)> TryResumeFromLeafPositionAsync(
        uint pageId,
        int index,
        IndexKey? key,
        BsonValue? value,
        bool forward,
        CancellationToken cancellationToken)
    {
        var startIndex = forward ? 0 : -1;

        if (pageId == 0 || index < 0 || key == null || value == null)
        {
            return (null, startIndex);
        }

        try
        {
            var candidate = await LoadNodeAsync(pageId, cancellationToken).ConfigureAwait(false);
            if (!candidate.IsLeaf ||
                index >= candidate.KeyCount ||
                candidate.Keys[index].CompareTo(key) != 0 ||
                !ValuesEqual(candidate.Values[index], value))
            {
                return (null, startIndex);
            }

            return (candidate, forward ? index + 1 : index - 1);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not OperationCanceledException)
        {
            return (null, startIndex);
        }
    }
}
