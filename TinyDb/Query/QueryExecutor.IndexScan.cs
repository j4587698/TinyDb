using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Serialization;

namespace TinyDb.Query;

public sealed partial class QueryExecutor
{
    private async IAsyncEnumerable<T> ExecuteIndexScanAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class
    {
        cancellationToken.ThrowIfCancellationRequested();

        var tx = _engine.GetCurrentTransaction();
        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        if (idxMgr == null || executionPlan.UseIndex == null)
        {
            await foreach (var item in ExecuteFullTableScanAsync<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
            yield break;
        }

        var index = idxMgr.GetIndex(executionPlan.UseIndex.Name);
        if (index == null)
        {
            await foreach (var item in ExecuteFullTableScanAsync<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
            yield break;
        }

        var range = BuildIndexScanRange(executionPlan);

        var qe = GetPlanQueryExpression<T>(executionPlan);
        var committedPostFilter = BuildCommittedPostFilter(executionPlan, qe);

        var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, executionPlan.CollectionName) : null;
        var committedIds = new List<BsonValue>(CommittedLookupBatchSize);

        await foreach (var id in index.FindRangeAsync(range.MinKey, range.MaxKey, range.IncludeMin, range.IncludeMax, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (txOverlay != null && txOverlay.TryGetValue(id, out var txDoc))
            {
                if (committedIds.Count > 0)
                {
                    var batch = await ResolveCommittedBatchAsync<T>(executionPlan.CollectionName, committedIds, committedPostFilter, cancellationToken).ConfigureAwait(false);
                    committedIds.Clear();
                    foreach (var item in batch)
                    {
                        yield return item;
                    }
                }

                txOverlay.Remove(id);
                if (txDoc == null) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, txDoc)) continue;

                var txEntity = AotBsonMapper.FromDocument<T>(txDoc);
                if (txEntity != null) yield return txEntity;
                continue;
            }

            committedIds.Add(id);
            if (committedIds.Count >= CommittedLookupBatchSize)
            {
                var batch = await ResolveCommittedBatchAsync<T>(executionPlan.CollectionName, committedIds, committedPostFilter, cancellationToken).ConfigureAwait(false);
                committedIds.Clear();
                foreach (var item in batch)
                {
                    yield return item;
                }
            }
        }

        if (committedIds.Count > 0)
        {
            var batch = await ResolveCommittedBatchAsync<T>(executionPlan.CollectionName, committedIds, committedPostFilter, cancellationToken).ConfigureAwait(false);
            committedIds.Clear();
            foreach (var item in batch)
            {
                yield return item;
            }
        }

        if (txOverlay != null)
        {
            foreach (var doc in txOverlay.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (doc == null) continue;
                if (!TransactionDocumentMatchesIndexRange(index, doc, range)) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private IEnumerable<T> ExecuteIndexScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class
    {
        var tx = _engine.GetCurrentTransaction();
        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        if (idxMgr == null || executionPlan.UseIndex == null)
        {
            foreach (var item in ExecuteFullTableScanFallback<T>(executionPlan)) yield return item;
            yield break;
        }
        var index = idxMgr.GetIndex(executionPlan.UseIndex.Name);
        if (index == null)
        {
            foreach (var item in ExecuteFullTableScanFallback<T>(executionPlan)) yield return item;
            yield break;
        }

        var range = BuildIndexScanRange(executionPlan);
        var ids = index.FindRange(range.MinKey, range.MaxKey, range.IncludeMin, range.IncludeMax);

        var qe = GetPlanQueryExpression<T>(executionPlan);
        var committedPostFilter = BuildCommittedPostFilter(executionPlan, qe);

        var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, executionPlan.CollectionName) : null;
        var committedIds = new List<BsonValue>(CommittedLookupBatchSize);

        foreach (var id in ids)
        {
            if (txOverlay != null && txOverlay.TryGetValue(id, out var txDoc))
            {
                foreach (var item in ResolveCommittedBatch<T>(executionPlan.CollectionName, committedIds, committedPostFilter))
                {
                    yield return item;
                }
                committedIds.Clear();

                txOverlay.Remove(id);
                if (txDoc == null) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, txDoc)) continue;

                var txEntity = AotBsonMapper.FromDocument<T>(txDoc);
                if (txEntity != null) yield return txEntity;
                continue;
            }

            committedIds.Add(id);
            if (committedIds.Count >= CommittedLookupBatchSize)
            {
                foreach (var item in ResolveCommittedBatch<T>(executionPlan.CollectionName, committedIds, committedPostFilter))
                {
                    yield return item;
                }
                committedIds.Clear();
            }
        }

        foreach (var item in ResolveCommittedBatch<T>(executionPlan.CollectionName, committedIds, committedPostFilter))
        {
            yield return item;
        }

        if (txOverlay != null)
        {
            foreach (var doc in txOverlay.Values)
            {
                if (doc == null) continue;
                if (!TransactionDocumentMatchesIndexRange(index, doc, range)) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private async IAsyncEnumerable<T> ExecuteIndexSeekAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class
    {
        cancellationToken.ThrowIfCancellationRequested();

        var tx = _engine.GetCurrentTransaction();
        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        if (idxMgr == null || executionPlan.UseIndex == null)
        {
            await foreach (var item in ExecuteFullTableScanAsync<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
            yield break;
        }

        var index = idxMgr.GetIndex(executionPlan.UseIndex.Name);
        if (index == null)
        {
            await foreach (var item in ExecuteFullTableScanAsync<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
            yield break;
        }

        var key = BuildExactIndexKey(executionPlan);
        if (key == null)
        {
            await foreach (var item in ExecuteFullTableScanAsync<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
            yield break;
        }

        var qe = GetPlanQueryExpression<T>(executionPlan);
        var committedPostFilter = BuildCommittedPostFilter(executionPlan, qe);

        var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, executionPlan.CollectionName) : null;

        if (index.IsUnique)
        {
            var id = await index.FindExactAsync(key, cancellationToken).ConfigureAwait(false);
            if (id != null)
            {
                bool usedTxDoc = false;
                if (txOverlay != null && txOverlay.TryGetValue(id, out var txDoc))
                {
                    txOverlay.Remove(id);
                    usedTxDoc = true;
                    if (txDoc != null && (qe == null || ExpressionEvaluator.Evaluate(qe, txDoc)))
                    {
                        var txEntity = AotBsonMapper.FromDocument<T>(txDoc);
                        if (txEntity != null) yield return txEntity;
                    }
                }

                if (!usedTxDoc)
                {
                    var doc = await _engine.FindByIdAsync(executionPlan.CollectionName, id, cancellationToken).ConfigureAwait(false);
                    if (doc != null && (committedPostFilter == null || ExpressionEvaluator.Evaluate(committedPostFilter, doc)))
                    {
                        var entity = AotBsonMapper.FromDocument<T>(doc);
                        if (entity != null) yield return entity;
                    }
                }
            }
        }
        else
        {
            var committedIds = new List<BsonValue>(CommittedLookupBatchSize);
            await foreach (var id in index.FindAsync(key, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (txOverlay != null && txOverlay.TryGetValue(id, out var txDoc))
                {
                    if (committedIds.Count > 0)
                    {
                        var batch = await ResolveCommittedBatchAsync<T>(executionPlan.CollectionName, committedIds, committedPostFilter, cancellationToken).ConfigureAwait(false);
                        committedIds.Clear();
                        foreach (var item in batch)
                        {
                            yield return item;
                        }
                    }

                    txOverlay.Remove(id);
                    if (txDoc == null) continue;
                    if (qe != null && !ExpressionEvaluator.Evaluate(qe, txDoc)) continue;

                    var txEntity = AotBsonMapper.FromDocument<T>(txDoc);
                    if (txEntity != null) yield return txEntity;
                    continue;
                }

                committedIds.Add(id);
                if (committedIds.Count >= CommittedLookupBatchSize)
                {
                    var batch = await ResolveCommittedBatchAsync<T>(executionPlan.CollectionName, committedIds, committedPostFilter, cancellationToken).ConfigureAwait(false);
                    committedIds.Clear();
                    foreach (var item in batch)
                    {
                        yield return item;
                    }
                }
            }

            if (committedIds.Count > 0)
            {
                var batch = await ResolveCommittedBatchAsync<T>(executionPlan.CollectionName, committedIds, committedPostFilter, cancellationToken).ConfigureAwait(false);
                committedIds.Clear();
                foreach (var item in batch)
                {
                    yield return item;
                }
            }
        }

        if (txOverlay != null)
        {
            foreach (var doc in txOverlay.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (doc == null) continue;
                if (!TransactionDocumentMatchesExactIndexKey(index, doc, key)) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private IEnumerable<T> ExecuteIndexSeek<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class
    {
        var tx = _engine.GetCurrentTransaction();
        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        if (idxMgr == null || executionPlan.UseIndex == null)
        {
            foreach (var item in ExecuteFullTableScanFallback<T>(executionPlan)) yield return item;
            yield break;
        }
        var index = idxMgr.GetIndex(executionPlan.UseIndex.Name);
        if (index == null)
        {
            foreach (var item in ExecuteFullTableScanFallback<T>(executionPlan)) yield return item;
            yield break;
        }

        var key = BuildExactIndexKey(executionPlan);
        if (key == null)
        {
            foreach (var item in ExecuteFullTableScanFallback<T>(executionPlan)) yield return item;
            yield break;
        }

        var qe = GetPlanQueryExpression<T>(executionPlan);
        var committedPostFilter = BuildCommittedPostFilter(executionPlan, qe);

        var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, executionPlan.CollectionName) : null;

        if (index.IsUnique)
        {
            var id = index.FindExact(key);
            if (id != null)
            {
                bool usedTxDoc = false;
                if (txOverlay != null && txOverlay.TryGetValue(id, out var txDoc))
                {
                    txOverlay.Remove(id);
                    usedTxDoc = true;
                    if (txDoc != null && (qe == null || ExpressionEvaluator.Evaluate(qe, txDoc)))
                    {
                        var txEntity = AotBsonMapper.FromDocument<T>(txDoc);
                        if (txEntity != null) yield return txEntity;
                    }
                }

                if (!usedTxDoc)
                {
                    var doc = _engine.FindById(executionPlan.CollectionName, id);
                    if (doc != null && (committedPostFilter == null || ExpressionEvaluator.Evaluate(committedPostFilter, doc)))
                    {
                        var entity = AotBsonMapper.FromDocument<T>(doc);
                        if (entity != null) yield return entity;
                    }
                }
            }
        }
        else
        {
            var committedIds = new List<BsonValue>(CommittedLookupBatchSize);
            foreach (var id in index.Find(key))
            {
                if (txOverlay != null && txOverlay.TryGetValue(id, out var txDoc))
                {
                    foreach (var item in ResolveCommittedBatch<T>(executionPlan.CollectionName, committedIds, committedPostFilter))
                    {
                        yield return item;
                    }
                    committedIds.Clear();

                    txOverlay.Remove(id);
                    if (txDoc == null) continue;
                    if (qe != null && !ExpressionEvaluator.Evaluate(qe, txDoc)) continue;

                    var txEntity = AotBsonMapper.FromDocument<T>(txDoc);
                    if (txEntity != null) yield return txEntity;
                    continue;
                }

                committedIds.Add(id);
                if (committedIds.Count >= CommittedLookupBatchSize)
                {
                    foreach (var item in ResolveCommittedBatch<T>(executionPlan.CollectionName, committedIds, committedPostFilter))
                    {
                        yield return item;
                    }
                    committedIds.Clear();
                }
            }

            foreach (var item in ResolveCommittedBatch<T>(executionPlan.CollectionName, committedIds, committedPostFilter))
            {
                yield return item;
            }
        }

        if (txOverlay != null)
        {
            foreach (var doc in txOverlay.Values)
            {
                if (doc == null) continue;
                if (!TransactionDocumentMatchesExactIndexKey(index, doc, key)) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private static bool TransactionDocumentMatchesIndexRange(BTreeIndex index, BsonDocument document, IndexScanRange range)
    {
        var key = OrderIndexTransactionRows.BuildKey(index, document);
        var minCompare = key.CompareTo(range.MinKey);
        if (minCompare < 0 || (minCompare == 0 && !range.IncludeMin)) return false;

        var maxCompare = key.CompareTo(range.MaxKey);
        return maxCompare < 0 || (maxCompare == 0 && range.IncludeMax);
    }

    private static bool TransactionDocumentMatchesExactIndexKey(BTreeIndex index, BsonDocument document, IndexKey expectedKey)
    {
        var key = OrderIndexTransactionRows.BuildKey(index, document);
        return key.CompareTo(expectedKey) == 0;
    }
}
