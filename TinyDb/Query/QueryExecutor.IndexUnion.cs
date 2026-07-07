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
    private IEnumerable<T> ExecuteIndexUnion<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan)
        where T : class
    {
        if (executionPlan.BranchPlans.Count == 0)
        {
            foreach (var item in ExecuteFullTableScanFallback<T>(executionPlan)) yield return item;
            yield break;
        }

        var queryExpression = GetPlanQueryExpression<T>(executionPlan);
        var yieldedIds = new HashSet<BsonValue>(BsonValueComparer.EqualityComparer);
        var tx = _engine.GetCurrentTransaction();
        var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, executionPlan.CollectionName) : null;
        var committedIds = new List<BsonValue>(CommittedLookupBatchSize);

        foreach (var branchPlan in executionPlan.BranchPlans)
        {
            if (!CanEnumeratePlanDocumentIds(branchPlan))
            {
                foreach (var item in ExecutePlanned<T>(branchPlan, branchPlan.CollectionName, fallbackExpression: null))
                {
                    var itemId = AotIdAccessor<T>.GetId(item);
                    if (!yieldedIds.Add(itemId)) continue;
                    if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, item)) continue;

                    yield return item;
                }

                continue;
            }

            foreach (var id in EnumeratePlanDocumentIds(branchPlan))
            {
                if (!yieldedIds.Add(id)) continue;

                if (txOverlay != null && txOverlay.TryGetValue(id, out var txDoc))
                {
                    foreach (var item in ResolveCommittedBatch<T>(executionPlan.CollectionName, committedIds, queryExpression))
                    {
                        yield return item;
                    }

                    committedIds.Clear();
                    txOverlay.Remove(id);
                    if (txDoc == null) continue;
                    if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, txDoc)) continue;

                    var txEntity = AotBsonMapper.FromDocument<T>(txDoc);
                    if (txEntity != null) yield return txEntity;
                    continue;
                }

                committedIds.Add(id);
                if (committedIds.Count >= CommittedLookupBatchSize)
                {
                    foreach (var item in ResolveCommittedBatch<T>(executionPlan.CollectionName, committedIds, queryExpression))
                    {
                        yield return item;
                    }

                    committedIds.Clear();
                }
            }
        }

        foreach (var item in ResolveCommittedBatch<T>(executionPlan.CollectionName, committedIds, queryExpression))
        {
            yield return item;
        }

        if (txOverlay != null)
        {
            foreach (var (id, doc) in txOverlay)
            {
                if (!yieldedIds.Add(id)) continue;
                if (doc == null) continue;
                if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private async IAsyncEnumerable<T> ExecuteIndexUnionAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class
    {
        if (executionPlan.BranchPlans.Count == 0)
        {
            await foreach (var item in ExecuteFullTableScanAsync<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression, cancellationToken).ConfigureAwait(false))
            {
                yield return item;
            }
            yield break;
        }

        var queryExpression = GetPlanQueryExpression<T>(executionPlan);
        var yieldedIds = new HashSet<BsonValue>(BsonValueComparer.EqualityComparer);
        var tx = _engine.GetCurrentTransaction();
        var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, executionPlan.CollectionName) : null;
        var committedIds = new List<BsonValue>(CommittedLookupBatchSize);

        foreach (var branchPlan in executionPlan.BranchPlans)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!CanEnumeratePlanDocumentIds(branchPlan))
            {
                await foreach (var item in ExecutePlannedAsync<T>(branchPlan, branchPlan.CollectionName, fallbackExpression: null, cancellationToken).ConfigureAwait(false))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var itemId = AotIdAccessor<T>.GetId(item);
                    if (!yieldedIds.Add(itemId)) continue;
                    if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, item)) continue;

                    yield return item;
                }

                continue;
            }

            await foreach (var id in EnumeratePlanDocumentIdsAsync(branchPlan, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!yieldedIds.Add(id)) continue;

                if (txOverlay != null && txOverlay.TryGetValue(id, out var txDoc))
                {
                    var batch = await ResolveCommittedBatchAsync<T>(executionPlan.CollectionName, committedIds, queryExpression, cancellationToken).ConfigureAwait(false);
                    committedIds.Clear();
                    foreach (var item in batch)
                    {
                        yield return item;
                    }

                    txOverlay.Remove(id);
                    if (txDoc == null) continue;
                    if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, txDoc)) continue;

                    var txEntity = AotBsonMapper.FromDocument<T>(txDoc);
                    if (txEntity != null) yield return txEntity;
                    continue;
                }

                committedIds.Add(id);
                if (committedIds.Count >= CommittedLookupBatchSize)
                {
                    var batch = await ResolveCommittedBatchAsync<T>(executionPlan.CollectionName, committedIds, queryExpression, cancellationToken).ConfigureAwait(false);
                    committedIds.Clear();
                    foreach (var item in batch)
                    {
                        yield return item;
                    }
                }
            }
        }

        if (committedIds.Count > 0)
        {
            var batch = await ResolveCommittedBatchAsync<T>(executionPlan.CollectionName, committedIds, queryExpression, cancellationToken).ConfigureAwait(false);
            committedIds.Clear();
            foreach (var item in batch)
            {
                yield return item;
            }
        }

        if (txOverlay != null)
        {
            foreach (var (id, doc) in txOverlay)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!yieldedIds.Add(id)) continue;
                if (doc == null) continue;
                if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private bool CanEnumeratePlanDocumentIds(QueryExecutionPlan executionPlan)
    {
        if (executionPlan.Strategy == QueryExecutionStrategy.PrimaryKeyLookup)
        {
            return executionPlan.IndexScanKeys.Count > 0;
        }

        if (executionPlan.Strategy is not (QueryExecutionStrategy.IndexSeek or QueryExecutionStrategy.IndexScan) ||
            executionPlan.UseIndex == null)
        {
            return false;
        }

        if (executionPlan.Strategy == QueryExecutionStrategy.IndexSeek &&
            BuildExactIndexKey(executionPlan) == null)
        {
            return false;
        }

        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        return idxMgr?.GetIndex(executionPlan.UseIndex.Name) != null;
    }

    private IEnumerable<BsonValue> EnumeratePlanDocumentIds(QueryExecutionPlan executionPlan)
    {
        if (executionPlan.Strategy == QueryExecutionStrategy.PrimaryKeyLookup)
        {
            if (executionPlan.IndexScanKeys.Count > 0)
            {
                yield return executionPlan.IndexScanKeys[0].Value;
            }

            yield break;
        }

        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        if (idxMgr == null || executionPlan.UseIndex == null) yield break;

        var index = idxMgr.GetIndex(executionPlan.UseIndex.Name);
        if (index == null) yield break;

        if (executionPlan.Strategy == QueryExecutionStrategy.IndexSeek)
        {
            var key = BuildExactIndexKey(executionPlan);
            if (key == null) yield break;

            if (index.IsUnique)
            {
                var id = index.FindExact(key);
                if (id != null) yield return id;
                yield break;
            }

            foreach (var id in index.Find(key))
            {
                yield return id;
            }

            yield break;
        }

        if (executionPlan.Strategy == QueryExecutionStrategy.IndexScan)
        {
            var range = BuildIndexScanRange(executionPlan);
            foreach (var id in index.FindRange(range.MinKey, range.MaxKey, range.IncludeMin, range.IncludeMax))
            {
                yield return id;
            }
        }
    }

    private async IAsyncEnumerable<BsonValue> EnumeratePlanDocumentIdsAsync(
        QueryExecutionPlan executionPlan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (executionPlan.Strategy == QueryExecutionStrategy.PrimaryKeyLookup)
        {
            if (executionPlan.IndexScanKeys.Count > 0)
            {
                yield return executionPlan.IndexScanKeys[0].Value;
            }

            yield break;
        }

        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        if (idxMgr == null || executionPlan.UseIndex == null) yield break;

        var index = idxMgr.GetIndex(executionPlan.UseIndex.Name);
        if (index == null) yield break;

        if (executionPlan.Strategy == QueryExecutionStrategy.IndexSeek)
        {
            var key = BuildExactIndexKey(executionPlan);
            if (key == null) yield break;

            if (index.IsUnique)
            {
                var id = await index.FindExactAsync(key, cancellationToken).ConfigureAwait(false);
                if (id != null) yield return id;
                yield break;
            }

            await foreach (var id in index.FindAsync(key, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return id;
            }

            yield break;
        }

        if (executionPlan.Strategy == QueryExecutionStrategy.IndexScan)
        {
            var range = BuildIndexScanRange(executionPlan);
            await foreach (var id in index.FindRangeAsync(range.MinKey, range.MaxKey, range.IncludeMin, range.IncludeMax, cancellationToken).ConfigureAwait(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return id;
            }
        }
    }

    private bool TryCreatePredicateIndexBeforeOrderPlan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        [NotNullWhen(true)] out QueryExecutionPlan? predicatePlan)
        where T : class
    {
        if (shape.Predicate == null || shape.Sort.Count == 0 || shape.Take.HasValue)
        {
            predicatePlan = null;
            return false;
        }

        predicatePlan = _queryOptimizer.CreateExecutionPlan(
            collectionName,
            shape.Predicate,
            planningMetadataOnly: true);

        if (predicatePlan.Strategy == QueryExecutionStrategy.FullTableScan)
        {
            predicatePlan = null;
            return false;
        }

        if (predicatePlan.Strategy == QueryExecutionStrategy.PrimaryKeyLookup)
        {
            return true;
        }

        if (predicatePlan.UseIndex == null || predicatePlan.IndexScanKeys.Count == 0)
        {
            predicatePlan = null;
            return false;
        }

        if (IsSortPrefixOfIndex(shape.Sort, predicatePlan.UseIndex.Fields))
        {
            predicatePlan = null;
            return false;
        }

        return true;
    }

    private static bool IsSortPrefixOfIndex(IReadOnlyList<QuerySortField> sort, IReadOnlyList<string> indexFields)
    {
        if (sort.Count == 0 || indexFields.Count < sort.Count)
        {
            return false;
        }

        for (int i = 0; i < sort.Count; i++)
        {
            if (!string.Equals(sort[i].FieldName, indexFields[i], StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }
}
