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
    private static async IAsyncEnumerable<T> AsyncEmpty<T>()
    {
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }

    private static async IAsyncEnumerable<T> ApplySkipTakeAsync<T>(
        IAsyncEnumerable<T> source,
        int skip,
        int? take,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var remainingSkip = Math.Max(skip, 0);
        var remainingTake = take;

        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (remainingSkip > 0)
            {
                remainingSkip--;
                continue;
            }

            if (remainingTake is int t)
            {
                if (t <= 0) yield break;
                remainingTake = t - 1;
            }

            yield return item;
        }
    }

    private static async IAsyncEnumerable<T> OrderByIdAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        IAsyncEnumerable<T> source,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class
    {
        var items = new List<T>();
        await foreach (var item in source.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            items.Add(item);
        }

        items.Sort(static (a, b) => BsonValueSortComparer.Instance.Compare(AotIdAccessor<T>.GetId(a), AotIdAccessor<T>.GetId(b)));
        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return item;
        }
    }

    private QueryExpression? GetPlanQueryExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan)
        where T : class
    {
        if (executionPlan.QueryExpression != null) return executionPlan.QueryExpression;
        return executionPlan.OriginalExpression != null
            ? _expressionParser.Parse<T>((Expression<Func<T, bool>>)executionPlan.OriginalExpression)
            : null;
    }

    private IEnumerable<T> ExecuteFullTableScanFallback<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan)
        where T : class
    {
        return executionPlan.QueryExpression != null
            ? ExecuteFullTableScanQuery<T>(executionPlan.CollectionName, executionPlan.QueryExpression)
            : ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression);
    }

    private async IAsyncEnumerable<T> ExecutePrimaryKeyLookupAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (executionPlan.IndexScanKeys.Count == 0) yield break;

        var id = executionPlan.IndexScanKeys[0].Value;
        var tx = _engine.GetCurrentTransaction();
        if (tx != null && QueryTransactionOverlay.TryGetDocument(tx, executionPlan.CollectionName, id, out var txDoc))
        {
            if (txDoc != null)
            {
                bool match = executionPlan.QueryExpression == null || ExpressionEvaluator.Evaluate(executionPlan.QueryExpression, txDoc);
                if (match)
                {
                    var entity = AotBsonMapper.FromDocument<T>(txDoc);
                    if (entity != null) yield return entity;
                }
            }
            yield break;
        }

        var doc = await _engine.FindByIdAsync(executionPlan.CollectionName, id, cancellationToken).ConfigureAwait(false);
        if (doc != null)
        {
            bool match = executionPlan.QueryExpression == null || ExpressionEvaluator.Evaluate(executionPlan.QueryExpression, doc);
            if (match)
            {
                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private IEnumerable<T> ExecutePrimaryKeyLookup<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class
    {
        if (executionPlan.IndexScanKeys.Count == 0) yield break;

        var id = executionPlan.IndexScanKeys[0].Value;
        var tx = _engine.GetCurrentTransaction();
        if (tx != null && QueryTransactionOverlay.TryGetDocument(tx, executionPlan.CollectionName, id, out var txDoc))
        {
            if (txDoc != null)
            {
                bool match = executionPlan.QueryExpression == null || ExpressionEvaluator.Evaluate(executionPlan.QueryExpression, txDoc);
                if (match)
                {
                    var entity = AotBsonMapper.FromDocument<T>(txDoc);
                    if (entity != null) yield return entity;
                }
            }
            yield break;
        }

        var doc = _engine.FindById(executionPlan.CollectionName, id);
        if (doc != null)
        {
            bool match = executionPlan.QueryExpression == null || ExpressionEvaluator.Evaluate(executionPlan.QueryExpression, doc);
            if (match)
            {
                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private IEnumerable<T> ResolveCommittedBatch<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        List<BsonValue> ids,
        QueryExpression? queryExpression)
        where T : class
    {
        if (ids.Count == 0) yield break;

        var documents = _engine.FindCommittedByIds(collectionName, ids);
        foreach (var doc in documents)
        {
            if (doc == null) continue;
            if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, doc)) continue;

            var entity = AotBsonMapper.FromDocument<T>(doc);
            if (entity != null) yield return entity;
        }
    }

    private async Task<List<T>> ResolveCommittedBatchAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        List<BsonValue> ids,
        QueryExpression? queryExpression,
        CancellationToken cancellationToken)
        where T : class
    {
        var results = new List<T>(ids.Count);
        if (ids.Count == 0) return results;

        var documents = await _engine.FindCommittedByIdsAsync(collectionName, ids, cancellationToken).ConfigureAwait(false);
        foreach (var doc in documents)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (doc == null) continue;
            if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, doc)) continue;

            var entity = AotBsonMapper.FromDocument<T>(doc);
            if (entity != null) results.Add(entity);
        }

        return results;
    }
}
