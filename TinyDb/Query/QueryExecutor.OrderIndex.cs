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
    private bool TryGetOrderIndex(string collectionName, IReadOnlyList<QuerySortField> sort, [NotNullWhen(true)] out BTreeIndex? index, out bool allDescending)
    {
        index = null;
        allDescending = false;

        if (sort.Count == 0) return false;

        allDescending = sort[0].Descending;
        for (int i = 1; i < sort.Count; i++)
        {
            if (sort[i].Descending != allDescending) return false;
        }

        var idxMgr = _engine.GetIndexManager(collectionName);
        if (idxMgr == null) return false;

        var desiredFields = new string[sort.Count];
        for (int i = 0; i < sort.Count; i++)
        {
            desiredFields[i] = sort[i].FieldName;
        }

        IndexStatistics? best = null;
        foreach (var stat in idxMgr.GetPlanningStatistics())
        {
            if (stat.Type != IndexType.BTree) continue;
            if (stat.IsSparse) continue;
            if (stat.Fields.Length < desiredFields.Length) continue;

            bool match = true;
            for (int i = 0; i < desiredFields.Length; i++)
            {
                if (!string.Equals(stat.Fields[i], desiredFields[i], StringComparison.Ordinal))
                {
                    match = false;
                    break;
                }
            }

            if (!match) continue;

            if (best == null || stat.Fields.Length < best.Fields.Length)
            {
                best = stat;
            }
        }

        if (best == null) return false;

        var idx = idxMgr.GetIndex(best.Name);
        if (idx == null) return false;

        index = idx;
        return true;
    }

    internal IEnumerable<T> ExecuteByOrderIndex<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        BTreeIndex orderIndex,
        bool descending,
        out QueryPushdownInfo pushdown)
        where T : class
    {
        var skipRemaining = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var takeRemaining = shape.Take;

        var qe = ParsePredicate(shape.Predicate);

        pushdown = CreatePushdownInfo(
            shape,
            orderPushedCount: shape.Sort.Count,
            skipPushedCount: shape.Skip.GetValueOrDefault() > 0 ? 1 : 0,
            takePushedCount: shape.Take != null ? 1 : 0);

        if (takeRemaining is int initialTake && initialTake <= 0)
        {
            return Enumerable.Empty<T>();
        }

        return Iterator();

        IEnumerable<T> Iterator()
        {
            var ids = descending ? orderIndex.GetAllReverse() : orderIndex.GetAll();
            var remainingSkip = skipRemaining;
            var remainingTake = takeRemaining;

            foreach (var id in ids)
            {
                var doc = _engine.FindById(collectionName, id);
                if (doc == null) continue;

                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity == null) continue;

                if (remainingSkip > 0)
                {
                    remainingSkip--;
                    continue;
                }

                yield return entity;

                if (remainingTake is int take)
                {
                    take--;
                    if (take <= 0) yield break;
                    remainingTake = take;
                }
            }
        }
    }

    internal IEnumerable<T> ExecuteByOrderIndexWithTransaction<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        BTreeIndex orderIndex,
        bool descending,
        Transaction tx,
        out QueryPushdownInfo pushdown)
        where T : class
    {
        if (tx == null) throw new ArgumentNullException(nameof(tx));

        var skipRemaining = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var takeRemaining = shape.Take;

        var qe = ParsePredicate(shape.Predicate);

        pushdown = CreatePushdownInfo(
            shape,
            orderPushedCount: shape.Sort.Count,
            skipPushedCount: shape.Skip.GetValueOrDefault() > 0 ? 1 : 0,
            takePushedCount: shape.Take != null ? 1 : 0);

        if (takeRemaining is int initialTake && initialTake <= 0)
        {
            return Enumerable.Empty<T>();
        }

        var txOverlay = QueryTransactionOverlay.Build(tx, collectionName);
        var txRows = OrderIndexTransactionRows.FromOverlay(txOverlay, orderIndex, qe, descending);

        return Iterator();

        IEnumerable<T> Iterator()
        {
            var ids = descending ? orderIndex.GetAllReverse() : orderIndex.GetAll();
            var remainingSkip = skipRemaining;
            var remainingTake = takeRemaining;
            int txIndex = 0;

            foreach (var id in ids)
            {
                if (txOverlay != null && txOverlay.ContainsKey(id))
                {
                    continue;
                }

                var doc = _engine.FindById(collectionName, id);
                if (doc == null) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var baseKey = OrderIndexTransactionRows.BuildKey(orderIndex, doc);

                if (txRows != null)
                {
                    while (txIndex < txRows.Count && OrderIndexTransactionRows.CompareToBase(txRows[txIndex], baseKey, id, descending) < 0)
                    {
                        var txDoc = txRows[txIndex].Document;
                        txIndex++;

                        var entity = AotBsonMapper.FromDocument<T>(txDoc);
                        if (entity == null) continue;

                        if (remainingSkip > 0)
                        {
                            remainingSkip--;
                            continue;
                        }

                        yield return entity;

                        if (remainingTake is int take)
                        {
                            take--;
                            if (take <= 0) yield break;
                            remainingTake = take;
                        }
                    }
                }

                var baseEntity = AotBsonMapper.FromDocument<T>(doc);
                if (baseEntity == null) continue;

                if (remainingSkip > 0)
                {
                    remainingSkip--;
                    continue;
                }

                yield return baseEntity;

                if (remainingTake is int baseTake)
                {
                    baseTake--;
                    if (baseTake <= 0) yield break;
                    remainingTake = baseTake;
                }
            }

            if (txRows != null)
            {
                while (txIndex < txRows.Count)
                {
                    var txDoc = txRows[txIndex].Document;
                    txIndex++;

                    var entity = AotBsonMapper.FromDocument<T>(txDoc);
                    if (entity == null) continue;

                    if (remainingSkip > 0)
                    {
                        remainingSkip--;
                        continue;
                    }

                    yield return entity;

                    if (remainingTake is int take)
                    {
                        take--;
                        if (take <= 0) yield break;
                        remainingTake = take;
                    }
                }
            }
        }
    }

    private IAsyncEnumerable<T> ExecuteByOrderIndexAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        BTreeIndex orderIndex,
        bool descending,
        out QueryPushdownInfo pushdown,
        CancellationToken cancellationToken)
        where T : class
    {
        var skipRemaining = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var takeRemaining = shape.Take;

        var qe = ParsePredicate(shape.Predicate);

        pushdown = CreatePushdownInfo(
            shape,
            orderPushedCount: shape.Sort.Count,
            skipPushedCount: shape.Skip.GetValueOrDefault() > 0 ? 1 : 0,
            takePushedCount: shape.Take != null ? 1 : 0);

        if (takeRemaining is int initialTake && initialTake <= 0)
        {
            return AsyncEmpty<T>();
        }

        return Iterator(skipRemaining, takeRemaining, qe, cancellationToken);

        async IAsyncEnumerable<T> Iterator(
            int remainingSkip,
            int? remainingTake,
            QueryExpression? queryExpression,
            [EnumeratorCancellation] CancellationToken ct)
        {
            var ids = descending ? orderIndex.GetAllReverseAsync(ct) : orderIndex.GetAllAsync(ct);
            await foreach (var id in ids.WithCancellation(ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();

                var doc = await _engine.FindByIdAsync(collectionName, id, ct).ConfigureAwait(false);
                if (doc == null) continue;

                if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity == null) continue;

                if (remainingSkip > 0)
                {
                    remainingSkip--;
                    continue;
                }

                yield return entity;

                if (remainingTake is int take)
                {
                    take--;
                    if (take <= 0) yield break;
                    remainingTake = take;
                }
            }
        }
    }

    private IAsyncEnumerable<T> ExecuteByOrderIndexWithTransactionAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        BTreeIndex orderIndex,
        bool descending,
        Transaction tx,
        out QueryPushdownInfo pushdown,
        CancellationToken cancellationToken)
        where T : class
    {
        if (tx == null) throw new ArgumentNullException(nameof(tx));

        var skipRemaining = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var takeRemaining = shape.Take;

        var qe = ParsePredicate(shape.Predicate);

        pushdown = CreatePushdownInfo(
            shape,
            orderPushedCount: shape.Sort.Count,
            skipPushedCount: shape.Skip.GetValueOrDefault() > 0 ? 1 : 0,
            takePushedCount: shape.Take != null ? 1 : 0);

        if (takeRemaining is int initialTake && initialTake <= 0)
        {
            return AsyncEmpty<T>();
        }

        var txOverlay = QueryTransactionOverlay.Build(tx, collectionName);
        var txRows = OrderIndexTransactionRows.FromOverlay(txOverlay, orderIndex, qe, descending);

        return Iterator(skipRemaining, takeRemaining, qe, txOverlay, txRows, cancellationToken);

        async IAsyncEnumerable<T> Iterator(
            int remainingSkip,
            int? remainingTake,
            QueryExpression? queryExpression,
            Dictionary<BsonValue, BsonDocument?>? overlay,
            List<OrderIndexRow>? rows,
            [EnumeratorCancellation] CancellationToken ct)
        {
            var ids = descending ? orderIndex.GetAllReverseAsync(ct) : orderIndex.GetAllAsync(ct);
            int txIndex = 0;

            (T? Item, bool Done) TryYieldRow(BsonDocument rowDocument)
            {
                var entity = AotBsonMapper.FromDocument<T>(rowDocument);
                if (entity == null) return (null, false);

                if (remainingSkip > 0)
                {
                    remainingSkip--;
                    return (null, false);
                }

                if (remainingTake is int take)
                {
                    take--;
                    remainingTake = take;
                    return (entity, take <= 0);
                }

                return (entity, false);
            }

            await foreach (var id in ids.WithCancellation(ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();

                if (overlay != null && overlay.ContainsKey(id))
                {
                    continue;
                }

                var doc = await _engine.FindByIdAsync(collectionName, id, ct).ConfigureAwait(false);
                if (doc == null) continue;
                if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, doc)) continue;

                var baseKey = OrderIndexTransactionRows.BuildKey(orderIndex, doc);

                if (rows != null)
                {
                    while (txIndex < rows.Count && OrderIndexTransactionRows.CompareToBase(rows[txIndex], baseKey, id, descending) < 0)
                    {
                        var yielded = TryYieldRow(rows[txIndex].Document);
                        txIndex++;
                        if (yielded.Item != null) yield return yielded.Item;
                        if (yielded.Done) yield break;
                    }
                }

                var baseYielded = TryYieldRow(doc);
                if (baseYielded.Item != null) yield return baseYielded.Item;
                if (baseYielded.Done) yield break;
            }

            if (rows != null)
            {
                while (txIndex < rows.Count)
                {
                    ct.ThrowIfCancellationRequested();

                    var yielded = TryYieldRow(rows[txIndex].Document);
                    txIndex++;
                    if (yielded.Item != null) yield return yielded.Item;
                    if (yielded.Done) yield break;
                }
            }
        }
    }

}
