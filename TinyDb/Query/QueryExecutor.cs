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

public sealed class QueryExecutor
{
    private readonly TinyDbEngine _engine;
    private readonly ExpressionParser _expressionParser;
    private readonly QueryOptimizer _queryOptimizer;
    private const int CommittedLookupBatchSize = 256;

    public QueryExecutor(TinyDbEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _expressionParser = new ExpressionParser();
        _queryOptimizer = new QueryOptimizer(engine);
    }

    private sealed class BsonValueSortComparer : IComparer<BsonValue>
    {
        public static readonly BsonValueSortComparer Instance = new();

        public int Compare(BsonValue? x, BsonValue? y)
        {
            return QueryValueComparer.Compare(x, y);
        }
    }

    public IEnumerable<T> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class, new()
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var executionPlan = _queryOptimizer.CreateExecutionPlan(collectionName, expression);

        return ExecutePlanned(executionPlan, collectionName, expression);
    }

    internal IEnumerable<T> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryExpression? queryExpression)
        where T : class, new()
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var executionPlan = _queryOptimizer.CreateExecutionPlan(collectionName, queryExpression);

        return executionPlan.Strategy switch
        {
            QueryExecutionStrategy.PrimaryKeyLookup => ExecutePrimaryKeyLookup<T>(executionPlan),
            QueryExecutionStrategy.IndexScan => ExecuteIndexScan<T>(executionPlan),
            QueryExecutionStrategy.IndexSeek => ExecuteIndexSeek<T>(executionPlan),
            _ => ExecuteFullTableScanQuery<T>(collectionName, queryExpression)
        };
    }

    public IAsyncEnumerable<T> ExecuteAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        Expression<Func<T, bool>>? expression = null,
        CancellationToken cancellationToken = default)
        where T : class, new()
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var executionPlan = _queryOptimizer.CreateExecutionPlan(collectionName, expression, planningMetadataOnly: true);

        return ExecutePlannedAsync(executionPlan, collectionName, expression, cancellationToken);
    }

    internal IEnumerable<T> ExecuteShaped<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        out QueryPushdownInfo pushdown)
        where T : class, new()
    {
        if (shape == null) throw new ArgumentNullException(nameof(shape));
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var skip = shape.Skip.GetValueOrDefault();
        var take = shape.Take;

        if (shape.Sort.Count == 0)
        {
            var result = Execute<T>(collectionName, shape.Predicate);
            if (skip > 0 || take.HasValue)
            {
                result = result.OrderBy(static item => AotIdAccessor<T>.GetId(item), BsonValueSortComparer.Instance);
            }

            bool skipPushed = false;
            bool takePushed = false;

            if (skip > 0)
            {
                result = result.Skip(skip);
                skipPushed = true;
            }

            if (take is int t)
            {
                if (t <= 0)
                {
                    pushdown = new QueryPushdownInfo
                    {
                        WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
                        SkipPushedCount = skipPushed ? 1 : 0,
                        TakePushedCount = 1,
                        OrderPushedCount = 0
                    };
                    return Enumerable.Empty<T>();
                }

                result = result.Take(t);
                takePushed = true;
            }

            pushdown = new QueryPushdownInfo
            {
                WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
                SkipPushedCount = skipPushed ? 1 : 0,
                TakePushedCount = takePushed ? 1 : 0,
                OrderPushedCount = 0
            };

            return result;
        }

        var tx = _engine.GetCurrentTransaction();

        if (TryCreatePredicateIndexBeforeOrderPlan(collectionName, shape, out var predicatePlan))
        {
            pushdown = new QueryPushdownInfo
            {
                WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0
            };
            return ExecutePlanned(predicatePlan, collectionName, shape.Predicate);
        }

        if (TryGetOrderIndex(collectionName, shape.Sort, out var orderIndex, out var allDescending))
        {
            if (tx == null)
            {
                return ExecuteByOrderIndex<T>(collectionName, shape, orderIndex, allDescending, out pushdown);
            }

            return ExecuteByOrderIndexWithTransaction<T>(collectionName, shape, orderIndex, allDescending, tx, out pushdown);
        }

        if (take is int takeCount)
        {
            if (takeCount <= 0)
            {
                pushdown = new QueryPushdownInfo
                {
                    WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
                    OrderPushedCount = shape.Sort.Count,
                    SkipPushedCount = skip > 0 ? 1 : 0,
                    TakePushedCount = 1
                };
                return Enumerable.Empty<T>();
            }

            return ExecuteTopKScan<T>(collectionName, shape, out pushdown);
        }

        pushdown = new QueryPushdownInfo { WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0 };
        return Execute<T>(collectionName, shape.Predicate);
    }

    internal IAsyncEnumerable<T> ExecuteShapedAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        out QueryPushdownInfo pushdown,
        CancellationToken cancellationToken = default)
        where T : class, new()
    {
        if (shape == null) throw new ArgumentNullException(nameof(shape));
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var skip = shape.Skip.GetValueOrDefault();
        var take = shape.Take;

        if (shape.Sort.Count == 0)
        {
            var result = ExecuteAsync<T>(collectionName, shape.Predicate, cancellationToken);
            if (skip > 0 || take.HasValue)
            {
                result = OrderByIdAsync(result, cancellationToken);
            }

            bool skipPushed = false;
            bool takePushed = false;

            if (skip > 0)
            {
                skipPushed = true;
            }

            if (take is int t)
            {
                if (t <= 0)
                {
                    pushdown = new QueryPushdownInfo
                    {
                        WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
                        SkipPushedCount = skipPushed ? 1 : 0,
                        TakePushedCount = 1,
                        OrderPushedCount = 0
                    };
                    return AsyncEmpty<T>();
                }

                takePushed = true;
            }

            pushdown = new QueryPushdownInfo
            {
                WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
                SkipPushedCount = skipPushed ? 1 : 0,
                TakePushedCount = takePushed ? 1 : 0,
                OrderPushedCount = 0
            };

            return ApplySkipTakeAsync(result, skip, take, cancellationToken);
        }

        var tx = _engine.GetCurrentTransaction();

        if (TryCreatePredicateIndexBeforeOrderPlan(collectionName, shape, out var predicatePlan))
        {
            pushdown = new QueryPushdownInfo
            {
                WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0
            };
            return ExecutePlannedAsync(predicatePlan, collectionName, shape.Predicate, cancellationToken);
        }

        if (TryGetOrderIndex(collectionName, shape.Sort, out var orderIndex, out var allDescending))
        {
            if (tx == null)
            {
                return ExecuteByOrderIndexAsync<T>(collectionName, shape, orderIndex, allDescending, out pushdown, cancellationToken);
            }

            return ExecuteByOrderIndexWithTransactionAsync<T>(collectionName, shape, orderIndex, allDescending, tx, out pushdown, cancellationToken);
        }

        if (take is int takeCount)
        {
            if (takeCount <= 0)
            {
                pushdown = new QueryPushdownInfo
                {
                    WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
                    OrderPushedCount = shape.Sort.Count,
                    SkipPushedCount = skip > 0 ? 1 : 0,
                    TakePushedCount = 1
                };
                return AsyncEmpty<T>();
            }

            return ExecuteTopKScanAsync<T>(collectionName, shape, out pushdown, cancellationToken);
        }

        pushdown = new QueryPushdownInfo { WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0 };
        return ExecuteAsync<T>(collectionName, shape.Predicate, cancellationToken);
    }

    // ... (省略 Index 相关方法，保持不变) ...
    internal IEnumerable<BsonDocument> ExecuteIndexScanForTests(QueryExecutionPlan executionPlan) => ExecuteIndexScan<BsonDocument>(executionPlan);
    internal IEnumerable<T> ExecutePrimaryKeyLookupForTests<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new() => ExecutePrimaryKeyLookup<T>(executionPlan);
    internal IEnumerable<T> ExecuteIndexSeekForTests<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new() => ExecuteIndexSeek<T>(executionPlan);

    private IEnumerable<T> ExecutePlanned<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        string collectionName,
        Expression<Func<T, bool>>? fallbackExpression)
        where T : class, new()
    {
        return executionPlan.Strategy switch
        {
            QueryExecutionStrategy.PrimaryKeyLookup => ExecutePrimaryKeyLookup<T>(executionPlan),
            QueryExecutionStrategy.IndexScan => ExecuteIndexScan<T>(executionPlan),
            QueryExecutionStrategy.IndexSeek => ExecuteIndexSeek<T>(executionPlan),
            _ => ExecuteFullTableScan(collectionName, fallbackExpression)
        };
    }

    private IAsyncEnumerable<T> ExecutePlannedAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        string collectionName,
        Expression<Func<T, bool>>? fallbackExpression,
        CancellationToken cancellationToken)
        where T : class, new()
    {
        return executionPlan.Strategy switch
        {
            QueryExecutionStrategy.PrimaryKeyLookup => ExecutePrimaryKeyLookupAsync<T>(executionPlan, cancellationToken),
            QueryExecutionStrategy.IndexScan => ExecuteIndexScanAsync<T>(executionPlan, cancellationToken),
            QueryExecutionStrategy.IndexSeek => ExecuteIndexSeekAsync<T>(executionPlan, cancellationToken),
            _ => ExecuteFullTableScanAsync(collectionName, fallbackExpression, cancellationToken)
        };
    }

    private bool TryCreatePredicateIndexBeforeOrderPlan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        string collectionName,
        QueryShape<T> shape,
        [NotNullWhen(true)] out QueryExecutionPlan? predicatePlan)
        where T : class, new()
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
        where T : class, new()
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
        where T : class, new()
    {
        if (executionPlan.QueryExpression != null) return executionPlan.QueryExpression;
        return executionPlan.OriginalExpression != null
            ? _expressionParser.Parse<T>((Expression<Func<T, bool>>)executionPlan.OriginalExpression)
            : null;
    }

    private IEnumerable<T> ExecuteFullTableScanFallback<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan)
        where T : class, new()
    {
        return executionPlan.QueryExpression != null
            ? ExecuteFullTableScanQuery<T>(executionPlan.CollectionName, executionPlan.QueryExpression)
            : ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression);
    }

    private async IAsyncEnumerable<T> ExecutePrimaryKeyLookupAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class, new()
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (executionPlan.IndexScanKeys.Count == 0) yield break;

        var id = executionPlan.IndexScanKeys[0].Value;
        var tx = _engine.GetCurrentTransaction();
        if (tx != null && TryGetTransactionDocument(tx, executionPlan.CollectionName, id, out var txDoc))
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

    private IEnumerable<T> ExecutePrimaryKeyLookup<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new()
    {
        if (executionPlan.IndexScanKeys.Count == 0) yield break;

        var id = executionPlan.IndexScanKeys[0].Value;
        var tx = _engine.GetCurrentTransaction();
        if (tx != null && TryGetTransactionDocument(tx, executionPlan.CollectionName, id, out var txDoc))
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
        where T : class, new()
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
        where T : class, new()
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

    private async IAsyncEnumerable<T> ExecuteIndexScanAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class, new()
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

        var txOverlay = tx != null ? BuildTransactionOverlay(tx, executionPlan.CollectionName) : null;
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
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private IEnumerable<T> ExecuteIndexScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new()
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

        var txOverlay = tx != null ? BuildTransactionOverlay(tx, executionPlan.CollectionName) : null;
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
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private async IAsyncEnumerable<T> ExecuteIndexSeekAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class, new()
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

        var txOverlay = tx != null ? BuildTransactionOverlay(tx, executionPlan.CollectionName) : null;

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
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private IEnumerable<T> ExecuteIndexSeek<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new()
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

        var txOverlay = tx != null ? BuildTransactionOverlay(tx, executionPlan.CollectionName) : null;

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
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }
        }
    }

    private IEnumerable<T> ExecuteFullTableScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class, new()
    {
        QueryExpression? queryExpression = null;

        if (expression != null)
        {
            try { queryExpression = _expressionParser.Parse(expression); }
            catch (Exception ex) { throw new NotSupportedException("Parse failed", ex); }
        }

        return ExecuteFullTableScanQuery<T>(collectionName, queryExpression);
    }

    private IEnumerable<T> ExecuteFullTableScanQuery<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryExpression? queryExpression)
        where T : class, new()
    {
        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;

        return Iterator();

        IEnumerable<T> Iterator()
        {
            Dictionary<string, BsonDocument?>? txOverlay = null;
            var tx = _engine.GetCurrentTransaction();
            if (tx != null)
            {
                txOverlay = new Dictionary<string, BsonDocument?>(StringComparer.Ordinal);
                foreach (var op in tx.GetOperationsSnapshot())
                {
                    if (op.CollectionName != collectionName) continue;
                    var key = op.DocumentId?.ToString() ?? string.Empty;
                    if (op.OperationType == TransactionOperationType.Delete) txOverlay[key] = null;
                    else if (op.NewDocument != null) txOverlay[key] = op.NewDocument;
                }

                if (txOverlay.Count == 0) txOverlay = null;
            }

            foreach (var result in _engine.FindAllRawWithPredicateInfo(collectionName, pushDownPredicates))
            {
                var doc = DeserializeDocumentOrThrow(result.Slice);
                if (doc.TryGetValue("_collection", out var c) && c.ToString() != collectionName)
                {
                    continue;
                }

                doc = _engine.ResolveLargeDocument(doc);
                var requiresPostFilter = result.RequiresPostFilter;

                if (txOverlay != null)
                {
                    var idKey = doc["_id"].ToString();
                    if (txOverlay.TryGetValue(idKey, out var txDoc))
                    {
                        txOverlay.Remove(idKey);
                        if (txDoc == null) continue;
                        doc = txDoc;
                        requiresPostFilter = true;
                    }
                }

                if (queryExpression != null)
                {
                    var matched = fullyPushed
                        ? (!requiresPostFilter || ExpressionEvaluator.Evaluate(queryExpression, doc))
                        : ExpressionEvaluator.Evaluate(queryExpression, doc);
                    if (!matched) continue;
                }

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }

            if (txOverlay != null)
            {
                foreach (var txDoc in txOverlay.Values)
                {
                    if (txDoc == null) continue;
                    if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, txDoc)) continue;

                    var entity = AotBsonMapper.FromDocument<T>(txDoc);
                    if (entity != null) yield return entity;
                }
            }
        }
    }

    private async IAsyncEnumerable<T> ExecuteFullTableScanAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        Expression<Func<T, bool>>? expression = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
        where T : class, new()
    {
        QueryExpression? queryExpression = null;

        if (expression != null)
        {
            try { queryExpression = _expressionParser.Parse(expression); }
            catch (Exception ex) { throw new NotSupportedException("Parse failed", ex); }
        }

        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;

        Dictionary<string, BsonDocument?>? txOverlay = null;
        var tx = _engine.GetCurrentTransaction();
        if (tx != null)
        {
            txOverlay = new Dictionary<string, BsonDocument?>(StringComparer.Ordinal);
            foreach (var op in tx.GetOperationsSnapshot())
            {
                if (op.CollectionName != collectionName) continue;
                var key = op.DocumentId?.ToString() ?? string.Empty;
                if (op.OperationType == TransactionOperationType.Delete) txOverlay[key] = null;
                else if (op.NewDocument != null) txOverlay[key] = op.NewDocument;
            }

            if (txOverlay.Count == 0) txOverlay = null;
        }

        await foreach (var result in _engine.FindAllRawWithPredicateInfoAsync(collectionName, pushDownPredicates, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var doc = DeserializeDocumentOrThrow(result.Slice);
            if (doc.TryGetValue("_collection", out var c) && c.ToString() != collectionName)
            {
                continue;
            }

            doc = await _engine.ResolveLargeDocumentAsync(doc, cancellationToken).ConfigureAwait(false);
            var requiresPostFilter = result.RequiresPostFilter;

            if (txOverlay != null)
            {
                var idKey = doc["_id"].ToString();
                if (txOverlay.TryGetValue(idKey, out var txDoc))
                {
                    txOverlay.Remove(idKey);
                    if (txDoc == null) continue;
                    doc = txDoc;
                    requiresPostFilter = true;
                }
            }

            if (queryExpression != null)
            {
                var matched = fullyPushed
                    ? (!requiresPostFilter || ExpressionEvaluator.Evaluate(queryExpression, doc))
                    : ExpressionEvaluator.Evaluate(queryExpression, doc);
                if (!matched) continue;
            }

            var entity = AotBsonMapper.FromDocument<T>(doc);
            if (entity != null) yield return entity;
        }

        if (txOverlay != null)
        {
            foreach (var txDoc in txOverlay.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (txDoc == null) continue;
                if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, txDoc)) continue;

                var entity = AotBsonMapper.FromDocument<T>(txDoc);
                if (entity != null) yield return entity;
            }
        }
    }

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

    private IEnumerable<T> ExecuteByOrderIndex<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        BTreeIndex orderIndex,
        bool descending,
        out QueryPushdownInfo pushdown)
        where T : class, new()
    {
        var skipRemaining = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var takeRemaining = shape.Take;

        QueryExpression? qe = null;
        if (shape.Predicate != null)
        {
            try { qe = _expressionParser.Parse(shape.Predicate); }
            catch (Exception ex) { throw new NotSupportedException("Parse failed", ex); }
        }

        pushdown = new QueryPushdownInfo
        {
            WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
            OrderPushedCount = shape.Sort.Count,
            SkipPushedCount = shape.Skip.GetValueOrDefault() > 0 ? 1 : 0,
            TakePushedCount = shape.Take != null ? 1 : 0
        };

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

    private IEnumerable<T> ExecuteByOrderIndexWithTransaction<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        BTreeIndex orderIndex,
        bool descending,
        Transaction tx,
        out QueryPushdownInfo pushdown)
        where T : class, new()
    {
        if (tx == null) throw new ArgumentNullException(nameof(tx));

        var skipRemaining = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var takeRemaining = shape.Take;

        QueryExpression? qe = null;
        if (shape.Predicate != null)
        {
            try { qe = _expressionParser.Parse(shape.Predicate); }
            catch (Exception ex) { throw new NotSupportedException("Parse failed", ex); }
        }

        pushdown = new QueryPushdownInfo
        {
            WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
            OrderPushedCount = shape.Sort.Count,
            SkipPushedCount = shape.Skip.GetValueOrDefault() > 0 ? 1 : 0,
            TakePushedCount = shape.Take != null ? 1 : 0
        };

        if (takeRemaining is int initialTake && initialTake <= 0)
        {
            return Enumerable.Empty<T>();
        }

        var txOverlay = new Dictionary<BsonValue, BsonDocument?>(BsonValueComparer.EqualityComparer);
        foreach (var op in tx.GetOperationsSnapshot())
        {
            if (op.CollectionName != collectionName) continue;
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;

            if (op.OperationType == TransactionOperationType.Delete)
            {
                txOverlay[op.DocumentId] = null;
            }
            else if (op.NewDocument != null)
            {
                txOverlay[op.DocumentId] = op.NewDocument;
            }
        }

        List<TxOrderRow>? txRows = null;
        if (txOverlay.Count > 0)
        {
            txRows = new List<TxOrderRow>(txOverlay.Count);
            foreach (var (id, doc) in txOverlay)
            {
                if (doc == null) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;
                txRows.Add(new TxOrderRow(id, BuildIndexKeyForOrder(orderIndex, doc), doc));
            }

            if (txRows.Count == 0)
            {
                txRows = null;
            }
            else
            {
                txRows.Sort((a, b) => CompareTxRows(a, b, descending));
            }
        }

        return Iterator();

        IEnumerable<T> Iterator()
        {
            var ids = descending ? orderIndex.GetAllReverse() : orderIndex.GetAll();
            var remainingSkip = skipRemaining;
            var remainingTake = takeRemaining;
            int txIndex = 0;

            foreach (var id in ids)
            {
                if (txOverlay.ContainsKey(id))
                {
                    continue;
                }

                var doc = _engine.FindById(collectionName, id);
                if (doc == null) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;

                var baseKey = BuildIndexKeyForOrder(orderIndex, doc);

                if (txRows != null)
                {
                    while (txIndex < txRows.Count && CompareTxRowToBase(txRows[txIndex], baseKey, id, descending) < 0)
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
        where T : class, new()
    {
        var skipRemaining = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var takeRemaining = shape.Take;

        QueryExpression? qe = null;
        if (shape.Predicate != null)
        {
            try { qe = _expressionParser.Parse(shape.Predicate); }
            catch (Exception ex) { throw new NotSupportedException("Parse failed", ex); }
        }

        pushdown = new QueryPushdownInfo
        {
            WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
            OrderPushedCount = shape.Sort.Count,
            SkipPushedCount = shape.Skip.GetValueOrDefault() > 0 ? 1 : 0,
            TakePushedCount = shape.Take != null ? 1 : 0
        };

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
        where T : class, new()
    {
        if (tx == null) throw new ArgumentNullException(nameof(tx));

        var skipRemaining = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var takeRemaining = shape.Take;

        QueryExpression? qe = null;
        if (shape.Predicate != null)
        {
            try { qe = _expressionParser.Parse(shape.Predicate); }
            catch (Exception ex) { throw new NotSupportedException("Parse failed", ex); }
        }

        pushdown = new QueryPushdownInfo
        {
            WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
            OrderPushedCount = shape.Sort.Count,
            SkipPushedCount = shape.Skip.GetValueOrDefault() > 0 ? 1 : 0,
            TakePushedCount = shape.Take != null ? 1 : 0
        };

        if (takeRemaining is int initialTake && initialTake <= 0)
        {
            return AsyncEmpty<T>();
        }

        var txOverlay = new Dictionary<BsonValue, BsonDocument?>(BsonValueComparer.EqualityComparer);
        foreach (var op in tx.GetOperationsSnapshot())
        {
            if (op.CollectionName != collectionName) continue;
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;

            if (op.OperationType == TransactionOperationType.Delete)
            {
                txOverlay[op.DocumentId] = null;
            }
            else if (op.NewDocument != null)
            {
                txOverlay[op.DocumentId] = op.NewDocument;
            }
        }

        List<TxOrderRow>? txRows = null;
        if (txOverlay.Count > 0)
        {
            txRows = new List<TxOrderRow>(txOverlay.Count);
            foreach (var (id, doc) in txOverlay)
            {
                if (doc == null) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, doc)) continue;
                txRows.Add(new TxOrderRow(id, BuildIndexKeyForOrder(orderIndex, doc), doc));
            }

            if (txRows.Count == 0)
            {
                txRows = null;
            }
            else
            {
                txRows.Sort((a, b) => CompareTxRows(a, b, descending));
            }
        }

        return Iterator(skipRemaining, takeRemaining, qe, txOverlay, txRows, cancellationToken);

        async IAsyncEnumerable<T> Iterator(
            int remainingSkip,
            int? remainingTake,
            QueryExpression? queryExpression,
            Dictionary<BsonValue, BsonDocument?> overlay,
            List<TxOrderRow>? rows,
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

                if (overlay.ContainsKey(id))
                {
                    continue;
                }

                var doc = await _engine.FindByIdAsync(collectionName, id, ct).ConfigureAwait(false);
                if (doc == null) continue;
                if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, doc)) continue;

                var baseKey = BuildIndexKeyForOrder(orderIndex, doc);

                if (rows != null)
                {
                    while (txIndex < rows.Count && CompareTxRowToBase(rows[txIndex], baseKey, id, descending) < 0)
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

    private IEnumerable<T> ExecuteTopKScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        out QueryPushdownInfo pushdown)
        where T : class, new()
    {
        var sort = shape.Sort;
        var skip = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var take = shape.Take ?? 0;

        var kLong = (long)skip + take;
        if (kLong <= 0)
        {
            pushdown = new QueryPushdownInfo
            {
                WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
                OrderPushedCount = shape.Sort.Count,
                SkipPushedCount = skip > 0 ? 1 : 0,
                TakePushedCount = 1
            };
            return Enumerable.Empty<T>();
        }

        if (kLong > int.MaxValue)
        {
            throw new NotSupportedException("Skip + Take is too large.");
        }

        var k = (int)kLong;

        QueryExpression? queryExpression = null;
        if (shape.Predicate != null)
        {
            try { queryExpression = _expressionParser.Parse(shape.Predicate); }
            catch (Exception ex) { throw new NotSupportedException("Parse failed", ex); }
        }

        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;

        pushdown = new QueryPushdownInfo
        {
            WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
            OrderPushedCount = shape.Sort.Count,
            SkipPushedCount = skip > 0 ? 1 : 0,
            TakePushedCount = 1
        };

        var sortFields = new SortFieldBytes[sort.Count];
        for (int i = 0; i < sort.Count; i++)
        {
            sortFields[i] = SortFieldBytes.Create(sort[i].FieldName);
        }

        var collectionFieldNameBytes = Encoding.UTF8.GetBytes("_collection");
        var collectionNameBytes = Encoding.UTF8.GetBytes(collectionName);
        var isLargeDocumentFieldNameBytes = Encoding.UTF8.GetBytes("_isLargeDocument");

        return Iterator();

        IEnumerable<T> Iterator()
        {
            var heap = new List<TopKRow>(Math.Min(k, 256));
            long sequence = 0;

            Dictionary<string, BsonDocument?>? txOverlay = null;
            var tx = _engine.GetCurrentTransaction();
            if (tx != null)
            {
                txOverlay = new Dictionary<string, BsonDocument?>(StringComparer.Ordinal);
                foreach (var op in tx.GetOperationsSnapshot())
                {
                    if (op.CollectionName != collectionName) continue;
                    var key = op.DocumentId?.ToString() ?? "";
                    if (op.OperationType == TransactionOperationType.Delete) txOverlay[key] = null;
                    else if (op.NewDocument != null) txOverlay[key] = op.NewDocument;
                }

                if (txOverlay.Count == 0) txOverlay = null;
            }

            foreach (var result in _engine.FindAllRawWithPredicateInfo(collectionName, pushDownPredicates))
            {
                var slice = result.Slice;
                var span = slice.Span;

                if (!MatchesCollection(span, collectionFieldNameBytes, collectionNameBytes))
                {
                    continue;
                }

                if (txOverlay != null)
                {
                    if (TryReadBsonValue(span, SortFieldBytes.Id, out var idValue))
                    {
                        var idKey = idValue.ToString();
                        if (txOverlay.TryGetValue(idKey, out var txDoc))
                        {
                            txOverlay.Remove(idKey);
                            if (txDoc == null) continue;
                            ConsiderDocument(txDoc, requiresPostFilter: true);
                            continue;
                        }
                    }
                }

                ConsiderSlice(slice, result.RequiresPostFilter);
            }

            if (txOverlay != null)
            {
                foreach (var doc in txOverlay.Values)
                {
                    if (doc == null) continue;
                    ConsiderDocument(doc, requiresPostFilter: true);
                }
            }

            heap.Sort((a, b) => CompareRows(a, b, sort));

            var start = Math.Min(skip, heap.Count);
            var end = Math.Min(start + take, heap.Count);

            for (int i = start; i < end; i++)
            {
                var row = heap[i];
                var doc = row.TransactionDocument ?? _engine.FindById(collectionName, row.Id);
                if (doc == null) continue;

                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
            }

            void ConsiderDocument(BsonDocument doc, bool requiresPostFilter)
            {
                bool match;
                if (queryExpression == null)
                {
                    match = true;
                }
                else
                {
                    match = fullyPushed
                        ? (!requiresPostFilter || ExpressionEvaluator.Evaluate(queryExpression, doc))
                        : ExpressionEvaluator.Evaluate(queryExpression, doc);
                }

                if (!match) return;

                var seq = sequence++;

                var keys = new SortKey[sort.Count];
                for (int i = 0; i < sort.Count; i++)
                {
                    var bsonValue = TryGetSortValue(doc, sort[i].FieldName);
                    keys[i] = SortKey.FromBsonValue(bsonValue);
                }

                if (!doc.TryGetValue("_id", out var id) || id == null) return;

                var row = new TopKRow(id, keys, seq, doc);
                ConsiderRow(row);
            }

            void ConsiderSlice(ReadOnlyMemory<byte> slice, bool requiresPostFilter)
            {
                var span = slice.Span;

                BsonDocument? doc = null;
                bool match;

                if (queryExpression == null)
                {
                    if (IsLargeDocumentStub(span))
                    {
                        doc = DeserializeDocumentOrThrow(slice);
                        doc = _engine.ResolveLargeDocument(doc);
                    }

                    match = true;
                }
                else if (fullyPushed && !requiresPostFilter && !IsLargeDocumentStub(span))
                {
                    match = true;
                }
                else
                {
                    doc = DeserializeDocumentOrThrow(slice);
                    doc = _engine.ResolveLargeDocument(doc);

                    match = fullyPushed
                        ? (!requiresPostFilter || ExpressionEvaluator.Evaluate(queryExpression, doc))
                        : ExpressionEvaluator.Evaluate(queryExpression, doc);
                }

                if (!match) return;

                var seq = sequence++;

                if (heap.Count == k)
                {
                    var worst = heap[0];
                    var cmp = doc != null
                        ? CompareDocumentToRow(doc, worst, seq, sort)
                        : CompareSliceToRow(span, sortFields, worst, seq, sort);

                    if (cmp >= 0) return;
                }

                var keys = doc != null
                    ? MaterializeKeysFromDocument(doc, sort)
                    : MaterializeKeysFromSlice(span, sortFields, sort);

                if (!TryReadBsonValue(span, SortFieldBytes.Id, out var idValue))
                {
                    return;
                }

                var rowDocument = doc;
                if (rowDocument == null)
                {
                    rowDocument = DeserializeDocumentOrThrow(slice);
                    rowDocument = _engine.ResolveLargeDocument(rowDocument);
                }

                var row = new TopKRow(idValue, keys, seq, rowDocument);
                ConsiderRow(row);
            }

            void ConsiderRow(TopKRow row)
            {
                if (heap.Count < k)
                {
                    heap.Add(row);
                    HeapifyUp(heap, heap.Count - 1, sort);
                    return;
                }

                if (CompareRows(row, heap[0], sort) < 0)
                {
                    heap[0] = row;
                    HeapifyDown(heap, 0, sort);
                }
            }

            bool IsLargeDocumentStub(ReadOnlySpan<byte> span)
            {
                if (!BsonScanner.TryLocateField(span, isLargeDocumentFieldNameBytes, out var offset, out var type)) return false;
                return type == BsonType.Boolean && offset >= 0 && offset < span.Length && span[offset] != 0;
            }
        }
    }

    private IAsyncEnumerable<T> ExecuteTopKScanAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        out QueryPushdownInfo pushdown,
        CancellationToken cancellationToken)
        where T : class, new()
    {
        var sort = shape.Sort;
        var skip = Math.Max(shape.Skip.GetValueOrDefault(), 0);
        var take = shape.Take ?? 0;

        var kLong = (long)skip + take;
        if (kLong <= 0)
        {
            pushdown = new QueryPushdownInfo
            {
                WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
                OrderPushedCount = shape.Sort.Count,
                SkipPushedCount = skip > 0 ? 1 : 0,
                TakePushedCount = 1
            };
            return AsyncEmpty<T>();
        }

        if (kLong > int.MaxValue)
        {
            throw new NotSupportedException("Skip + Take is too large.");
        }

        var k = (int)kLong;

        QueryExpression? queryExpression = null;
        if (shape.Predicate != null)
        {
            try { queryExpression = _expressionParser.Parse(shape.Predicate); }
            catch (Exception ex) { throw new NotSupportedException("Parse failed", ex); }
        }

        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;

        pushdown = new QueryPushdownInfo
        {
            WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
            OrderPushedCount = shape.Sort.Count,
            SkipPushedCount = skip > 0 ? 1 : 0,
            TakePushedCount = 1
        };

        return Iterator(queryExpression, fullyPushed, pushDownPredicates, cancellationToken);

        async IAsyncEnumerable<T> Iterator(
            QueryExpression? queryExpr,
            bool allPredicatesPushed,
            ScanPredicate[]? scanPredicates,
            [EnumeratorCancellation] CancellationToken ct)
        {
            var heap = new List<TopKRow>(Math.Min(k, 256));
            long sequence = 0;

            Dictionary<string, BsonDocument?>? txOverlay = null;
            var tx = _engine.GetCurrentTransaction();
            if (tx != null)
            {
                txOverlay = new Dictionary<string, BsonDocument?>(StringComparer.Ordinal);
                foreach (var op in tx.GetOperationsSnapshot())
                {
                    if (op.CollectionName != collectionName) continue;
                    var key = op.DocumentId?.ToString() ?? string.Empty;
                    if (op.OperationType == TransactionOperationType.Delete) txOverlay[key] = null;
                    else if (op.NewDocument != null) txOverlay[key] = op.NewDocument;
                }

                if (txOverlay.Count == 0) txOverlay = null;
            }

            await foreach (var result in _engine.FindAllRawWithPredicateInfoAsync(collectionName, scanPredicates, ct).ConfigureAwait(false))
            {
                ct.ThrowIfCancellationRequested();

                var doc = DeserializeDocumentOrThrow(result.Slice);
                if (doc.TryGetValue("_collection", out var c) && c.ToString() != collectionName)
                {
                    continue;
                }

                doc = await _engine.ResolveLargeDocumentAsync(doc, ct).ConfigureAwait(false);
                var requiresPostFilter = result.RequiresPostFilter;

                if (txOverlay != null && doc.TryGetValue("_id", out var idValue))
                {
                    var idKey = idValue.ToString();
                    if (txOverlay.TryGetValue(idKey, out var txDoc))
                    {
                        txOverlay.Remove(idKey);
                        if (txDoc == null) continue;
                        ConsiderDocument(txDoc, requiresPostFilter: true);
                        continue;
                    }
                }

                ConsiderDocument(doc, requiresPostFilter);
            }

            if (txOverlay != null)
            {
                foreach (var doc in txOverlay.Values)
                {
                    ct.ThrowIfCancellationRequested();
                    if (doc == null) continue;
                    ConsiderDocument(doc, requiresPostFilter: true);
                }
            }

            heap.Sort((a, b) => CompareRows(a, b, sort));

            var start = Math.Min(skip, heap.Count);
            var end = Math.Min(start + take, heap.Count);

            for (int i = start; i < end; i++)
            {
                ct.ThrowIfCancellationRequested();

                var row = heap[i];
                if (row.TransactionDocument == null) continue;

                var entity = AotBsonMapper.FromDocument<T>(row.TransactionDocument);
                if (entity != null) yield return entity;
            }

            void ConsiderDocument(BsonDocument doc, bool requiresPostFilter)
            {
                bool match;
                if (queryExpr == null)
                {
                    match = true;
                }
                else
                {
                    match = allPredicatesPushed
                        ? (!requiresPostFilter || ExpressionEvaluator.Evaluate(queryExpr, doc))
                        : ExpressionEvaluator.Evaluate(queryExpr, doc);
                }

                if (!match) return;
                if (!doc.TryGetValue("_id", out var id) || id == null) return;

                var seq = sequence++;
                var row = new TopKRow(id, MaterializeKeysFromDocument(doc, sort), seq, doc);

                if (heap.Count < k)
                {
                    heap.Add(row);
                    HeapifyUp(heap, heap.Count - 1, sort);
                    return;
                }

                if (CompareRows(row, heap[0], sort) < 0)
                {
                    heap[0] = row;
                    HeapifyDown(heap, 0, sort);
                }
            }
        }
    }

    private static bool MatchesCollection(ReadOnlySpan<byte> document, byte[] collectionFieldNameBytes, byte[] collectionNameBytes)
    {
        if (!BsonScanner.TryLocateField(document, collectionFieldNameBytes, out int valueOffset, out var type))
        {
            return true;
        }

        if (type != BsonType.String) return false;
        if (valueOffset < 0 || valueOffset + 4 > document.Length) return false;

        var len = BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4));
        if (len <= 0) return false;

        var bytesLen = len - 1;
        var start = valueOffset + 4;
        if (start < 0 || start + bytesLen > document.Length) return false;

        return document.Slice(start, bytesLen).SequenceEqual(collectionNameBytes);
    }

    private static SortKey[] MaterializeKeysFromDocument(BsonDocument doc, IReadOnlyList<QuerySortField> sort)
    {
        var keys = new SortKey[sort.Count];
        for (int i = 0; i < sort.Count; i++)
        {
            keys[i] = SortKey.FromBsonValue(TryGetSortValue(doc, sort[i].FieldName));
        }
        return keys;
    }

    private static SortKey[] MaterializeKeysFromSlice(ReadOnlySpan<byte> slice, SortFieldBytes[] sortFields, IReadOnlyList<QuerySortField> sort)
    {
        var keys = new SortKey[sort.Count];
        for (int i = 0; i < sort.Count; i++)
        {
            keys[i] = TryReadKeyRef(slice, sortFields[i], out var keyRef) ? SortKey.Materialize(keyRef) : SortKey.Null;
        }
        return keys;
    }

    private static bool TryReadKeyRef(ReadOnlySpan<byte> document, in SortFieldBytes field, out SortKeyRef key)
    {
        if (!TryLocateFieldWithAlternates(document, field, out var valueOffset, out var type))
        {
            key = SortKeyRef.Null;
            return true;
        }

        return SortKeyRef.TryRead(document, valueOffset, type, out key);
    }

    private static bool TryLocateFieldWithAlternates(ReadOnlySpan<byte> document, in SortFieldBytes field, out int valueOffset, out BsonType type)
    {
        if (BsonScanner.TryLocateField(document, field.Primary, out valueOffset, out type)) return true;
        if (field.Alternate != null && BsonScanner.TryLocateField(document, field.Alternate, out valueOffset, out type)) return true;
        if (field.SecondAlternate != null && BsonScanner.TryLocateField(document, field.SecondAlternate, out valueOffset, out type)) return true;
        valueOffset = 0;
        type = BsonType.Null;
        return false;
    }

    private static bool TryReadBsonValue(ReadOnlySpan<byte> document, in SortFieldBytes field, [NotNullWhen(true)] out BsonValue? value)
    {
        value = null;

        if (!TryLocateFieldWithAlternates(document, field, out var valueOffset, out var type))
        {
            return false;
        }

        try
        {
            value = type switch
            {
                BsonType.Int32 => new BsonInt32(BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4))),
                BsonType.Int64 => new BsonInt64(BinaryPrimitives.ReadInt64LittleEndian(document.Slice(valueOffset, 8))),
                BsonType.Double => new BsonDouble(BitConverter.ToDouble(document.Slice(valueOffset, 8))),
                BsonType.Boolean => new BsonBoolean(document[valueOffset] != 0),
                BsonType.String => ReadString(document, valueOffset),
                BsonType.ObjectId => new BsonObjectId(new ObjectId(document.Slice(valueOffset, 12))),
                BsonType.Null => BsonNull.Value,
                _ => null
            };

            return value != null;
        }
        catch (Exception ex) when (ex is ArgumentException or ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
        {
            value = null;
            return false;
        }
    }

    private static BsonString? ReadString(ReadOnlySpan<byte> document, int valueOffset)
    {
        if (valueOffset < 0 || valueOffset + 4 > document.Length) return null;
        var len = BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4));
        if (len <= 0) return null;
        var bytesLen = len - 1;
        var start = valueOffset + 4;
        if (start < 0 || start + bytesLen > document.Length) return null;
        var s = Encoding.UTF8.GetString(document.Slice(start, bytesLen));
        return new BsonString(s);
    }

    private static BsonValue? TryGetSortValue(BsonDocument doc, string fieldName)
    {
        if (doc.TryGetValue(fieldName, out var v) && v != null) return v;

        if (fieldName.Length > 0 && fieldName[0] != '_')
        {
            var alt = char.ToUpperInvariant(fieldName[0]) + fieldName.Substring(1);
            if (doc.TryGetValue(alt, out var v2) && v2 != null) return v2;
        }

        if (string.Equals(fieldName, "_id", StringComparison.Ordinal))
        {
            if (doc.TryGetValue("id", out var v3) && v3 != null) return v3;
            if (doc.TryGetValue("Id", out var v4) && v4 != null) return v4;
        }

        return null;
    }

    private static int CompareSliceToRow(ReadOnlySpan<byte> slice, SortFieldBytes[] sortFields, in TopKRow row, long candidateSequence, IReadOnlyList<QuerySortField> sort)
    {
        for (int i = 0; i < sort.Count; i++)
        {
            var ok = TryReadKeyRef(slice, sortFields[i], out var keyRef);
            var cmp = ok ? SortKey.Compare(keyRef, row.Keys[i]) : SortKey.Compare(SortKeyRef.Null, row.Keys[i]);
            if (cmp != 0)
            {
                if (sort[i].Descending) cmp = -cmp;
                return cmp;
            }
        }

        return candidateSequence.CompareTo(row.Sequence);
    }

    private static int CompareDocumentToRow(BsonDocument doc, in TopKRow row, long candidateSequence, IReadOnlyList<QuerySortField> sort)
    {
        for (int i = 0; i < sort.Count; i++)
        {
            var cmp = SortKey.Compare(SortKey.FromBsonValue(TryGetSortValue(doc, sort[i].FieldName)), row.Keys[i]);
            if (cmp != 0)
            {
                if (sort[i].Descending) cmp = -cmp;
                return cmp;
            }
        }

        return candidateSequence.CompareTo(row.Sequence);
    }

    private static int CompareRows(in TopKRow a, in TopKRow b, IReadOnlyList<QuerySortField> sort)
    {
        for (int i = 0; i < sort.Count; i++)
        {
            var cmp = SortKey.Compare(a.Keys[i], b.Keys[i]);
            if (cmp != 0)
            {
                if (sort[i].Descending) cmp = -cmp;
                return cmp;
            }
        }

        return a.Sequence.CompareTo(b.Sequence);
    }

    private static void HeapifyUp(List<TopKRow> heap, int index, IReadOnlyList<QuerySortField> sort)
    {
        while (index > 0)
        {
            int parent = (index - 1) / 2;
            if (CompareRows(heap[index], heap[parent], sort) <= 0) break;
            (heap[parent], heap[index]) = (heap[index], heap[parent]);
            index = parent;
        }
    }

    private static void HeapifyDown(List<TopKRow> heap, int index, IReadOnlyList<QuerySortField> sort)
    {
        int count = heap.Count;
        while (true)
        {
            int left = (index * 2) + 1;
            if (left >= count) break;

            int right = left + 1;
            int largest = left;
            if (right < count && CompareRows(heap[right], heap[left], sort) > 0)
            {
                largest = right;
            }

            if (CompareRows(heap[largest], heap[index], sort) <= 0) break;

            (heap[largest], heap[index]) = (heap[index], heap[largest]);
            index = largest;
        }
    }

    private readonly struct SortFieldBytes
    {
        public static SortFieldBytes Id { get; } = Create("_id");

        public byte[] Primary { get; }
        public byte[]? Alternate { get; }
        public byte[]? SecondAlternate { get; }

        private SortFieldBytes(byte[] primary, byte[]? alternate, byte[]? secondAlternate)
        {
            Primary = primary;
            Alternate = alternate;
            SecondAlternate = secondAlternate;
        }

        public static SortFieldBytes Create(string fieldName)
        {
            if (string.Equals(fieldName, "_id", StringComparison.Ordinal))
            {
                return new SortFieldBytes(
                    Encoding.UTF8.GetBytes("_id"),
                    Encoding.UTF8.GetBytes("id"),
                    Encoding.UTF8.GetBytes("Id"));
            }

            var primary = Encoding.UTF8.GetBytes(fieldName);
            byte[]? alt = null;
            if (fieldName.Length > 0 && fieldName[0] != '_')
            {
                var altName = char.ToUpperInvariant(fieldName[0]) + fieldName.Substring(1);
                if (!string.Equals(altName, fieldName, StringComparison.Ordinal))
                {
                    alt = Encoding.UTF8.GetBytes(altName);
                }
            }
            return new SortFieldBytes(primary, alt, null);
        }
    }

    private readonly struct SortKey
    {
        public static SortKey Null => new SortKey(BsonNull.Value);

        public BsonType Type { get; }
        private BsonValue Value { get; }

        private SortKey(BsonValue value)
        {
            Value = value;
            Type = value.BsonType;
        }

        public static SortKey Materialize(in SortKeyRef key)
        {
            return new SortKey(key.ToBsonValue());
        }

        public static SortKey FromBsonValue(BsonValue? value)
        {
            if (value == null || value.IsNull) return Null;
            return new SortKey(value);
        }

        public static int Compare(in SortKeyRef a, in SortKey b)
        {
            return BsonValueComparer.Compare(a.ToBsonValue(), b.Value);
        }

        public static int Compare(in SortKey a, in SortKey b)
        {
            return BsonValueComparer.Compare(a.Value, b.Value);
        }
    }

    private readonly ref struct SortKeyRef
    {
        public static SortKeyRef Null => new SortKeyRef(BsonType.Null, 0, 0, default, default);

        public BsonType Type { get; }
        public double Double { get; }
        public long Int64 { get; }
        public Decimal128 Decimal128 { get; }
        public ReadOnlySpan<byte> Bytes { get; }

        private SortKeyRef(BsonType type, double @double, long int64, Decimal128 decimal128, ReadOnlySpan<byte> bytes)
        {
            Type = type;
            Double = @double;
            Int64 = int64;
            Decimal128 = decimal128;
            Bytes = bytes;
        }

        public static bool TryRead(ReadOnlySpan<byte> document, int valueOffset, BsonType type, out SortKeyRef key)
        {
            key = Null;
            try
            {
                switch (type)
                {
                    case BsonType.Null:
                        key = Null;
                        return true;
                    case BsonType.Int32:
                        key = new SortKeyRef(type, 0, BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4)), default, default);
                        return true;
                    case BsonType.Int64:
                        key = new SortKeyRef(type, 0, BinaryPrimitives.ReadInt64LittleEndian(document.Slice(valueOffset, 8)), default, default);
                        return true;
                    case BsonType.Double:
                        key = new SortKeyRef(type, BitConverter.ToDouble(document.Slice(valueOffset, 8)), 0, default, default);
                        return true;
                    case BsonType.Decimal128:
                    {
                        var lo = BinaryPrimitives.ReadUInt64LittleEndian(document.Slice(valueOffset, 8));
                        var hi = BinaryPrimitives.ReadUInt64LittleEndian(document.Slice(valueOffset + 8, 8));
                        key = new SortKeyRef(type, 0, 0, new Decimal128(lo, hi), default);
                        return true;
                    }
                    case BsonType.Boolean:
                        key = new SortKeyRef(type, 0, document[valueOffset] != 0 ? 1 : 0, default, default);
                        return true;
                    case BsonType.DateTime:
                    {
                        var storedDateTime = BinaryPrimitives.ReadInt64LittleEndian(document.Slice(valueOffset, 8));
                        var dateTime = BsonDateTime.DecodeStoredValue(storedDateTime);
                        key = new SortKeyRef(type, 0, BsonDateTime.GetComparableTicks(dateTime), default, default);
                        return true;
                    }
                    case BsonType.ObjectId:
                        key = new SortKeyRef(type, 0, 0, default, document.Slice(valueOffset, 12));
                        return true;
                    case BsonType.String:
                    {
                        var len = BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4));
                        if (len <= 0) return false;
                        var bytesLen = len - 1;
                        var start = valueOffset + 4;
                        if (start < 0 || start + bytesLen > document.Length) return false;
                        key = new SortKeyRef(type, 0, 0, default, document.Slice(start, bytesLen));
                        return true;
                    }
                    default:
                        return false;
                }
            }
            catch (Exception ex) when (ex is ArgumentException or ArgumentOutOfRangeException or IndexOutOfRangeException or OverflowException)
            {
                key = Null;
                return false;
            }
        }

        public BsonValue ToBsonValue()
        {
            return Type switch
            {
                BsonType.Null => BsonNull.Value,
                BsonType.Int32 => new BsonInt32(checked((int)Int64)),
                BsonType.Int64 => new BsonInt64(Int64),
                BsonType.Double => new BsonDouble(Double),
                BsonType.Decimal128 => new BsonDecimal128(Decimal128),
                BsonType.Boolean => new BsonBoolean(Int64 != 0),
                BsonType.DateTime => new BsonDateTime(new DateTime(Int64, DateTimeKind.Utc)),
                BsonType.ObjectId => new BsonObjectId(new ObjectId(Bytes)),
                BsonType.String => new BsonString(Encoding.UTF8.GetString(Bytes)),
                _ => BsonNull.Value
            };
        }
    }

    private readonly struct TopKRow
    {
        public BsonValue Id { get; }
        public SortKey[] Keys { get; }
        public long Sequence { get; }
        public BsonDocument? TransactionDocument { get; }

        public TopKRow(BsonValue id, SortKey[] keys, long sequence, BsonDocument? transactionDocument)
        {
            Id = id;
            Keys = keys;
            Sequence = sequence;
            TransactionDocument = transactionDocument;
        }
    }

    private bool CollectPredicates(QueryExpression? expr, List<ScanPredicate> predicates)
    {
        if (expr == null) return true;
        if (expr is not BinaryExpression binary) return false;

        if (binary.NodeType == ExpressionType.AndAlso)
        {
            bool left = CollectPredicates(binary.Left, predicates);
            bool right = CollectPredicates(binary.Right, predicates);
            return left && right;
        }

        if (!IsSupportedBinaryPredicate(binary.NodeType)) return false;

        var (member, constant, op) = ExtractComparison(binary);
        if (member == null || constant == null) return false;

        // 只对根对象的字段进行下推，避免嵌套属性导致误过滤。
        if (member.Expression != null && member.Expression.NodeType != ExpressionType.Parameter) return false;

        var memberName = member.MemberName;

        byte[] fieldNameBytes;
        byte[]? alternateFieldNameBytes = null;
        byte[]? secondAlternateFieldNameBytes = null;

        // 与 ExpressionEvaluator 行为保持一致：优先 camelCase，其次原字段名，Id 特殊映射到 _id。
        if (string.Equals(memberName, "Id", StringComparison.OrdinalIgnoreCase))
        {
            fieldNameBytes = System.Text.Encoding.UTF8.GetBytes("id");
            alternateFieldNameBytes = System.Text.Encoding.UTF8.GetBytes("Id");
            secondAlternateFieldNameBytes = System.Text.Encoding.UTF8.GetBytes("_id");
        }
        else
        {
            var camelName = ToCamelCase(memberName);
            fieldNameBytes = System.Text.Encoding.UTF8.GetBytes(camelName);

            if (!string.Equals(camelName, memberName, StringComparison.Ordinal))
            {
                alternateFieldNameBytes = System.Text.Encoding.UTF8.GetBytes(memberName);
            }
        }

        predicates.Add(new ScanPredicate(fieldNameBytes, alternateFieldNameBytes, secondAlternateFieldNameBytes, constant.Value, op));
        return true;
    }

    private static bool TryGetTransactionDocument(Transaction tx, string collectionName, BsonValue id, out BsonDocument? document)
    {
        document = null;
        var operations = tx.GetOperationsSnapshot();
        if (operations.Length == 0) return false;

        for (int i = operations.Length - 1; i >= 0; i--)
        {
            var op = operations[i];
            if (op.CollectionName != collectionName) continue;
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;
            if (!BsonValueComparer.ValueEquals(op.DocumentId, id)) continue;

            if (op.OperationType == TransactionOperationType.Delete)
            {
                document = null;
                return true;
            }

            if (op.NewDocument != null)
            {
                document = op.NewDocument;
                return true;
            }

            document = null;
            return true;
        }

        return false;
    }

    private static Dictionary<BsonValue, BsonDocument?>? BuildTransactionOverlay(Transaction tx, string collectionName)
    {
        Dictionary<BsonValue, BsonDocument?>? overlay = null;

        foreach (var op in tx.GetOperationsSnapshot())
        {
            if (op.CollectionName != collectionName) continue;
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;

            overlay ??= new Dictionary<BsonValue, BsonDocument?>(BsonValueComparer.EqualityComparer);

            if (op.OperationType == TransactionOperationType.Delete)
            {
                overlay[op.DocumentId] = null;
            }
            else if (op.NewDocument != null)
            {
                overlay[op.DocumentId] = op.NewDocument;
            }
        }

        return overlay is { Count: > 0 } ? overlay : null;
    }

    private static IndexKey BuildIndexKeyForOrder(BTreeIndex index, BsonDocument doc)
    {
        var fields = index.Fields;

        if (fields.Count == 1)
        {
            return doc.TryGetValue(fields[0], out var v) && v != null ? IndexKey.Create(v) : IndexKey.Create(BsonNull.Value);
        }

        var values = new BsonValue[fields.Count];
        for (int i = 0; i < fields.Count; i++)
        {
            values[i] = doc.TryGetValue(fields[i], out var v) && v != null ? v : BsonNull.Value;
        }

        return new IndexKey(values);
    }

    private static int CompareTxRows(in TxOrderRow a, in TxOrderRow b, bool descending)
    {
        var cmp = a.Key.CompareTo(b.Key);
        if (cmp != 0) return descending ? -cmp : cmp;

        cmp = a.Id.CompareTo(b.Id);
        return descending ? -cmp : cmp;
    }

    private static int CompareTxRowToBase(in TxOrderRow txRow, IndexKey baseKey, BsonValue baseId, bool descending)
    {
        var cmp = txRow.Key.CompareTo(baseKey);
        if (cmp != 0) return descending ? -cmp : cmp;

        cmp = txRow.Id.CompareTo(baseId);
        return descending ? -cmp : cmp;
    }

    private readonly struct TxOrderRow
    {
        public BsonValue Id { get; }
        public IndexKey Key { get; }
        public BsonDocument Document { get; }

        public TxOrderRow(BsonValue id, IndexKey key, BsonDocument document)
        {
            Id = id;
            Key = key;
            Document = document;
        }
    }

    private static bool IsSupportedBinaryPredicate(ExpressionType op)
    {
        return op is ExpressionType.Equal or ExpressionType.NotEqual
            or ExpressionType.GreaterThan or ExpressionType.GreaterThanOrEqual
            or ExpressionType.LessThan or ExpressionType.LessThanOrEqual;
    }

    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();
        return char.ToLowerInvariant(name[0]) + name.Substring(1);
    }

    private static ExpressionType ReverseComparisonOperator(ExpressionType op)
    {
        return op switch
        {
            ExpressionType.GreaterThan => ExpressionType.LessThan,
            ExpressionType.GreaterThanOrEqual => ExpressionType.LessThanOrEqual,
            ExpressionType.LessThan => ExpressionType.GreaterThan,
            ExpressionType.LessThanOrEqual => ExpressionType.GreaterThanOrEqual,
            _ => op
        };
    }

    private (MemberExpression? member, ConstantExpression? constant, ExpressionType op) ExtractComparison(BinaryExpression binary)
    {
        // Case 1: Member OP Constant
        if (binary.Left is MemberExpression m1 && binary.Right is ConstantExpression c1) return (m1, c1, binary.NodeType);
        
        // Case 2: Member OP Convert(Constant)
        if (binary.Left is MemberExpression m2 && binary.Right is UnaryExpression u2 && u2.NodeType == ExpressionType.Convert && u2.Operand is ConstantExpression c2)
        {
            if (TryConvertConstant(c2, u2.Type, out var converted))
            {
                return (m2, converted, binary.NodeType);
            }
        }

        // Case 3: Constant OP Member
        if (binary.Left is ConstantExpression c3 && binary.Right is MemberExpression m3) return (m3, c3, ReverseComparisonOperator(binary.NodeType));

        // Case 4: Convert(Constant) OP Member
        if (binary.Left is UnaryExpression u4 && u4.NodeType == ExpressionType.Convert && u4.Operand is ConstantExpression c4 && binary.Right is MemberExpression m4)
        {
            if (TryConvertConstant(c4, u4.Type, out var converted))
            {
                return (m4, converted, ReverseComparisonOperator(binary.NodeType));
            }
        }

        return (null, null, binary.NodeType);
    }

    private static bool TryConvertConstant(ConstantExpression constant, Type targetType, out ConstantExpression converted)
    {
        converted = constant;

        object? convertedValue;
        try
        {
            convertedValue = Convert.ChangeType(constant.Value, targetType);
        }
        catch (InvalidCastException)
        {
            return false;
        }
        catch (FormatException)
        {
            return false;
        }
        catch (OverflowException)
        {
            return false;
        }

        converted = new ConstantExpression(convertedValue);
        return true;
    }

    private static bool TryConvertDecimal128ToDouble(Decimal128 value, out double converted)
    {
        try
        {
            converted = (double)value.ToDecimal();
            return true;
        }
        catch (OverflowException)
        {
            converted = default;
            return false;
        }
    }

    private static BsonDocument DeserializeDocumentOrThrow(ReadOnlyMemory<byte> slice)
    {
        try
        {
            return BsonSerializer.DeserializeDocument(slice);
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Failed to deserialize BSON document from storage slice.", ex);
        }
    }

    // 构建索引扫描范围
    private static QueryExpression? BuildCommittedPostFilter(QueryExecutionPlan executionPlan, QueryExpression? queryExpression)
    {
        if (queryExpression == null ||
            executionPlan.UseIndex == null ||
            executionPlan.IndexScanKeys.Count == 0)
        {
            return queryExpression;
        }

        return RemoveIndexCoveredPredicates(queryExpression, executionPlan.IndexScanKeys);
    }

    private static QueryExpression? RemoveIndexCoveredPredicates(
        QueryExpression expression,
        IReadOnlyList<IndexScanKey> scanKeys)
    {
        if (expression is BinaryExpression binary &&
            binary.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
        {
            var left = RemoveIndexCoveredPredicates(binary.Left, scanKeys);
            var right = RemoveIndexCoveredPredicates(binary.Right, scanKeys);

            if (left == null) return right;
            if (right == null) return left;
            if (ReferenceEquals(left, binary.Left) && ReferenceEquals(right, binary.Right)) return expression;

            return new BinaryExpression(System.Linq.Expressions.ExpressionType.AndAlso, left, right);
        }

        return IsPredicateCoveredByIndex(expression, scanKeys) ? null : expression;
    }

    private static bool IsPredicateCoveredByIndex(QueryExpression expression, IReadOnlyList<IndexScanKey> scanKeys)
    {
        if (expression is not BinaryExpression binary ||
            !TryExtractIndexedComparison(binary, out var fieldName, out var comparisonType, out var value))
        {
            return false;
        }

        foreach (var scanKey in scanKeys)
        {
            if (!string.Equals(scanKey.FieldName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (IsComparisonCoveredByScanKey(comparisonType, value, scanKey))
            {
                return true;
            }
        }

        return false;
    }

    private static bool TryExtractIndexedComparison(
        BinaryExpression expression,
        [NotNullWhen(true)] out string? fieldName,
        out ComparisonType comparisonType,
        [NotNullWhen(true)] out BsonValue? value)
    {
        if (!TryMapComparisonType(expression.NodeType, reversed: false, out var leftComparisonType) ||
            !TryMapComparisonType(expression.NodeType, reversed: true, out var rightComparisonType))
        {
            fieldName = null;
            comparisonType = default;
            value = null;
            return false;
        }

        if (expression.Left is MemberExpression leftMember &&
            TryConvertConstantExpression(expression.Right, out var rightValue))
        {
            fieldName = leftMember.MemberName;
            comparisonType = leftComparisonType;
            value = rightValue;
            return true;
        }

        if (expression.Right is MemberExpression rightMember &&
            TryConvertConstantExpression(expression.Left, out var leftValue))
        {
            fieldName = rightMember.MemberName;
            comparisonType = rightComparisonType;
            value = leftValue;
            return true;
        }

        fieldName = null;
        comparisonType = default;
        value = null;
        return false;
    }

    private static bool TryMapComparisonType(
        System.Linq.Expressions.ExpressionType nodeType,
        bool reversed,
        out ComparisonType comparisonType)
    {
        if (reversed)
        {
            nodeType = nodeType switch
            {
                System.Linq.Expressions.ExpressionType.GreaterThan => System.Linq.Expressions.ExpressionType.LessThan,
                System.Linq.Expressions.ExpressionType.GreaterThanOrEqual => System.Linq.Expressions.ExpressionType.LessThanOrEqual,
                System.Linq.Expressions.ExpressionType.LessThan => System.Linq.Expressions.ExpressionType.GreaterThan,
                System.Linq.Expressions.ExpressionType.LessThanOrEqual => System.Linq.Expressions.ExpressionType.GreaterThanOrEqual,
                _ => nodeType
            };
        }

        comparisonType = nodeType switch
        {
            System.Linq.Expressions.ExpressionType.Equal => ComparisonType.Equal,
            System.Linq.Expressions.ExpressionType.GreaterThan => ComparisonType.GreaterThan,
            System.Linq.Expressions.ExpressionType.GreaterThanOrEqual => ComparisonType.GreaterThanOrEqual,
            System.Linq.Expressions.ExpressionType.LessThan => ComparisonType.LessThan,
            System.Linq.Expressions.ExpressionType.LessThanOrEqual => ComparisonType.LessThanOrEqual,
            _ => default
        };

        return nodeType is System.Linq.Expressions.ExpressionType.Equal
            or System.Linq.Expressions.ExpressionType.GreaterThan
            or System.Linq.Expressions.ExpressionType.GreaterThanOrEqual
            or System.Linq.Expressions.ExpressionType.LessThan
            or System.Linq.Expressions.ExpressionType.LessThanOrEqual;
    }

    private static bool TryConvertConstantExpression(
        QueryExpression expression,
        [NotNullWhen(true)] out BsonValue? value)
    {
        if (expression is ConstantExpression constant)
        {
            try
            {
                value = constant.Value == null ? BsonNull.Value : BsonConversion.ToBsonValue(constant.Value);
                return true;
            }
            catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or InvalidCastException or FormatException or OverflowException or ArgumentException)
            {
                value = null;
                return false;
            }
        }

        value = null;
        return false;
    }

    private static bool IsComparisonCoveredByScanKey(
        ComparisonType comparisonType,
        BsonValue value,
        IndexScanKey scanKey)
    {
        if (comparisonType == ComparisonType.Equal)
        {
            return scanKey.ComparisonType == ComparisonType.Equal &&
                   BsonValueComparer.ValueEquals(scanKey.Value, value);
        }

        if (scanKey.ComparisonType == comparisonType &&
            BsonValueComparer.ValueEquals(scanKey.Value, value))
        {
            return true;
        }

        if (scanKey.ComparisonType != ComparisonType.Range)
        {
            return false;
        }

        return comparisonType switch
        {
            ComparisonType.GreaterThan =>
                scanKey.LowerValue != null &&
                !scanKey.IncludeLower &&
                BsonValueComparer.ValueEquals(scanKey.LowerValue, value),
            ComparisonType.GreaterThanOrEqual =>
                scanKey.LowerValue != null &&
                scanKey.IncludeLower &&
                BsonValueComparer.ValueEquals(scanKey.LowerValue, value),
            ComparisonType.LessThan =>
                scanKey.UpperValue != null &&
                !scanKey.IncludeUpper &&
                BsonValueComparer.ValueEquals(scanKey.UpperValue, value),
            ComparisonType.LessThanOrEqual =>
                scanKey.UpperValue != null &&
                scanKey.IncludeUpper &&
                BsonValueComparer.ValueEquals(scanKey.UpperValue, value),
            _ => false
        };
    }

    private static IndexScanRange BuildIndexScanRange(QueryExecutionPlan executionPlan)
    {
        if (executionPlan.IndexScanKeys.Count == 0)
        {
            return new IndexScanRange
            {
                MinKey = IndexKey.MinValue,
                MaxKey = IndexKey.MaxValue,
                IncludeMin = true,
                IncludeMax = true
            };
        }

        var minValues = new List<BsonValue>();
        var maxValues = new List<BsonValue>();
        bool includeMin = true;
        bool includeMax = true;
        bool stoppedAtRange = false;
        ComparisonType lastOp = ComparisonType.Equal;
        bool rangeIncludesUpper = false;

        foreach (var key in executionPlan.IndexScanKeys)
        {
            if (key.ComparisonType == ComparisonType.Equal)
            {
                minValues.Add(key.Value);
                maxValues.Add(key.Value);
            }
            else
            {
                stoppedAtRange = true;
                lastOp = key.ComparisonType;
                switch (key.ComparisonType)
                {
                    case ComparisonType.NotEqual:
                        maxValues.Add(BsonMaxKey.Value);
                        includeMin = true;
                        includeMax = true;
                        break;
                    case ComparisonType.GreaterThan:
                        minValues.Add(key.Value);
                        maxValues.Add(BsonMaxKey.Value);
                        includeMin = false;
                        break;
                    case ComparisonType.GreaterThanOrEqual:
                        minValues.Add(key.Value);
                        maxValues.Add(BsonMaxKey.Value);
                        includeMin = true;
                        break;
                    case ComparisonType.LessThan:
                        maxValues.Add(key.Value);
                        includeMax = false;
                        break;
                    case ComparisonType.LessThanOrEqual:
                        maxValues.Add(key.Value);
                        includeMax = true;
                        break;
                    case ComparisonType.Range:
                        minValues.Add(key.LowerValue ?? BsonMinKey.Value);
                        maxValues.Add(key.UpperValue ?? BsonMaxKey.Value);
                        includeMin = key.IncludeLower;
                        includeMax = key.IncludeUpper;
                        rangeIncludesUpper = key.IncludeUpper;
                        break;
                }
                break;
            }
        }

        // Pad MaxKey to ensure correct prefix/range matching
        if (!stoppedAtRange)
        {
            // All Equals: Prefix match, so we want everything starting with this prefix
            maxValues.Add(BsonMaxKey.Value);
        }
        else
        {
            // If we ended with LT/LTE, we need to ensure we include children of the boundary
            // e.g. A <= 5 should include (5, 1)
            if (lastOp == ComparisonType.LessThanOrEqual ||
                (lastOp == ComparisonType.Range && rangeIncludesUpper))
            {
                maxValues.Add(BsonMaxKey.Value);
            }
        }

        return new IndexScanRange
        {
            MinKey = CreateIndexKey(minValues),
            MaxKey = CreateIndexKey(maxValues),
            IncludeMin = includeMin,
            IncludeMax = includeMax
        };
    }

    private static IndexKey? BuildExactIndexKey(QueryExecutionPlan executionPlan)
    {
        if (executionPlan.IndexScanKeys.Count == 0) return null;
        var values = new List<BsonValue>();
        foreach (var key in executionPlan.IndexScanKeys)
        {
            if (key.ComparisonType != ComparisonType.Equal) return null;
            values.Add(key.Value);
        }
        return CreateIndexKey(values);
    }

    private static IndexKey CreateIndexKey(List<BsonValue> values)
    {
        return values.Count == 1
            ? IndexKey.Create(values[0])
            : new IndexKey(values.ToArray());
    }
}
