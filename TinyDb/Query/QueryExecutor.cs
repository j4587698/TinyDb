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

    private QueryExpression? ParsePredicate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        Expression<Func<T, bool>>? expression)
        where T : class
    {
        if (expression == null) return null;

        try
        {
            return RuntimeQueryExpressionBinder.Bind(_expressionParser.Parse(expression));
        }
        catch (Exception ex)
        {
            throw new NotSupportedException("Parse failed", ex);
        }
    }

    private static QueryPushdownInfo CreatePushdownInfo<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryShape<T> shape,
        int orderPushedCount = 0,
        int skipPushedCount = 0,
        int takePushedCount = 0)
        where T : class
    {
        return new QueryPushdownInfo
        {
            WherePushedCount = shape.Predicate != null ? shape.PushedWhereCount : 0,
            OrderPushedCount = orderPushedCount,
            SkipPushedCount = skipPushedCount,
            TakePushedCount = takePushedCount
        };
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
        where T : class
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var executionPlan = _queryOptimizer.CreateExecutionPlan(collectionName, expression);

        return ExecutePlanned(executionPlan, collectionName, expression);
    }

    internal IEnumerable<T> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryExpression? queryExpression)
        where T : class
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        queryExpression = RuntimeQueryExpressionBinder.Bind(queryExpression);
        var executionPlan = _queryOptimizer.CreateExecutionPlan(collectionName, queryExpression);

        return executionPlan.Strategy switch
        {
            QueryExecutionStrategy.PrimaryKeyLookup => ExecutePrimaryKeyLookup<T>(executionPlan),
            QueryExecutionStrategy.IndexScan => ExecuteIndexScan<T>(executionPlan),
            QueryExecutionStrategy.IndexSeek => ExecuteIndexSeek<T>(executionPlan),
            QueryExecutionStrategy.IndexUnion => ExecuteIndexUnion<T>(executionPlan),
            _ => ExecuteFullTableScanQuery<T>(collectionName, queryExpression)
        };
    }

    public IAsyncEnumerable<T> ExecuteAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        Expression<Func<T, bool>>? expression = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var executionPlan = _queryOptimizer.CreateExecutionPlan(collectionName, expression, planningMetadataOnly: true);

        return ExecutePlannedAsync(executionPlan, collectionName, expression, cancellationToken);
    }

    internal long Count<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        Expression<Func<T, bool>>? expression = null)
        where T : class
    {
        var queryExpression = ParsePredicate(expression);

        return Count(collectionName, queryExpression);
    }

    internal Task<long> CountAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        Expression<Func<T, bool>>? expression = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        var queryExpression = ParsePredicate(expression);

        return CountAsync(collectionName, queryExpression, cancellationToken);
    }

    internal long Count(string collectionName, QueryExpression? queryExpression)
    {
        queryExpression = RuntimeQueryExpressionBinder.Bind(queryExpression);
        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;

        var collectionFieldNameBytes = Encoding.UTF8.GetBytes("_collection");
        var collectionNameBytes = Encoding.UTF8.GetBytes(collectionName);
        var isLargeDocumentFieldNameBytes = Encoding.UTF8.GetBytes("_isLargeDocument");

        long count = 0;
        var tx = _engine.GetCurrentTransaction();
        var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, collectionName) : null;

        foreach (var result in _engine.FindAllRawWithPredicateInfo(collectionName, pushDownPredicates))
        {
            var slice = result.Slice;
            var span = slice.Span;
            if (!MatchesCollection(span, collectionFieldNameBytes, collectionNameBytes))
            {
                continue;
            }

            if (txOverlay != null && QuerySortKeyReader.TryReadBsonValue(span, SortFieldBytes.Id, out var idValue))
            {
                if (txOverlay.TryGetValue(idValue, out var txDoc))
                {
                    txOverlay.Remove(idValue);
                    if (txDoc == null) continue;
                    if (queryExpression == null || ExpressionEvaluator.Evaluate(queryExpression, txDoc)) count++;
                    continue;
                }
            }

            if (queryExpression == null)
            {
                count++;
                continue;
            }

            if (fullyPushed &&
                !result.RequiresPostFilter &&
                !IsLargeDocumentStub(span, isLargeDocumentFieldNameBytes))
            {
                count++;
                continue;
            }

            var doc = DeserializeDocumentOrThrow(slice);
            doc = _engine.ResolveLargeDocument(doc);
            if (ExpressionEvaluator.Evaluate(queryExpression, doc))
            {
                count++;
            }
        }

        if (txOverlay != null)
        {
            foreach (var txDoc in txOverlay.Values)
            {
                if (txDoc == null) continue;
                if (queryExpression == null || ExpressionEvaluator.Evaluate(queryExpression, txDoc)) count++;
            }
        }

        return count;
    }

    internal async Task<long> CountAsync(
        string collectionName,
        QueryExpression? queryExpression,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        queryExpression = RuntimeQueryExpressionBinder.Bind(queryExpression);
        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;

        var collectionFieldNameBytes = Encoding.UTF8.GetBytes("_collection");
        var collectionNameBytes = Encoding.UTF8.GetBytes(collectionName);
        var isLargeDocumentFieldNameBytes = Encoding.UTF8.GetBytes("_isLargeDocument");

        long count = 0;
        var tx = _engine.GetCurrentTransaction();
        var txOverlay = tx != null ? QueryTransactionOverlay.Build(tx, collectionName) : null;

        await foreach (var result in _engine.FindAllRawWithPredicateInfoAsync(collectionName, pushDownPredicates, cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();

            var slice = result.Slice;
            var span = slice.Span;
            if (!MatchesCollection(span, collectionFieldNameBytes, collectionNameBytes))
            {
                continue;
            }

            if (txOverlay != null && QuerySortKeyReader.TryReadBsonValue(span, SortFieldBytes.Id, out var idValue))
            {
                if (txOverlay.TryGetValue(idValue, out var txDoc))
                {
                    txOverlay.Remove(idValue);
                    if (txDoc == null) continue;
                    if (queryExpression == null || ExpressionEvaluator.Evaluate(queryExpression, txDoc)) count++;
                    continue;
                }
            }

            if (queryExpression == null)
            {
                count++;
                continue;
            }

            if (fullyPushed &&
                !result.RequiresPostFilter &&
                !IsLargeDocumentStub(span, isLargeDocumentFieldNameBytes))
            {
                count++;
                continue;
            }

            var doc = DeserializeDocumentOrThrow(slice);
            doc = await _engine.ResolveLargeDocumentAsync(doc, cancellationToken).ConfigureAwait(false);
            if (ExpressionEvaluator.Evaluate(queryExpression, doc))
            {
                count++;
            }
        }

        if (txOverlay != null)
        {
            foreach (var txDoc in txOverlay.Values)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (txDoc == null) continue;
                if (queryExpression == null || ExpressionEvaluator.Evaluate(queryExpression, txDoc)) count++;
            }
        }

        return count;
    }

    internal IEnumerable<T> ExecuteShaped<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        out QueryPushdownInfo pushdown)
        where T : class
    {
        if (shape == null) throw new ArgumentNullException(nameof(shape));
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var skip = shape.Skip.GetValueOrDefault();
        var take = shape.Take;

        if (shape.Sort.Count == 0)
        {
            var result = Execute<T>(collectionName, shape.Predicate);

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
                    pushdown = CreatePushdownInfo(shape, skipPushedCount: skipPushed ? 1 : 0, takePushedCount: 1);
                    return Enumerable.Empty<T>();
                }

                result = result.Take(t);
                takePushed = true;
            }

            pushdown = CreatePushdownInfo(
                shape,
                skipPushedCount: skipPushed ? 1 : 0,
                takePushedCount: takePushed ? 1 : 0);

            return result;
        }

        var tx = _engine.GetCurrentTransaction();

        if (TryCreatePredicateIndexBeforeOrderPlan(collectionName, shape, out var predicatePlan))
        {
            pushdown = CreatePushdownInfo(shape);
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
                pushdown = CreatePushdownInfo(
                    shape,
                    orderPushedCount: shape.Sort.Count,
                    skipPushedCount: skip > 0 ? 1 : 0,
                    takePushedCount: 1);
                return Enumerable.Empty<T>();
            }

            return ExecuteTopKScan<T>(collectionName, shape, out pushdown);
        }

        pushdown = CreatePushdownInfo(shape);
        return Execute<T>(collectionName, shape.Predicate);
    }

    internal IAsyncEnumerable<T> ExecuteShapedAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        string collectionName,
        QueryShape<T> shape,
        out QueryPushdownInfo pushdown,
        CancellationToken cancellationToken = default)
        where T : class
    {
        if (shape == null) throw new ArgumentNullException(nameof(shape));
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var skip = shape.Skip.GetValueOrDefault();
        var take = shape.Take;

        if (shape.Sort.Count == 0)
        {
            var result = ExecuteAsync<T>(collectionName, shape.Predicate, cancellationToken);

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
                    pushdown = CreatePushdownInfo(shape, skipPushedCount: skipPushed ? 1 : 0, takePushedCount: 1);
                    return AsyncEmpty<T>();
                }

                takePushed = true;
            }

            pushdown = CreatePushdownInfo(
                shape,
                skipPushedCount: skipPushed ? 1 : 0,
                takePushedCount: takePushed ? 1 : 0);

            return ApplySkipTakeAsync(result, skip, take, cancellationToken);
        }

        var tx = _engine.GetCurrentTransaction();

        if (TryCreatePredicateIndexBeforeOrderPlan(collectionName, shape, out var predicatePlan))
        {
            pushdown = CreatePushdownInfo(shape);
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
                pushdown = CreatePushdownInfo(
                    shape,
                    orderPushedCount: shape.Sort.Count,
                    skipPushedCount: skip > 0 ? 1 : 0,
                    takePushedCount: 1);
                return AsyncEmpty<T>();
            }

            return ExecuteTopKScanAsync<T>(collectionName, shape, out pushdown, cancellationToken);
        }

        pushdown = CreatePushdownInfo(shape);
        return ExecuteAsync<T>(collectionName, shape.Predicate, cancellationToken);
    }

    private IEnumerable<T> ExecutePlanned<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        string collectionName,
        Expression<Func<T, bool>>? fallbackExpression)
        where T : class
    {
        return executionPlan.Strategy switch
        {
            QueryExecutionStrategy.PrimaryKeyLookup => ExecutePrimaryKeyLookup<T>(executionPlan),
            QueryExecutionStrategy.IndexScan => ExecuteIndexScan<T>(executionPlan),
            QueryExecutionStrategy.IndexSeek => ExecuteIndexSeek<T>(executionPlan),
            QueryExecutionStrategy.IndexUnion => ExecuteIndexUnion<T>(executionPlan),
            _ => ExecuteFullTableScan(collectionName, fallbackExpression)
        };
    }

    private IAsyncEnumerable<T> ExecutePlannedAsync<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(
        QueryExecutionPlan executionPlan,
        string collectionName,
        Expression<Func<T, bool>>? fallbackExpression,
        CancellationToken cancellationToken)
        where T : class
    {
        return executionPlan.Strategy switch
        {
            QueryExecutionStrategy.PrimaryKeyLookup => ExecutePrimaryKeyLookupAsync<T>(executionPlan, cancellationToken),
            QueryExecutionStrategy.IndexScan => ExecuteIndexScanAsync<T>(executionPlan, cancellationToken),
            QueryExecutionStrategy.IndexSeek => ExecuteIndexSeekAsync<T>(executionPlan, cancellationToken),
            QueryExecutionStrategy.IndexUnion => ExecuteIndexUnionAsync<T>(executionPlan, cancellationToken),
            _ => ExecuteFullTableScanAsync(collectionName, fallbackExpression, cancellationToken)
        };
    }

}
