using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Serialization;

namespace TinyDb.Query;

public sealed class QueryExecutor
{
    private static readonly long UnixEpochTicks = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

    private readonly TinyDbEngine _engine;
    private readonly ExpressionParser _expressionParser;
    private readonly QueryOptimizer _queryOptimizer;

    public QueryExecutor(TinyDbEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _expressionParser = new ExpressionParser();
        _queryOptimizer = new QueryOptimizer(engine);
    }

    public IEnumerable<T> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class, new()
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var executionPlan = _queryOptimizer.CreateExecutionPlan(collectionName, expression);

        return executionPlan.Strategy switch
        {
            QueryExecutionStrategy.PrimaryKeyLookup => ExecutePrimaryKeyLookup<T>(executionPlan),
            QueryExecutionStrategy.IndexScan => ExecuteIndexScan<T>(executionPlan),
            QueryExecutionStrategy.IndexSeek => ExecuteIndexSeek<T>(executionPlan),
            _ => ExecuteFullTableScan<T>(collectionName, expression)
        };
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

    // ... (省略 Index 相关方法，保持不变) ...
    internal IEnumerable<BsonDocument> ExecuteIndexScanForTests(QueryExecutionPlan executionPlan) => ExecuteIndexScan<BsonDocument>(executionPlan);
    internal IEnumerable<T> ExecutePrimaryKeyLookupForTests<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new() => ExecutePrimaryKeyLookup<T>(executionPlan);
    internal IEnumerable<T> ExecuteIndexSeekForTests<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new() => ExecuteIndexSeek<T>(executionPlan);

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

    private IEnumerable<T> ExecuteIndexScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new()
    {
        var tx = _engine.GetCurrentTransaction();
        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        if (idxMgr == null || executionPlan.UseIndex == null)
        {
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression)) yield return item;
            yield break;
        }
        var index = idxMgr.GetIndex(executionPlan.UseIndex.Name);
        if (index == null)
        {
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression)) yield return item;
            yield break;
        }

        var range = BuildIndexScanRange(executionPlan);
        var ids = index.FindRange(range.MinKey, range.MaxKey, range.IncludeMin, range.IncludeMax);
        
        QueryExpression? qe = null;
        if (executionPlan.OriginalExpression != null) qe = _expressionParser.Parse<T>((Expression<Func<T, bool>>)executionPlan.OriginalExpression);

        var txOverlay = tx != null ? BuildTransactionOverlay(tx, executionPlan.CollectionName) : null;

        foreach (var id in ids)
        {
            if (txOverlay != null && txOverlay.TryGetValue(id, out var txDoc))
            {
                txOverlay.Remove(id);
                if (txDoc == null) continue;
                if (qe != null && !ExpressionEvaluator.Evaluate(qe, txDoc)) continue;

                var txEntity = AotBsonMapper.FromDocument<T>(txDoc);
                if (txEntity != null) yield return txEntity;
                continue;
            }

            var doc = _engine.FindById(executionPlan.CollectionName, id);
            if (doc != null && (qe == null || ExpressionEvaluator.Evaluate(qe, doc)))
            {
                var entity = AotBsonMapper.FromDocument<T>(doc);
                if (entity != null) yield return entity;
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

    private IEnumerable<T> ExecuteIndexSeek<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan) where T : class, new()
    {
        var tx = _engine.GetCurrentTransaction();
        var idxMgr = _engine.GetIndexManager(executionPlan.CollectionName);
        if (idxMgr == null || executionPlan.UseIndex == null)
        {
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression)) yield return item;
            yield break;
        }
        var index = idxMgr.GetIndex(executionPlan.UseIndex.Name);
        if (index == null)
        {
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression)) yield return item;
            yield break;
        }

        var key = BuildExactIndexKey(executionPlan);
        if (key == null)
        {
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression)) yield return item;
            yield break;
        }

        QueryExpression? qe = null;
        if (executionPlan.OriginalExpression != null) qe = _expressionParser.Parse<T>((Expression<Func<T, bool>>)executionPlan.OriginalExpression);

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
                    if (doc != null && (qe == null || ExpressionEvaluator.Evaluate(qe, doc)))
                    {
                        var entity = AotBsonMapper.FromDocument<T>(doc);
                        if (entity != null) yield return entity;
                    }
                }
            }
        }
        else
        {
            foreach (var id in index.Find(key))
            {
                if (txOverlay != null && txOverlay.TryGetValue(id, out var txDoc))
                {
                    txOverlay.Remove(id);
                    if (txDoc == null) continue;
                    if (qe != null && !ExpressionEvaluator.Evaluate(qe, txDoc)) continue;

                    var txEntity = AotBsonMapper.FromDocument<T>(txDoc);
                    if (txEntity != null) yield return txEntity;
                    continue;
                }

                var doc = _engine.FindById(executionPlan.CollectionName, id);
                if (doc != null && (qe == null || ExpressionEvaluator.Evaluate(qe, doc)))
                {
                    var entity = AotBsonMapper.FromDocument<T>(doc);
                    if (entity != null) yield return entity;
                }
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
        var tx = _engine.GetCurrentTransaction();
        QueryExpression? queryExpression = null;

        if (expression != null)
        {
            try { queryExpression = _expressionParser.Parse(expression); }
            catch (Exception ex) { throw new NotSupportedException("Parse failed", ex); }
        }

        // 1. 准备下推谓词
        var predicates = new List<ScanPredicate>();
        bool fullyPushed = CollectPredicates(queryExpression, predicates);
        var pushDownPredicates = predicates.Count > 0 ? predicates.ToArray() : null;

        // 2. 处理管道
        var rawPipeline = _engine.FindAllRawWithPredicateInfo(collectionName, pushDownPredicates)
            .Select(r => (Doc: TryDeserialize(r.Slice), r.RequiresPostFilter))
            .Where(x => x.Doc != null)
            .Select(x => (Doc: x.Doc!, x.RequiresPostFilter))
            .Where(x => !x.Doc.TryGetValue("_collection", out var c) || c.ToString() == collectionName)
            .Select(x => (Doc: _engine.ResolveLargeDocument(x.Doc), x.RequiresPostFilter));

        // 3. 事务覆盖
        IEnumerable<(BsonDocument Doc, bool RequiresPostFilter)> docs;
        if (tx != null)
        {
            var txOverlay = new ConcurrentDictionary<string, BsonDocument?>();
            foreach (var op in tx.Operations)
            {
                if (op.CollectionName != collectionName) continue;
                var k = op.DocumentId?.ToString() ?? "";
                if (op.OperationType == TransactionOperationType.Delete) txOverlay[k] = null;
                else if (op.NewDocument != null) txOverlay[k] = op.NewDocument;
            }

            if (!txOverlay.IsEmpty)
            {
                docs = rawPipeline.Select(item =>
                    {
                        var id = item.Doc["_id"].ToString();
                        if (txOverlay.TryRemove(id, out var txDoc))
                        {
                            return (Doc: txDoc, RequiresPostFilter: true);
                        }

                        return (Doc: item.Doc, item.RequiresPostFilter);
                    })
                    .Where(x => x.Doc != null)
                    .Select(x => (Doc: x.Doc!, x.RequiresPostFilter))
                    .Concat(txOverlay.Values.Where(v => v != null).Select(v => (Doc: v!, RequiresPostFilter: true)));
            }
            else docs = rawPipeline;
        }
        else docs = rawPipeline;

        // 4. 内存过滤与映射
        var filtered = queryExpression == null
            ? docs.Select(x => x.Doc)
            : fullyPushed
                ? docs.Where(x => !x.RequiresPostFilter || ExpressionEvaluator.Evaluate(queryExpression, x.Doc)).Select(x => x.Doc)
                : docs.Where(x => ExpressionEvaluator.Evaluate(queryExpression, x.Doc)).Select(x => x.Doc);

        return filtered
            .Select(doc => AotBsonMapper.FromDocument<T>(doc))
            .Where(entity => entity != null)!;
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
        foreach (var stat in idxMgr.GetAllStatistics())
        {
            if (stat.Type != IndexType.BTree) continue;
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

        var txOverlay = new Dictionary<BsonValue, BsonDocument?>(EqualityComparer<BsonValue>.Default);
        foreach (var op in tx.Operations)
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
                foreach (var op in tx.Operations)
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
                    match = true;
                }
                else if (fullyPushed && !requiresPostFilter && !IsLargeDocumentStub(span))
                {
                    match = true;
                }
                else
                {
                    doc = TryDeserialize(slice);
                    if (doc == null) return;
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

                var row = new TopKRow(idValue, keys, seq, transactionDocument: null);
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
        catch
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
        public static SortKey Null => new SortKey(BsonType.Null, 0, 0, 0, null);

        public BsonType Type { get; }
        private double Numeric { get; }
        private long Int64 { get; }
        private long DateTimeTicks { get; }
        private byte[]? Bytes { get; }

        private SortKey(BsonType type, double numeric, long int64, long dateTimeTicks, byte[]? bytes)
        {
            Type = type;
            Numeric = numeric;
            Int64 = int64;
            DateTimeTicks = dateTimeTicks;
            Bytes = bytes;
        }

        public static SortKey Materialize(in SortKeyRef key)
        {
            return key.Type switch
            {
                BsonType.Null => Null,
                BsonType.Int32 or BsonType.Int64 or BsonType.Double or BsonType.Decimal128 =>
                    new SortKey(key.Type, key.Numeric, 0, 0, null),
                BsonType.Boolean =>
                    new SortKey(key.Type, 0, key.Int64, 0, null),
                BsonType.DateTime =>
                    new SortKey(key.Type, 0, 0, key.Int64, null),
                BsonType.String or BsonType.ObjectId =>
                    new SortKey(key.Type, 0, 0, 0, key.Bytes.ToArray()),
                _ => Null
            };
        }

        public static SortKey FromBsonValue(BsonValue? value)
        {
            if (value == null || value.IsNull) return Null;

            return value switch
            {
                BsonInt32 i32 => new SortKey(BsonType.Int32, i32.Value, 0, 0, null),
                BsonInt64 i64 => new SortKey(BsonType.Int64, i64.Value, 0, 0, null),
                BsonDouble d => new SortKey(BsonType.Double, d.Value, 0, 0, null),
                BsonDecimal128 dec =>
                    new SortKey(BsonType.Decimal128, SafeToDouble(dec.Value), 0, 0, null),
                BsonBoolean b => new SortKey(BsonType.Boolean, 0, b.Value ? 1 : 0, 0, null),
                BsonDateTime dt => new SortKey(BsonType.DateTime, 0, 0, dt.Value.ToUniversalTime().Ticks, null),
                BsonString s => new SortKey(BsonType.String, 0, 0, 0, Encoding.UTF8.GetBytes(s.Value)),
                BsonObjectId oid => new SortKey(BsonType.ObjectId, 0, 0, 0, oid.Value.ToByteArray().ToArray()),
                _ => new SortKey(BsonType.String, 0, 0, 0, Encoding.UTF8.GetBytes(value.ToString()))
            };
        }

        private static double SafeToDouble(Decimal128 d)
        {
            try { return (double)d.ToDecimal(); }
            catch { return 0d; }
        }

        public static int Compare(in SortKeyRef a, in SortKey b)
        {
            if (a.Type == BsonType.Null && b.Type == BsonType.Null) return 0;
            if (a.Type == BsonType.Null) return -1;
            if (b.Type == BsonType.Null) return 1;

            if (IsNumeric(a.Type) && IsNumeric(b.Type))
            {
                return a.Numeric.CompareTo(b.Numeric);
            }

            if (a.Type == BsonType.Boolean && b.Type == BsonType.Boolean)
            {
                return a.Int64.CompareTo(b.Int64);
            }

            if (a.Type == BsonType.DateTime && b.Type == BsonType.DateTime)
            {
                return a.Int64.CompareTo(b.DateTimeTicks);
            }

            if ((a.Type == BsonType.String && b.Type == BsonType.String) ||
                (a.Type == BsonType.ObjectId && b.Type == BsonType.ObjectId))
            {
                return CompareBytes(a.Bytes, b.Bytes);
            }

            return ((byte)a.Type).CompareTo((byte)b.Type);
        }

        public static int Compare(in SortKey a, in SortKey b)
        {
            if (a.Type == BsonType.Null && b.Type == BsonType.Null) return 0;
            if (a.Type == BsonType.Null) return -1;
            if (b.Type == BsonType.Null) return 1;

            if (IsNumeric(a.Type) && IsNumeric(b.Type))
            {
                return a.Numeric.CompareTo(b.Numeric);
            }

            if (a.Type == BsonType.Boolean && b.Type == BsonType.Boolean)
            {
                return a.Int64.CompareTo(b.Int64);
            }

            if (a.Type == BsonType.DateTime && b.Type == BsonType.DateTime)
            {
                return a.DateTimeTicks.CompareTo(b.DateTimeTicks);
            }

            if ((a.Type == BsonType.String && b.Type == BsonType.String) ||
                (a.Type == BsonType.ObjectId && b.Type == BsonType.ObjectId))
            {
                return CompareBytes(a.Bytes, b.Bytes);
            }

            return ((byte)a.Type).CompareTo((byte)b.Type);
        }

        private static bool IsNumeric(BsonType type)
        {
            return type is BsonType.Int32 or BsonType.Int64 or BsonType.Double or BsonType.Decimal128;
        }

        private static int CompareBytes(ReadOnlySpan<byte> a, byte[]? b)
        {
            if (b == null) return a.Length == 0 ? 0 : 1;
            return a.SequenceCompareTo(b);
        }

        private static int CompareBytes(byte[]? a, byte[]? b)
        {
            if (ReferenceEquals(a, b)) return 0;
            if (a == null) return -1;
            if (b == null) return 1;
            return ((ReadOnlySpan<byte>)a).SequenceCompareTo(b);
        }
    }

    private readonly ref struct SortKeyRef
    {
        public static SortKeyRef Null => new SortKeyRef(BsonType.Null, 0, 0, default);

        public BsonType Type { get; }
        public double Numeric { get; }
        public long Int64 { get; }
        public ReadOnlySpan<byte> Bytes { get; }

        private SortKeyRef(BsonType type, double numeric, long int64, ReadOnlySpan<byte> bytes)
        {
            Type = type;
            Numeric = numeric;
            Int64 = int64;
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
                        key = new SortKeyRef(type, BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4)), 0, default);
                        return true;
                    case BsonType.Int64:
                        key = new SortKeyRef(type, BinaryPrimitives.ReadInt64LittleEndian(document.Slice(valueOffset, 8)), 0, default);
                        return true;
                    case BsonType.Double:
                        key = new SortKeyRef(type, BitConverter.ToDouble(document.Slice(valueOffset, 8)), 0, default);
                        return true;
                    case BsonType.Decimal128:
                    {
                        var lo = BinaryPrimitives.ReadUInt64LittleEndian(document.Slice(valueOffset, 8));
                        var hi = BinaryPrimitives.ReadUInt64LittleEndian(document.Slice(valueOffset + 8, 8));
                        var dec = new Decimal128(lo, hi);
                        double d;
                        try { d = (double)dec.ToDecimal(); }
                        catch { d = 0d; }
                        key = new SortKeyRef(type, d, 0, default);
                        return true;
                    }
                    case BsonType.Boolean:
                        key = new SortKeyRef(type, 0, document[valueOffset] != 0 ? 1 : 0, default);
                        return true;
                    case BsonType.DateTime:
                    {
                        var ms = BinaryPrimitives.ReadInt64LittleEndian(document.Slice(valueOffset, 8));
                        var ticks = UnixEpochTicks + (ms * TimeSpan.TicksPerMillisecond);
                        key = new SortKeyRef(type, 0, ticks, default);
                        return true;
                    }
                    case BsonType.ObjectId:
                        key = new SortKeyRef(type, 0, 0, document.Slice(valueOffset, 12));
                        return true;
                    case BsonType.String:
                    {
                        var len = BinaryPrimitives.ReadInt32LittleEndian(document.Slice(valueOffset, 4));
                        if (len <= 0) return false;
                        var bytesLen = len - 1;
                        var start = valueOffset + 4;
                        if (start < 0 || start + bytesLen > document.Length) return false;
                        key = new SortKeyRef(type, 0, 0, document.Slice(start, bytesLen));
                        return true;
                    }
                    default:
                        return false;
                }
            }
            catch
            {
                key = Null;
                return false;
            }
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
        if (string.Equals(memberName, "Id", StringComparison.Ordinal))
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
        if (tx.Operations.Count == 0) return false;

        for (int i = tx.Operations.Count - 1; i >= 0; i--)
        {
            var op = tx.Operations[i];
            if (op.CollectionName != collectionName) continue;
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;
            if (!op.DocumentId.Equals(id)) continue;

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

        foreach (var op in tx.Operations)
        {
            if (op.CollectionName != collectionName) continue;
            if (op.DocumentId == null || op.DocumentId.IsNull) continue;

            overlay ??= new Dictionary<BsonValue, BsonDocument?>(EqualityComparer<BsonValue>.Default);

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
            return doc.TryGetValue(fields[0], out var v) && v != null ? new IndexKey(v) : new IndexKey(BsonNull.Value);
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
            try 
            {
                var converted = Convert.ChangeType(c2.Value, u2.Type);
                return (m2, new ConstantExpression(converted), binary.NodeType);
            }
            catch { }
        }

        // Case 3: Constant OP Member
        if (binary.Left is ConstantExpression c3 && binary.Right is MemberExpression m3) return (m3, c3, ReverseComparisonOperator(binary.NodeType));

        // Case 4: Convert(Constant) OP Member
        if (binary.Left is UnaryExpression u4 && u4.NodeType == ExpressionType.Convert && u4.Operand is ConstantExpression c4 && binary.Right is MemberExpression m4)
        {
            try
            {
                var converted = Convert.ChangeType(c4.Value, u4.Type);
                return (m4, new ConstantExpression(converted), ReverseComparisonOperator(binary.NodeType));
            }
            catch { }
        }

        return (null, null, binary.NodeType);
    }

    private static BsonDocument? TryDeserialize(ReadOnlyMemory<byte> slice)
    {
        try { return BsonSerializer.DeserializeDocument(slice); }
        catch { return null; }
    }

    // 构建索引扫描范围
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
            if (lastOp == ComparisonType.LessThan || lastOp == ComparisonType.LessThanOrEqual)
            {
                maxValues.Add(BsonMaxKey.Value);
            }
        }

        return new IndexScanRange
        {
            MinKey = new IndexKey(minValues.ToArray()),
            MaxKey = new IndexKey(maxValues.ToArray()),
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
        return new IndexKey(values.ToArray());
    }
}
