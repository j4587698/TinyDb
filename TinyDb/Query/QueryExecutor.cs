using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Collections.Concurrent;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;
using TinyDb.Serialization;

namespace TinyDb.Query;

/// <summary>
/// 查询执行器
/// </summary>
public sealed class QueryExecutor
{
    private readonly TinyDbEngine _engine;
    private readonly ExpressionParser _expressionParser;
    private readonly QueryOptimizer _queryOptimizer;

    /// <summary>
    /// 初始化查询执行器
    /// </summary>
    /// <param name="engine">数据库引擎</param>
    public QueryExecutor(TinyDbEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _expressionParser = new ExpressionParser();
        _queryOptimizer = new QueryOptimizer(engine);
    }

    /// <summary>
    /// 执行查询
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="collectionName">集合名称</param>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询结果</returns>
    public IEnumerable<T> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class, new()
    {
        // 立即验证参数，不延迟执行
        if (collectionName == null)
            throw new ArgumentException("Collection name cannot be null", nameof(collectionName));
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("Collection name cannot be null, empty, or whitespace", nameof(collectionName));

        // 创建查询执行计划
        var executionPlan = _queryOptimizer.CreateExecutionPlan(collectionName, expression);

        // 根据执行计划选择查询策略
        return executionPlan.Strategy switch
        {
            QueryExecutionStrategy.PrimaryKeyLookup => ExecutePrimaryKeyLookup<T>(executionPlan),
            QueryExecutionStrategy.IndexScan => ExecuteIndexScan<T>(executionPlan),
            QueryExecutionStrategy.IndexSeek => ExecuteIndexSeek<T>(executionPlan),
            _ => ExecuteFullTableScan<T>(collectionName, expression)
        };
    }

    internal IEnumerable<BsonDocument> ExecuteIndexScanForTests(QueryExecutionPlan executionPlan)
    {
        return ExecuteIndexScan<BsonDocument>(executionPlan);
    }

    internal IEnumerable<T> ExecuteIndexSeekForTests<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan)
        where T : class, new()
    {
        return ExecuteIndexSeek<T>(executionPlan);
    }

    /// <summary>
    /// 执行主键查找
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="executionPlan">执行计划</param>
    /// <returns>查询结果</returns>
    private IEnumerable<T> ExecutePrimaryKeyLookup<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan)
        where T : class, new()
    {
        if (executionPlan.IndexScanKeys.Count == 0)
            yield break;

        var idValue = executionPlan.IndexScanKeys[0].Value;
        var document = _engine.FindById(executionPlan.CollectionName, idValue);

        if (document != null)
        {
            // 必须验证原始表达式，因为可能存在除主键外的其他条件（例如 Id == 1 && Name == "Bob"）
            // 如果表达式为空（不应该发生），默认匹配
            var isMatch = true;
            if (executionPlan.QueryExpression != null)
            {
                isMatch = ExpressionEvaluator.Evaluate(executionPlan.QueryExpression, document);
            }
            else if (executionPlan.OriginalExpression != null)
            {
                // Fallback to original expression if QueryExpression is not available (though it should be)
                // Note: Evaluate(Expression<...>) for documents is not directly available, need QueryExpression
                // But CreateExecutionPlan always sets QueryExpression.
            }

            if (isMatch)
            {
                var entity = AotBsonMapper.FromDocument<T>(document);
                if (entity != null)
                {
                    yield return entity;
                }
            }
        }
    }

    /// <summary>
    /// 执行索引扫描
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="executionPlan">执行计划</param>
    /// <returns>查询结果</returns>
    private IEnumerable<T> ExecuteIndexScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan)
        where T : class, new()
    {
        var indexManager = _engine.GetIndexManager(executionPlan.CollectionName);
        if (indexManager == null || executionPlan.UseIndex == null)
        {
            // 回退到全表扫描
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression))
                yield return item;
            yield break;
        }

        var index = indexManager.GetIndex(executionPlan.UseIndex.Name);
        if (index == null)
        {
            // 回退到全表扫描
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression))
                yield return item;
            yield break;
        }

        // 构建索引扫描范围
        var scanRange = BuildIndexScanRange(executionPlan);

        // 使用索引查找文档ID
        var documentIds = index.FindRange(scanRange.MinKey, scanRange.MaxKey, scanRange.IncludeMin, scanRange.IncludeMax);

        // 预解析查询表达式（如果有）
        QueryExpression? queryExpression = null;
        if (executionPlan.OriginalExpression != null)
        {
            queryExpression = _expressionParser.Parse<T>((Expression<Func<T, bool>>)executionPlan.OriginalExpression);
        }

        // 根据文档ID获取完整文档
        foreach (var documentId in documentIds)
        {
            var document = _engine.FindById(executionPlan.CollectionName, documentId);
            if (document != null)
            {
                // 优化：如果有额外条件，先在 BsonDocument 上评估
                if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, document))
                {
                    continue;
                }

                var entity = AotBsonMapper.FromDocument<T>(document);
                if (entity != null)
                {
                    yield return entity;
                }
            }
        }
    }

    /// <summary>
    /// 执行索引查找（用于唯一索引）
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="executionPlan">执行计划</param>
    /// <returns>查询结果</returns>
    private IEnumerable<T> ExecuteIndexSeek<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(QueryExecutionPlan executionPlan)
        where T : class, new()
    {
        var indexManager = _engine.GetIndexManager(executionPlan.CollectionName);
        if (indexManager == null || executionPlan.UseIndex == null)
        {
            // 回退到全表扫描
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression))
                yield return item;
            yield break;
        }

        var index = indexManager.GetIndex(executionPlan.UseIndex.Name);
        if (index == null)
        {
            // 回退到全表扫描
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression))
                yield return item;
            yield break;
        }

        // 构建精确的索引键
        var exactKey = BuildExactIndexKey(executionPlan);
        if (exactKey == null)
        {
            // 回退到全表扫描
            foreach (var item in ExecuteFullTableScan<T>(executionPlan.CollectionName, (Expression<Func<T, bool>>?)executionPlan.OriginalExpression))
                yield return item;
            yield break;
        }

        // 预解析查询表达式（如果有）
        QueryExpression? queryExpression = null;
        if (executionPlan.OriginalExpression != null)
        {
            queryExpression = _expressionParser.Parse<T>((Expression<Func<T, bool>>)executionPlan.OriginalExpression);
        }

        // 优化：对于唯一索引，使用 FindExact 直接获取单一结果，避免返回 List 的开销
        if (index.IsUnique)
        {
            var documentId = index.FindExact(exactKey);
            if (documentId != null)
            {
                var document = _engine.FindById(executionPlan.CollectionName, documentId);
                if (document != null)
                {
                    // 如果有额外条件，先在 BsonDocument 上评估
                    if (queryExpression == null || ExpressionEvaluator.Evaluate(queryExpression, document))
                    {
                        var entity = AotBsonMapper.FromDocument<T>(document);
                        if (entity != null)
                        {
                            yield return entity;
                        }
                    }
                }
            }
            yield break;
        }

        // 非唯一索引：使用 Find 以支持返回多个结果
        var documentIds = index.Find(exactKey);

        // 根据文档ID获取完整文档
        foreach (var documentId in documentIds)
        {
            var document = _engine.FindById(executionPlan.CollectionName, documentId);
            if (document != null)
            {
                // 优化：如果有额外条件，先在 BsonDocument 上评估
                if (queryExpression != null && !ExpressionEvaluator.Evaluate(queryExpression, document))
                {
                    continue;
                }

                var entity = AotBsonMapper.FromDocument<T>(document);
                if (entity != null)
                {
                    yield return entity;
                }
            }
        }
    }

    /// <summary>
    /// 执行全表扫描
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="collectionName">集合名称</param>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询结果</returns>
    private IEnumerable<T> ExecuteFullTableScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class, new()
    {
        var tx = _engine.GetCurrentTransaction();
        QueryExpression? queryExpression = null;

        if (expression != null)
        {
            try
            {
                queryExpression = _expressionParser.Parse(expression);
            }
            catch (Exception ex)
            {
                // 解析失败（例如包含不支持的方法），回退到内存过滤
                throw new NotSupportedException("不支持的查询表达式：AOT-only 模式下不再提供运行时编译回退。", ex);
            }
        }

        // 1. 准备事务覆盖层
        ConcurrentDictionary<string, BsonDocument?>? txOverlay = null;
        if (tx != null)
        {
            txOverlay = new ConcurrentDictionary<string, BsonDocument?>();
            foreach (var op in tx.Operations)
            {
                if (op.CollectionName != collectionName) continue;
                var k = op.DocumentId?.ToString() ?? "";
                if (op.OperationType == TransactionOperationType.Delete)
                {
                    txOverlay[k] = null;
                }
                else if (op.NewDocument != null)
                {
                    txOverlay[k] = op.NewDocument;
                }
            }
        }

        // 2. 构建处理管道
        var rawPipeline = _engine.FindAllRaw(collectionName)
            // 2.1 并行解析
            .Select(slice => BsonSerializer.DeserializeDocument(slice))
            // 2.2 过滤集合 (处理共享页面)
            .Where(doc => !doc.TryGetValue("_collection", out var c) || c.ToString() == collectionName)
            // 2.3 解析大文档
            .Select(doc => _engine.ResolveLargeDocument(doc));

        // 2.4 应用事务覆盖
        IEnumerable<BsonDocument> docs;
        
        if (txOverlay != null && !txOverlay.IsEmpty)
        {
            docs = rawPipeline
                .Select(doc => {
                    var id = doc["_id"].ToString();
                    if (txOverlay.TryRemove(id, out var txDoc))
                    {
                        Console.WriteLine($"[DEBUG] Replaced {id} with tx version");
                        return txDoc; 
                    }
                    Console.WriteLine($"[DEBUG] Using disk version {id}: {doc}");
                    return doc; 
                })
                .Where(doc => doc != null)
                .Select(doc => doc!)
                .Concat(Enumerable.Range(0, 1).SelectMany(_ => txOverlay.Values).Where(v => v != null).Select(v => v!)); // 补充仅在事务中存在的新增文档
        }
        else
        {
            docs = rawPipeline;
        }

        // 3. 评估与映射 (继续并行化)
        var result = docs
            //.AsParallel() 
            .Where(doc => queryExpression == null || ExpressionEvaluator.Evaluate(queryExpression, doc!))
            .Select(doc => AotBsonMapper.FromDocument<T>(doc!))
            .Where(entity => entity != null)!;

        // 4. 如果 BSON 解析失败，应用内存过滤
        return result;
    }

    /// <summary>
    /// 构建索引扫描范围
    /// </summary>
    /// <param name="executionPlan">执行计划</param>
    /// <returns>索引扫描范围</returns>
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

    /// <summary>
    /// 构建精确索引键
    /// </summary>
    /// <param name="executionPlan">执行计划</param>
    /// <returns>精确索引键</returns>
    private static IndexKey? BuildExactIndexKey(QueryExecutionPlan executionPlan)
    {
        if (executionPlan.IndexScanKeys.Count == 0)
        {
            return null;
        }

        // 只有等值比较才能构建精确键
        // keys 已经在 QueryOptimizer 中按索引字段顺序提取
        var values = new List<BsonValue>();
        foreach (var key in executionPlan.IndexScanKeys)
        {
            if (key.ComparisonType != ComparisonType.Equal) return null;
            values.Add(key.Value);
        }

        return new IndexKey(values.ToArray());
    }
}
