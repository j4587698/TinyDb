using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
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
            // 再次验证查询条件（防止非唯一匹配或其他逻辑差异，虽然对于PK通常不需要）
            // 这里为了安全起见，或者如果原表达式有其他条件（目前优化器只支持纯PK查询，所以这里是冗余的，但在复合条件下有用）
            // 但目前的优化器逻辑：ExtractPrimaryKeyValue 只接受单一的 Equal 表达式。
            // 所以 document 一定匹配。
            
            var entity = AotBsonMapper.FromDocument<T>(document);
            if (entity != null)
            {
                yield return entity;
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

        // 使用索引查找文档ID（使用Find以支持非唯一索引返回多个结果）
        var documentIds = index.Find(exactKey);

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
    /// 执行全表扫描
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="collectionName">集合名称</param>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询结果</returns>
    private IEnumerable<T> ExecuteFullTableScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class, new()
    {
        // 如果没有查询条件，返回所有文档
        if (expression == null)
        {
            // 获取所有文档
            var documents = _engine.FindAll(collectionName);
            foreach (var document in documents)
            {
                var entity = AotBsonMapper.FromDocument<T>(document);
                if (entity != null)
                {
                    yield return entity;
                }
            }
            yield break;
        }

        // 解析查询表达式
        var queryExpression = _expressionParser.Parse(expression);

        // 获取所有文档
        var allDocuments = _engine.FindAll(collectionName);

        // 执行查询
        foreach (var document in allDocuments)
        {
            // 优化：直接在 BsonDocument 上评估查询条件，避免不必要的反序列化
            if (ExpressionEvaluator.Evaluate(queryExpression, document))
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
    /// 构建索引扫描范围
    /// </summary>
    /// <param name="executionPlan">执行计划</param>
    /// <returns>索引扫描范围</returns>
    private static IndexScanRange BuildIndexScanRange(QueryExecutionPlan executionPlan)
    {
        if (executionPlan.IndexScanKeys.Count == 0)
        {
            // 全索引扫描
            return new IndexScanRange
            {
                MinKey = IndexKey.MinValue,
                MaxKey = IndexKey.MaxValue,
                IncludeMin = true,
                IncludeMax = true
            };
        }

        // 简单的实现：仅基于第一个条件构建范围
        // 完善的查询引擎应该合并所有针对同一字段的条件
        var firstKey = executionPlan.IndexScanKeys[0];
        
        var minKey = IndexKey.MinValue;
        var maxKey = IndexKey.MaxValue;
        bool includeMin = true;
        bool includeMax = true;

        switch (firstKey.ComparisonType)
        {
            case ComparisonType.Equal:
                minKey = new IndexKey(firstKey.Value);
                maxKey = minKey;
                break;
            case ComparisonType.GreaterThan:
                minKey = new IndexKey(firstKey.Value);
                includeMin = false;
                break;
            case ComparisonType.GreaterThanOrEqual:
                minKey = new IndexKey(firstKey.Value);
                includeMin = true;
                break;
            case ComparisonType.LessThan:
                maxKey = new IndexKey(firstKey.Value);
                includeMax = false;
                break;
            case ComparisonType.LessThanOrEqual:
                maxKey = new IndexKey(firstKey.Value);
                includeMax = true;
                break;
        }

        return new IndexScanRange
        {
            MinKey = minKey,
            MaxKey = maxKey,
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
        var equalKeys = executionPlan.IndexScanKeys
            .Where(k => k.ComparisonType == ComparisonType.Equal)
            .OrderBy(k => k.FieldName)
            .ToList();

        if (equalKeys.Count != executionPlan.IndexScanKeys.Count)
        {
            return null; // 不是所有条件都是等值比较，无法构建精确键
        }

        var values = equalKeys.Select(k => k.Value).ToArray();
        return new IndexKey(values);
    }
}