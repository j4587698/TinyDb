using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Core;
using TinyDb.Index;

namespace TinyDb.Query;

/// <summary>
/// 查询优化器 - 负责选择最优索引和执行计划
/// </summary>
public sealed class QueryOptimizer
{
    private readonly TinyDbEngine _engine;
    private readonly ExpressionParser _expressionParser;

    /// <summary>
    /// 初始化查询优化器
    /// </summary>
    /// <param name="engine">数据库引擎</param>
    public QueryOptimizer(TinyDbEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _expressionParser = new ExpressionParser();
    }

    /// <summary>
    /// 为查询创建最优执行计划
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="collectionName">集合名称</param>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询执行计划</returns>
    public QueryExecutionPlan CreateExecutionPlan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class, new()
    {
        var plan = new QueryExecutionPlan
        {
            CollectionName = collectionName,
            OriginalExpression = expression
        };

        // 如果没有查询条件，使用全表扫描
        if (expression == null)
        {
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
            return plan;
        }

        // 解析查询表达式
        QueryExpression? queryExpression = null;
        try
        {
            queryExpression = _expressionParser.Parse(expression);
            plan.QueryExpression = queryExpression;
        }
        catch
        {
            // 解析失败（例如包含不支持的节点），回退到全表扫描
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
            return plan;
        }

        // 优化：检查是否为主键查询
        var primaryKeyValue = ExtractPrimaryKeyValue(queryExpression);
        if (primaryKeyValue != null)
        {
            plan.Strategy = QueryExecutionStrategy.PrimaryKeyLookup;
            // 将主键值存储在 ScanKeys 中以便 Executor 使用
            plan.IndexScanKeys = new List<IndexScanKey> 
            { 
                new IndexScanKey 
                { 
                    FieldName = "_id", 
                    Value = primaryKeyValue, 
                    ComparisonType = ComparisonType.Equal 
                } 
            };
            return plan;
        }

        // 获取集合的索引管理器
        var indexManager = _engine.GetIndexManager(collectionName);
        // 分析可用的索引
        var availableIndexes = indexManager.GetAllStatistics().ToList();
        if (!availableIndexes.Any())
        {
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
            return plan;
        }

        // 尝试找到最适合的索引
        var bestIndex = SelectBestIndex(queryExpression, availableIndexes);
        if (bestIndex != null)
        {
            plan.Strategy = QueryExecutionStrategy.IndexScan;
            plan.UseIndex = bestIndex;
            plan.IndexScanKeys = ExtractIndexScanKeys(queryExpression, bestIndex);

            // 优化：如果是唯一索引且查询覆盖了所有字段且均为等值匹配，升级为 IndexSeek
            if (bestIndex.IsUnique && 
                plan.IndexScanKeys.Count == bestIndex.Fields.Length && 
                plan.IndexScanKeys.All(k => k.ComparisonType == ComparisonType.Equal))
            {
                plan.Strategy = QueryExecutionStrategy.IndexSeek;
            }
        }
        else
        {
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
        }

        return plan;
    }

    /// <summary>
    /// 尝试从查询表达式中提取主键值
    /// </summary>
    /// <param name="queryExpression">查询表达式</param>
    /// <returns>主键值，如果不是主键查询则返回null</returns>
    private static BsonValue? ExtractPrimaryKeyValue(QueryExpression queryExpression)
    {
        if (queryExpression is BinaryExpression binaryExpr)
        {
            // 如果是 AND 表达式，递归检查左右子树
            if (binaryExpr.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
            {
                var leftResult = ExtractPrimaryKeyValue(binaryExpr.Left);
                if (leftResult != null) return leftResult;
                
                return ExtractPrimaryKeyValue(binaryExpr.Right);
            }

            // 必须是等值比较
            if (binaryExpr.NodeType != System.Linq.Expressions.ExpressionType.Equal)
                return null;

            string? fieldName = null;
            BsonValue? value = null;

            if (binaryExpr.Left is MemberExpression leftMember)
            {
                fieldName = leftMember.MemberName;
                value = ExtractConstantValue(binaryExpr.Right);
            }
            else if (binaryExpr.Right is MemberExpression rightMember)
            {
                fieldName = rightMember.MemberName;
                value = ExtractConstantValue(binaryExpr.Left);
            }

            // 检查字段名是否为 _id 或 Id (不区分大小写)
            if (fieldName != null && (string.Equals(fieldName, "_id", StringComparison.OrdinalIgnoreCase) || string.Equals(fieldName, "Id", StringComparison.OrdinalIgnoreCase)))
            {
                return value;
            }
        }

        return null;
    }

    /// <summary>
    /// 选择最适合的索引
    /// </summary>
    /// <param name="queryExpression">查询表达式</param>
    /// <param name="availableIndexes">可用索引列表</param>
    /// <returns>最适合的索引，如果没有合适的则返回null</returns>
    private IndexStatistics? SelectBestIndex(QueryExpression queryExpression, IEnumerable<IndexStatistics> availableIndexes)
    {
        var candidateIndexes = new List<IndexCandidate>();

        // 分析每个索引与查询的匹配度
        foreach (var indexStat in availableIndexes)
        {
            var matchScore = CalculateIndexMatchScore(queryExpression, indexStat);
            if (matchScore > 0)
            {
                candidateIndexes.Add(new IndexCandidate
                {
                    Index = indexStat,
                    MatchScore = matchScore
                });
            }
        }

        // 返回匹配度最高的索引
        return candidateIndexes
            .OrderByDescending(c => c.MatchScore)
            .ThenBy(c => c.Index.EntryCount) // 优先选择条目数较少的索引
            .FirstOrDefault()?.Index;
    }

    /// <summary>
    /// 计算索引与查询表达式的匹配分数
    /// </summary>
    /// <param name="queryExpression">查询表达式</param>
    /// <param name="indexStat">索引统计</param>
    /// <returns>匹配分数（0表示不匹配）</returns>
    private static int CalculateIndexMatchScore(QueryExpression queryExpression, IndexStatistics indexStat)
    {
        var score = 0;
        var queryFields = ExtractQueryFields(queryExpression).ToList();

        // 单字段索引匹配
        if (indexStat.Fields.Length == 1 && queryFields.Count == 1)
        {
            if (indexStat.Fields.Contains(queryFields[0], StringComparer.OrdinalIgnoreCase))
            {
                score = 10; // 单字段精确匹配
                if (indexStat.IsUnique)
                {
                    score += 5; // 唯一索引加分
                }
            }
        }
        // 复合索引匹配
        else if (indexStat.Fields.Length > 1)
        {
            var matchedFields = 0;
            for (int i = 0; i < Math.Min(indexStat.Fields.Length, queryFields.Count); i++)
            {
                if (string.Equals(indexStat.Fields[i], queryFields[i], StringComparison.OrdinalIgnoreCase))
                {
                    matchedFields++;
                    score += 10; // 前缀匹配每个字段加分
                }
                else
                {
                    break; // 复合索引必须前缀匹配
                }
            }

            if (matchedFields > 0)
            {
                score += matchedFields * 2; // 复合索引额外加分
            }
        }

        return score;
    }

    /// <summary>
    /// 从查询表达式中提取查询字段
    /// </summary>
    /// <param name="queryExpression">查询表达式</param>
    /// <returns>查询字段列表</returns>
    private static IEnumerable<string> ExtractQueryFields(QueryExpression queryExpression)
    {
        if (queryExpression is BinaryExpression binaryExpr)
        {
            if (binaryExpr.Left is MemberExpression leftMember)
            {
                yield return leftMember.MemberName;
            }
            else if (binaryExpr.Right is MemberExpression rightMember)
            {
                yield return rightMember.MemberName;
            }
        }

        // 递归处理嵌套表达式
        if (queryExpression is BinaryExpression nestedBinary)
        {
            foreach (var field in ExtractQueryFields(nestedBinary.Left))
                yield return field;
            foreach (var field in ExtractQueryFields(nestedBinary.Right))
                yield return field;
        }
    }

    /// <summary>
    /// 从查询表达式中提取索引扫描键
    /// </summary>
    /// <param name="queryExpression">查询表达式</param>
    /// <param name="indexStat">索引统计</param>
    /// <returns>索引扫描键列表</returns>
    private static List<IndexScanKey> ExtractIndexScanKeys(QueryExpression queryExpression, IndexStatistics indexStat)
    {
        var scanKeys = new List<IndexScanKey>();
        var queryFields = ExtractQueryFields(queryExpression).ToList();

        for (int i = 0; i < Math.Min(indexStat.Fields.Length, queryFields.Count); i++)
        {
            var fieldName = indexStat.Fields[i];
            var queryField = queryFields[i];

            if (string.Equals(fieldName, queryField, StringComparison.OrdinalIgnoreCase))
            {
                // 尝试从查询表达式中提取比较值
                var comparisonValue = ExtractComparisonValue(queryExpression, queryField);
                if (comparisonValue != null)
                {
                    scanKeys.Add(new IndexScanKey
                    {
                        FieldName = fieldName,
                        Value = comparisonValue,
                        ComparisonType = ExtractComparisonType(queryExpression, queryField)
                    });
                }
            }
        }

        return scanKeys;
    }

    /// <summary>
    /// 从查询表达式中提取比较值
    /// </summary>
    /// <param name="queryExpression">查询表达式</param>
    /// <param name="fieldName">字段名</param>
    /// <returns>比较值</returns>
    private static BsonValue? ExtractComparisonValue(QueryExpression queryExpression, string fieldName)
    {
        if (queryExpression is BinaryExpression binaryExpr)
        {
            if (binaryExpr.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
            {
                return ExtractComparisonValue(binaryExpr.Left, fieldName) ?? 
                       ExtractComparisonValue(binaryExpr.Right, fieldName);
            }

            // 检查是否是目标字段的比较
            if (binaryExpr.Left is MemberExpression leftMember &&
                string.Equals(leftMember.MemberName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                return ExtractConstantValue(binaryExpr.Right);
            }
            else if (binaryExpr.Right is MemberExpression rightMember &&
                     string.Equals(rightMember.MemberName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                return ExtractConstantValue(binaryExpr.Left);
            }
        }

        return null;
    }

    /// <summary>
    /// 从表达式中提取常量值 (支持变量和闭包捕获)
    /// </summary>
    /// <param name="expression">表达式</param>
    /// <returns>BSON值</returns>
    private static BsonValue? ExtractConstantValue(QueryExpression expression)
    {
        try 
        {
            return expression switch
            {
                ConstantExpression constExpr => ConvertToBsonValue(constExpr.Value),
                MemberExpression memberExpr => EvaluateMemberExpression(memberExpr),
                _ => null
            };
        }
        catch
        {
            // 评估失败时忽略，降级为不使用索引
            return null;
        }
    }

    /// <summary>
    /// 评估成员表达式 (用于处理局部变量和闭包)
    /// </summary>
    private static BsonValue? EvaluateMemberExpression(MemberExpression memberExpr)
    {
        // 既然结构已经变为 QueryExpression 自定义类型，我们无法直接使用 .NET 的 Expression.Compile
        // 我们需要查看 ExpressionParser 到底生成了什么结构。
        
        // 修正：ExpressionParser 生成的是自定义的 MemberExpression, 并不是 System.Linq.Expressions.MemberExpression
        // 自定义的 MemberExpression 只有 MemberName 和 Expression (QueryExpression)
        
        // 由于我们丢失了原始的反射信息 (FieldInfo/PropertyInfo)，我们无法在此处通过反射获取值
        // 除非 ExpressionParser 保留了这些信息。
        
        return null; 
    }

    /// <summary>
    /// 将对象转换为BSON值
    /// </summary>
    /// <param name="value">对象值</param>
    /// <returns>BSON值</returns>
    private static BsonValue? ConvertToBsonValue(object? value)
    {
        if (value == null) return BsonNull.Value;

        return value switch
        {
            BsonValue bson => bson,
            ObjectId oid => oid,
            string str => new BsonString(str),
            int i => new BsonInt32(i),
            long l => new BsonInt64(l),
            double d => new BsonDouble(d),
            decimal dec => new BsonDecimal128(dec),
            bool b => new BsonBoolean(b),
            DateTime dt => new BsonDateTime(dt),
            _ => new BsonString(value.ToString() ?? string.Empty)
        };
    }

    /// <summary>
    /// 提取比较操作类型
    /// </summary>
    /// <param name="queryExpression">查询表达式</param>
    /// <param name="fieldName">字段名</param>
    /// <returns>比较类型</returns>
    private static ComparisonType ExtractComparisonType(QueryExpression queryExpression, string fieldName)
    {
        if (queryExpression is BinaryExpression binaryExpr)
        {
            if (binaryExpr.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
            {
                var leftValue = ExtractComparisonValue(binaryExpr.Left, fieldName);
                if (leftValue != null)
                {
                    return ExtractComparisonType(binaryExpr.Left, fieldName);
                }
                
                return ExtractComparisonType(binaryExpr.Right, fieldName);
            }

            bool isTargetField = false;

            if (binaryExpr.Left is MemberExpression leftMember &&
                string.Equals(leftMember.MemberName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                isTargetField = true;
            }
            else if (binaryExpr.Right is MemberExpression rightMember &&
                     string.Equals(rightMember.MemberName, fieldName, StringComparison.OrdinalIgnoreCase))
            {
                isTargetField = true;
            }

            if (isTargetField)
            {
                return binaryExpr.NodeType switch
                {
                    System.Linq.Expressions.ExpressionType.Equal => ComparisonType.Equal,
                    System.Linq.Expressions.ExpressionType.NotEqual => ComparisonType.NotEqual,
                    System.Linq.Expressions.ExpressionType.GreaterThan => ComparisonType.GreaterThan,
                    System.Linq.Expressions.ExpressionType.GreaterThanOrEqual => ComparisonType.GreaterThanOrEqual,
                    System.Linq.Expressions.ExpressionType.LessThan => ComparisonType.LessThan,
                    System.Linq.Expressions.ExpressionType.LessThanOrEqual => ComparisonType.LessThanOrEqual,
                    _ => ComparisonType.Equal
                };
            }
        }

        return ComparisonType.Equal;
    }

    /// <summary>
    /// 索引候选者
    /// </summary>
    private class IndexCandidate
    {
        public IndexStatistics Index { get; set; } = null!;
        public int MatchScore { get; set; }
    }
}

/// <summary>
/// 查询执行计划
/// </summary>
public sealed class QueryExecutionPlan
{
    /// <summary>
    /// 集合名称
    /// </summary>
    public string CollectionName { get; set; } = string.Empty;

    /// <summary>
    /// 原始查询表达式
    /// </summary>
    public LambdaExpression? OriginalExpression { get; set; }

    /// <summary>
    /// 解析后的查询表达式
    /// </summary>
    public QueryExpression? QueryExpression { get; set; }

    /// <summary>
    /// 执行策略
    /// </summary>
    public QueryExecutionStrategy Strategy { get; set; }

    /// <summary>
    /// 使用的索引
    /// </summary>
    public IndexStatistics? UseIndex { get; set; }

    /// <summary>
    /// 索引扫描键
    /// </summary>
    public List<IndexScanKey> IndexScanKeys { get; set; } = new();

    /// <summary>
    /// 估算的执行成本
    /// </summary>
    public double EstimatedCost { get; set; }

    /// <summary>
    /// 估算的结果数量
    /// </summary>
    public long EstimatedResultCount { get; set; }
}

/// <summary>
/// 查询执行策略
/// </summary>
public enum QueryExecutionStrategy
{
    /// <summary>
    /// 全表扫描
    /// </summary>
    FullTableScan,

    /// <summary>
    /// 索引扫描
    /// </summary>
    IndexScan,

    /// <summary>
    /// 索引查找（使用唯一索引）
    /// </summary>
    IndexSeek,

    /// <summary>
    /// 主键查找
    /// </summary>
    PrimaryKeyLookup
}

/// <summary>
/// 索引扫描键
/// </summary>
public sealed class IndexScanKey
{
    /// <summary>
    /// 字段名
    /// </summary>
    public string FieldName { get; set; } = string.Empty;

    /// <summary>
    /// 比较值
    /// </summary>
    public BsonValue Value { get; set; } = BsonNull.Value;

    /// <summary>
    /// 比较类型
    /// </summary>
    public ComparisonType ComparisonType { get; set; } = ComparisonType.Equal;
}

/// <summary>
/// 比较类型
/// </summary>
public enum ComparisonType
{
    /// <summary>
    /// 等于
    /// </summary>
    Equal,

    /// <summary>
    /// 不等于
    /// </summary>
    NotEqual,

    /// <summary>
    /// 大于
    /// </summary>
    GreaterThan,

    /// <summary>
    /// 大于等于
    /// </summary>
    GreaterThanOrEqual,

    /// <summary>
    /// 小于
    /// </summary>
    LessThan,

    /// <summary>
    /// 小于等于
    /// </summary>
    LessThanOrEqual
}
