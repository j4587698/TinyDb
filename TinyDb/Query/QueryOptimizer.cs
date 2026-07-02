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

    private static Dictionary<string, FieldComparison> ExtractComparisonMap(QueryExpression queryExpression)
    {
        var comparisons = new Dictionary<string, FieldComparison>(StringComparer.OrdinalIgnoreCase);
        AddComparisons(queryExpression, comparisons);
        return comparisons;
    }

    private static void AddComparisons(QueryExpression queryExpression, Dictionary<string, FieldComparison> comparisons)
    {
        if (queryExpression is not BinaryExpression binaryExpr) return;

        if (binaryExpr.NodeType == System.Linq.Expressions.ExpressionType.AndAlso)
        {
            AddComparisons(binaryExpr.Left, comparisons);
            AddComparisons(binaryExpr.Right, comparisons);
            return;
        }

        if (!IsComparisonNode(binaryExpr.NodeType)) return;

        if (binaryExpr.Left is MemberExpression leftMember)
        {
            var value = ExtractConstantValue(binaryExpr.Right);
            if (value != null)
            {
                AddComparison(comparisons, leftMember.MemberName, value, ToComparisonType(binaryExpr.NodeType, reversed: false));
            }
            return;
        }

        if (binaryExpr.Right is MemberExpression rightMember)
        {
            var value = ExtractConstantValue(binaryExpr.Left);
            if (value != null)
            {
                AddComparison(comparisons, rightMember.MemberName, value, ToComparisonType(binaryExpr.NodeType, reversed: true));
            }
        }
    }

    private static void AddComparison(
        Dictionary<string, FieldComparison> comparisons,
        string fieldName,
        BsonValue value,
        ComparisonType comparisonType)
    {
        if (!comparisons.TryGetValue(fieldName, out var comparison))
        {
            comparison = new FieldComparison();
            comparisons[fieldName] = comparison;
        }

        comparison.Add(value, comparisonType);
    }

    private static bool IsComparisonNode(System.Linq.Expressions.ExpressionType nodeType)
    {
        return nodeType is System.Linq.Expressions.ExpressionType.Equal
            or System.Linq.Expressions.ExpressionType.NotEqual
            or System.Linq.Expressions.ExpressionType.GreaterThan
            or System.Linq.Expressions.ExpressionType.GreaterThanOrEqual
            or System.Linq.Expressions.ExpressionType.LessThan
            or System.Linq.Expressions.ExpressionType.LessThanOrEqual;
    }

    private static ComparisonType ToComparisonType(System.Linq.Expressions.ExpressionType nodeType, bool reversed)
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

        return nodeType switch
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

    private sealed class FieldComparison
    {
        private BsonValue? _equalValue;
        private BsonValue? _notEqualValue;
        private BsonValue? _lowerValue;
        private BsonValue? _upperValue;
        private bool _includeLower;
        private bool _includeUpper;

        public void Add(BsonValue value, ComparisonType comparisonType)
        {
            switch (comparisonType)
            {
                case ComparisonType.Equal:
                    _equalValue ??= value;
                    break;
                case ComparisonType.NotEqual:
                    _notEqualValue ??= value;
                    break;
                case ComparisonType.GreaterThan:
                    SetLower(value, include: false);
                    break;
                case ComparisonType.GreaterThanOrEqual:
                    SetLower(value, include: true);
                    break;
                case ComparisonType.LessThan:
                    SetUpper(value, include: false);
                    break;
                case ComparisonType.LessThanOrEqual:
                    SetUpper(value, include: true);
                    break;
            }
        }

        public IndexScanKey ToIndexScanKey(string fieldName)
        {
            if (_equalValue != null)
            {
                return new IndexScanKey
                {
                    FieldName = fieldName,
                    Value = _equalValue,
                    ComparisonType = ComparisonType.Equal
                };
            }

            if (_lowerValue != null && _upperValue != null)
            {
                return new IndexScanKey
                {
                    FieldName = fieldName,
                    Value = _lowerValue,
                    ComparisonType = ComparisonType.Range,
                    LowerValue = _lowerValue,
                    UpperValue = _upperValue,
                    IncludeLower = _includeLower,
                    IncludeUpper = _includeUpper
                };
            }

            if (_lowerValue != null)
            {
                return new IndexScanKey
                {
                    FieldName = fieldName,
                    Value = _lowerValue,
                    ComparisonType = _includeLower ? ComparisonType.GreaterThanOrEqual : ComparisonType.GreaterThan
                };
            }

            if (_upperValue != null)
            {
                return new IndexScanKey
                {
                    FieldName = fieldName,
                    Value = _upperValue,
                    ComparisonType = _includeUpper ? ComparisonType.LessThanOrEqual : ComparisonType.LessThan
                };
            }

            return new IndexScanKey
            {
                FieldName = fieldName,
                Value = _notEqualValue ?? BsonNull.Value,
                ComparisonType = ComparisonType.NotEqual
            };
        }

        private void SetLower(BsonValue value, bool include)
        {
            if (_lowerValue == null)
            {
                _lowerValue = value;
                _includeLower = include;
                return;
            }

            var comparison = BsonValueComparer.Compare(value, _lowerValue);
            if (comparison > 0 || (comparison == 0 && _includeLower && !include))
            {
                _lowerValue = value;
                _includeLower = include;
            }
        }

        private void SetUpper(BsonValue value, bool include)
        {
            if (_upperValue == null)
            {
                _upperValue = value;
                _includeUpper = include;
                return;
            }

            var comparison = BsonValueComparer.Compare(value, _upperValue);
            if (comparison < 0 || (comparison == 0 && _includeUpper && !include))
            {
                _upperValue = value;
                _includeUpper = include;
            }
        }
    }

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
        string collectionName, Expression<Func<T, bool>>? expression = null,
        bool planningMetadataOnly = false)
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
        catch (Exception ex) when (ex is NotSupportedException or InvalidOperationException or ArgumentException)
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
        var availableIndexes = planningMetadataOnly
            ? indexManager.GetPlanningStatistics()
            : indexManager.GetAllStatistics();

        // 尝试找到最适合的索引
        var comparisons = ExtractComparisonMap(queryExpression);
        var bestIndex = SelectBestIndex(availableIndexes, comparisons);
        if (bestIndex != null)
        {
            plan.Strategy = QueryExecutionStrategy.IndexScan;
            plan.UseIndex = bestIndex;
            plan.IndexScanKeys = ExtractIndexScanKeys(bestIndex, comparisons);

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

    internal QueryExecutionPlan CreateExecutionPlan(
        string collectionName,
        QueryExpression? queryExpression,
        bool planningMetadataOnly = false)
    {
        var plan = new QueryExecutionPlan
        {
            CollectionName = collectionName,
            QueryExpression = queryExpression
        };

        if (queryExpression == null)
        {
            plan.Strategy = QueryExecutionStrategy.FullTableScan;
            return plan;
        }

        var primaryKeyValue = ExtractPrimaryKeyValue(queryExpression);
        if (primaryKeyValue != null)
        {
            plan.Strategy = QueryExecutionStrategy.PrimaryKeyLookup;
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

        var indexManager = _engine.GetIndexManager(collectionName);
        var availableIndexes = planningMetadataOnly
            ? indexManager.GetPlanningStatistics()
            : indexManager.GetAllStatistics();

        var comparisons = ExtractComparisonMap(queryExpression);
        var bestIndex = SelectBestIndex(availableIndexes, comparisons);
        if (bestIndex != null)
        {
            plan.Strategy = QueryExecutionStrategy.IndexScan;
            plan.UseIndex = bestIndex;
            plan.IndexScanKeys = ExtractIndexScanKeys(bestIndex, comparisons);

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
    private static IndexStatistics? SelectBestIndex(
        IEnumerable<IndexStatistics> availableIndexes,
        IReadOnlyDictionary<string, FieldComparison> comparisons)
    {
        IndexStatistics? bestIndex = null;
        var bestScore = 0;

        // 分析每个索引与查询的匹配度
        foreach (var indexStat in availableIndexes)
        {
            var matchScore = CalculateIndexMatchScoreCore(indexStat, comparisons);
            if (matchScore <= 0)
            {
                continue;
            }

            if (bestIndex == null ||
                matchScore > bestScore ||
                matchScore == bestScore && indexStat.EntryCount < bestIndex.EntryCount)
            {
                bestIndex = indexStat;
                bestScore = matchScore;
            }
        }

        return bestIndex;
    }

    /// <summary>
    /// 计算索引与查询表达式的匹配分数
    /// </summary>
    /// <param name="queryExpression">查询表达式</param>
    /// <param name="indexStat">索引统计</param>
    /// <returns>匹配分数（0表示不匹配）</returns>
    private static int CalculateIndexMatchScoreCore(
        IndexStatistics indexStat,
        IReadOnlyDictionary<string, FieldComparison> comparisons)
    {
        var score = 0;
        // 单字段索引匹配
        if (indexStat.Fields.Length == 1)
        {
            if (comparisons.ContainsKey(indexStat.Fields[0]))
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
            for (int i = 0; i < indexStat.Fields.Length; i++)
            {
                if (comparisons.ContainsKey(indexStat.Fields[i]))
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

    private static int CalculateIndexMatchScore(QueryExpression queryExpression, IndexStatistics indexStat)
    {
        return CalculateIndexMatchScoreCore(indexStat, ExtractComparisonMap(queryExpression));
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
    private static List<IndexScanKey> ExtractIndexScanKeys(
        IndexStatistics indexStat,
        IReadOnlyDictionary<string, FieldComparison> comparisons)
    {
        var scanKeys = new List<IndexScanKey>();

        for (int i = 0; i < indexStat.Fields.Length; i++)
        {
            var fieldName = indexStat.Fields[i];

            if (comparisons.TryGetValue(fieldName, out var comparison))
            {
                scanKeys.Add(comparison.ToIndexScanKey(fieldName));
            }
            else
            {
                break;
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
        catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or InvalidCastException or FormatException or OverflowException or ArgumentException)
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
    /// 范围下界值，仅在 Range 比较时使用
    /// </summary>
    public BsonValue? LowerValue { get; set; }

    /// <summary>
    /// 范围上界值，仅在 Range 比较时使用
    /// </summary>
    public BsonValue? UpperValue { get; set; }

    /// <summary>
    /// 是否包含范围下界
    /// </summary>
    public bool IncludeLower { get; set; } = true;

    /// <summary>
    /// 是否包含范围上界
    /// </summary>
    public bool IncludeUpper { get; set; } = true;

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
    LessThanOrEqual,

    /// <summary>
    /// 同字段范围
    /// </summary>
    Range
}
