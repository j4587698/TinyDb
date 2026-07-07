using System.Linq.Expressions;
using TinyDb.Bson;
using TinyDb.Index;

namespace TinyDb.Query;

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

    public List<QueryExecutionPlan> BranchPlans { get; set; } = new();

    /// <summary>
    /// 估算的执行成本
    /// </summary>
    public double EstimatedCost { get; set; }

    /// <summary>
    /// 估算的结果数量
    /// </summary>
    public long EstimatedResultCount { get; set; }

    internal static QueryExecutionPlan FullTableScan(
        string collectionName,
        LambdaExpression? originalExpression = null,
        QueryExpression? queryExpression = null)
    {
        return new QueryExecutionPlan
        {
            CollectionName = collectionName,
            OriginalExpression = originalExpression,
            QueryExpression = queryExpression,
            Strategy = QueryExecutionStrategy.FullTableScan
        };
    }
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
    PrimaryKeyLookup,

    /// <summary>
    /// OR 分支索引并集
    /// </summary>
    IndexUnion
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
