using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using TinyDb.Core;

namespace TinyDb.Query;

/// <summary>
/// 查询优化器 - 负责把 LINQ 表达式解析为查询执行计划。
/// </summary>
public sealed class QueryOptimizer
{
    private readonly ExpressionParser _expressionParser;
    private readonly QueryPlanBuilder _planBuilder;

    /// <summary>
    /// 初始化查询优化器
    /// </summary>
    /// <param name="engine">数据库引擎</param>
    public QueryOptimizer(TinyDbEngine engine)
    {
        if (engine == null) throw new ArgumentNullException(nameof(engine));

        _expressionParser = new ExpressionParser();
        _planBuilder = new QueryPlanBuilder(engine);
    }

    /// <summary>
    /// 为查询创建最优执行计划
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="collectionName">集合名称</param>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询执行计划</returns>
    public QueryExecutionPlan CreateExecutionPlan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(
        string collectionName,
        Expression<Func<T, bool>>? expression = null,
        bool planningMetadataOnly = false)
        where T : class
    {
        if (expression == null)
        {
            return QueryExecutionPlan.FullTableScan(collectionName);
        }

        try
        {
            var queryExpression = RuntimeQueryExpressionBinder.Bind(_expressionParser.Parse(expression));
            return _planBuilder.CreatePlan(collectionName, queryExpression, expression, planningMetadataOnly);
        }
        catch (Exception ex) when (ex is NotSupportedException or InvalidOperationException or ArgumentException)
        {
            return QueryExecutionPlan.FullTableScan(collectionName, expression);
        }
    }

    internal QueryExecutionPlan CreateExecutionPlan(
        string collectionName,
        QueryExpression? queryExpression,
        bool planningMetadataOnly = false)
    {
        queryExpression = RuntimeQueryExpressionBinder.Bind(queryExpression);
        return _planBuilder.CreatePlan(collectionName, queryExpression, originalExpression: null, planningMetadataOnly);
    }

}
