using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using SimpleDb.Bson;
using SimpleDb.Core;
using SimpleDb.Index;
using SimpleDb.Serialization;

namespace SimpleDb.Query;

/// <summary>
/// 查询执行器
/// </summary>
public sealed class QueryExecutor
{
    private readonly SimpleDbEngine _engine;
    private readonly ExpressionParser _expressionParser;
    private readonly QueryOptimizer _queryOptimizer;

    /// <summary>
    /// 初始化查询执行器
    /// </summary>
    /// <param name="engine">数据库引擎</param>
    public QueryExecutor(SimpleDbEngine engine)
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
    public IEnumerable<T> Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
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
            QueryExecutionStrategy.IndexScan => ExecuteIndexScan<T>(executionPlan),
            QueryExecutionStrategy.IndexSeek => ExecuteIndexSeek<T>(executionPlan),
            _ => ExecuteFullTableScan<T>(collectionName, expression)
        };
    }

    /// <summary>
    /// 执行索引扫描
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="executionPlan">执行计划</param>
    /// <returns>查询结果</returns>
    private IEnumerable<T> ExecuteIndexScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExecutionPlan executionPlan)
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

        // 根据文档ID获取完整文档
        foreach (var documentId in documentIds)
        {
            var document = _engine.FindById(executionPlan.CollectionName, documentId);
            if (document != null)
            {
                var entity = AotBsonMapper.FromDocument<T>(document);
                if (entity != null)
                {
                    // 如果有额外的查询条件，需要进一步过滤
                    if (executionPlan.OriginalExpression != null)
                    {
                        var queryExpression = _expressionParser.Parse<T>((Expression<Func<T, bool>>)executionPlan.OriginalExpression);
                        if (EvaluateExpression(queryExpression, entity))
                        {
                            yield return entity;
                        }
                    }
                    else
                    {
                        yield return entity;
                    }
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
    private IEnumerable<T> ExecuteIndexSeek<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExecutionPlan executionPlan)
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

        // 使用索引查找文档ID
        var documentId = index.FindExact(exactKey);
        if (documentId == null)
        {
            yield break; // 没有找到匹配的文档
        }

        // 根据文档ID获取完整文档
        var document = _engine.FindById(executionPlan.CollectionName, documentId);
        if (document != null)
        {
            var entity = AotBsonMapper.FromDocument<T>(document);
            if (entity != null)
            {
                yield return entity;
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
    private IEnumerable<T> ExecuteFullTableScan<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(string collectionName, Expression<Func<T, bool>>? expression = null)
        where T : class, new()
    {
        // 获取所有文档
        var documents = _engine.FindAll(collectionName);

        // 如果没有查询条件，返回所有文档
        if (expression == null)
        {
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

        // 执行查询
        foreach (var document in documents)
        {
            var entity = AotBsonMapper.FromDocument<T>(document);
            if (entity != null && EvaluateExpression(queryExpression, entity))
            {
                yield return entity;
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

        // 根据第一个扫描键构建范围
        var firstKey = executionPlan.IndexScanKeys[0];
        var minKey = new IndexKey(firstKey.Value);
        var maxKey = minKey;

        bool includeMin = firstKey.ComparisonType == ComparisonType.Equal ||
                         firstKey.ComparisonType == ComparisonType.GreaterThanOrEqual ||
                         firstKey.ComparisonType == ComparisonType.LessThanOrEqual;

        bool includeMax = firstKey.ComparisonType == ComparisonType.Equal ||
                         firstKey.ComparisonType == ComparisonType.LessThanOrEqual;

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

    /// <summary>
    /// 评估查询表达式
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="expression">查询表达式</param>
    /// <param name="entity">实体对象</param>
    /// <returns>是否匹配</returns>
    private static bool EvaluateExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExpression expression, T entity)
        where T : class, new()
    {
        return expression switch
        {
            ConstantExpression constExpr => EvaluateConstantExpression(constExpr),
            BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, entity),
            MemberExpression memberExpr => EvaluateMemberExpression(memberExpr, entity),
            ParameterExpression paramExpr => true, // 参数表达式总是返回true
            FunctionExpression funcExpr => EvaluateFunctionExpression(funcExpr, entity),
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported")
        };
    }

    /// <summary>
    /// 评估常量表达式
    /// </summary>
    /// <param name="expression">常量表达式</param>
    /// <returns>评估结果</returns>
    private static bool EvaluateConstantExpression(ConstantExpression expression)
    {
        if (expression.Value is bool boolValue)
        {
            return boolValue;
        }

        throw new InvalidOperationException($"Constant expression must be boolean, got {expression.Value?.GetType().Name}");
    }

    /// <summary>
    /// 评估二元表达式
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="expression">二元表达式</param>
    /// <param name="entity">实体对象</param>
    /// <returns>评估结果</returns>
    private static bool EvaluateBinaryExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(BinaryExpression expression, T entity)
        where T : class, new()
    {
        var leftValue = EvaluateExpressionValue(expression.Left, entity);
        var rightValue = EvaluateExpressionValue(expression.Right, entity);

        return expression.NodeType switch
        {
            ExpressionType.Equal => Equals(leftValue, rightValue),
            ExpressionType.NotEqual => !Equals(leftValue, rightValue),
            ExpressionType.GreaterThan => Compare(leftValue, rightValue) > 0,
            ExpressionType.GreaterThanOrEqual => Compare(leftValue, rightValue) >= 0,
            ExpressionType.LessThan => Compare(leftValue, rightValue) < 0,
            ExpressionType.LessThanOrEqual => Compare(leftValue, rightValue) <= 0,
            ExpressionType.AndAlso => leftValue is bool leftBool && rightValue is bool rightBool && leftBool && rightBool,
            ExpressionType.OrElse => leftValue is bool leftBool2 && rightValue is bool rightBool2 && (leftBool2 || rightBool2),
            _ => throw new NotSupportedException($"Binary operation {expression.NodeType} is not supported")
        };
    }

    /// <summary>
    /// 评估成员表达式
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="expression">成员表达式</param>
    /// <param name="entity">实体对象</param>
    /// <returns>评估结果</returns>
    private static bool EvaluateMemberExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(MemberExpression expression, T entity)
        where T : class, new()
    {
        var value = GetMemberValue(expression, entity);

        if (value is bool boolValue)
        {
            return boolValue;
        }

        // 对于非布尔值，返回true（在查询中通常用于检查存在性）
        return value != null;
    }

    /// <summary>
    /// 评估表达式值
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="expression">表达式</param>
    /// <param name="entity">实体对象</param>
    /// <returns>评估结果</returns>
    private static object? EvaluateExpressionValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExpression expression, T entity)
        where T : class, new()
    {
        return expression switch
        {
            ConstantExpression constExpr => constExpr.Value,
            MemberExpression memberExpr => GetMemberValue(memberExpr, entity),
            ParameterExpression => entity,
            BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, entity),
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported for value evaluation")
        };
    }

    /// <summary>
    /// 获取成员值（使用AotBsonMapper避免反射）
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="expression">成员表达式</param>
    /// <param name="entity">实体对象</param>
    /// <returns>成员值</returns>
    private static object? GetMemberValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(MemberExpression expression, T entity)
        where T : class, new()
    {
        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.GetPropertyValue(entity, expression.MemberName);
        }

        return EntityMetadata<T>.TryGetProperty(expression.MemberName, out var property)
            ? property.GetValue(entity)
            : null;
    }

    /// <summary>
    /// 评估函数表达式
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="expression">函数表达式</param>
    /// <param name="entity">实体对象</param>
    /// <returns>评估结果</returns>
    private static bool EvaluateFunctionExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(FunctionExpression expression, T entity)
        where T : class, new()
    {
        return expression.FunctionName switch
        {
            "Contains" => EvaluateContainsFunction(expression, entity),
            "StartsWith" => EvaluateStartsWithFunction(expression, entity),
            "EndsWith" => EvaluateEndsWithFunction(expression, entity),
            _ => throw new NotSupportedException($"Function '{expression.FunctionName}' is not supported")
        };
    }

    /// <summary>
    /// 评估Contains函数
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="expression">函数表达式</param>
    /// <param name="entity">实体对象</param>
    /// <returns>评估结果</returns>
    private static bool EvaluateContainsFunction<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(FunctionExpression expression, T entity)
        where T : class, new()
    {
        // 获取目标值（左值）
        var targetValue = EvaluateExpressionValue(expression.Target, entity);
        // 获取参数值（右值）
        var argumentValue = EvaluateExpressionValue(expression.Argument, entity);

        // 处理字符串Contains
        if (targetValue is string targetStr && argumentValue is string argStr)
        {
            return targetStr.Contains(argStr);
        }

        // 处理数组/列表Contains
        if (targetValue is System.Collections.IEnumerable targetEnum)
        {
            foreach (var item in targetEnum)
            {
                if (Equals(item, argumentValue))
                {
                    return true;
                }
            }
            return false;
        }

        throw new NotSupportedException($"Contains operation is not supported for types {targetValue?.GetType().Name} and {argumentValue?.GetType().Name}");
    }

    /// <summary>
    /// 评估StartsWith函数
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="expression">函数表达式</param>
    /// <param name="entity">实体对象</param>
    /// <returns>评估结果</returns>
    private static bool EvaluateStartsWithFunction<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(FunctionExpression expression, T entity)
        where T : class, new()
    {
        // 获取目标值（左值）
        var targetValue = EvaluateExpressionValue(expression.Target, entity);
        // 获取参数值（右值）
        var argumentValue = EvaluateExpressionValue(expression.Argument, entity);

        // 处理字符串StartsWith
        if (targetValue is string targetStr && argumentValue is string argStr)
        {
            return targetStr.StartsWith(argStr);
        }

        throw new NotSupportedException($"StartsWith operation is not supported for types {targetValue?.GetType().Name} and {argumentValue?.GetType().Name}");
    }

    /// <summary>
    /// 评估EndsWith函数
    /// </summary>
    /// <typeparam name="T">文档类型</typeparam>
    /// <param name="expression">函数表达式</param>
    /// <param name="entity">实体对象</param>
    /// <returns>评估结果</returns>
    private static bool EvaluateEndsWithFunction<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(FunctionExpression expression, T entity)
        where T : class, new()
    {
        // 获取目标值（左值）
        var targetValue = EvaluateExpressionValue(expression.Target, entity);
        // 获取参数值（右值）
        var argumentValue = EvaluateExpressionValue(expression.Argument, entity);

        // 处理字符串EndsWith
        if (targetValue is string targetStr && argumentValue is string argStr)
        {
            return targetStr.EndsWith(argStr);
        }

        throw new NotSupportedException($"EndsWith operation is not supported for types {targetValue?.GetType().Name} and {argumentValue?.GetType().Name}");
    }


    /// <summary>
    /// 比较两个值
    /// </summary>
    /// <param name="left">左值</param>
    /// <param name="right">右值</param>
    /// <returns>比较结果</returns>
    private static int Compare(object? left, object? right)
    {
        if (left == null && right == null) return 0;
        if (left == null) return -1;
        if (right == null) return 1;

        // 优先处理数值类型，避免 Decimal 与其他数值类型比较时触发运行时异常
        if (IsNumericType(left) && IsNumericType(right))
        {
            if (left is decimal || right is decimal)
            {
                var leftDecimal = Convert.ToDecimal(left);
                var rightDecimal = Convert.ToDecimal(right);
                return leftDecimal.CompareTo(rightDecimal);
            }

            var leftDouble = Convert.ToDouble(left);
            var rightDouble = Convert.ToDouble(right);
            return leftDouble.CompareTo(rightDouble);
        }

        // 尝试转换为可比较的类型
        if (left is IComparable leftComparable)
        {
            try
            {
                return leftComparable.CompareTo(right);
            }
            catch (ArgumentException)
            {
                // 如果类型不兼容，回退到字符串比较
            }
        }

        // 如果都是字符串，进行字符串比较
        if (left is string leftStr && right is string rightStr)
        {
            return string.Compare(leftStr, rightStr, StringComparison.Ordinal);
        }

        // 最后尝试字符串比较
        return string.Compare(left.ToString(), right.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// 检查是否为数值类型
    /// </summary>
    /// <param name="value">值</param>
    /// <returns>是否为数值类型</returns>
    private static bool IsNumericType(object value)
    {
        return value is sbyte || value is byte || value is short || value is ushort ||
               value is int || value is uint || value is long || value is ulong ||
               value is float || value is double || value is decimal;
    }
}

/// <summary>
/// 查询表达式基类
/// </summary>
public abstract class QueryExpression
{
    /// <summary>
    /// 表达式类型
    /// </summary>
    public abstract ExpressionType NodeType { get; }
}

/// <summary>
/// 常量表达式
/// </summary>
public sealed class ConstantExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.Constant;
    public object? Value { get; }

    public ConstantExpression(object? value)
    {
        Value = value;
    }
}

/// <summary>
/// 二元表达式
/// </summary>
public sealed class BinaryExpression : QueryExpression
{
    public override ExpressionType NodeType { get; }
    public QueryExpression Left { get; }
    public QueryExpression Right { get; }

    public BinaryExpression(ExpressionType nodeType, QueryExpression left, QueryExpression right)
    {
        NodeType = nodeType;
        Left = left;
        Right = right;
    }
}

/// <summary>
/// 成员表达式
/// </summary>
public sealed class MemberExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.MemberAccess;
    public string MemberName { get; }
    public QueryExpression? Expression { get; }

    public MemberExpression(string memberName, QueryExpression? expression = null)
    {
        MemberName = memberName;
        Expression = expression;
    }
}

/// <summary>
/// 参数表达式
/// </summary>
public sealed class ParameterExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.Parameter;
    public string Name { get; }

    public ParameterExpression(string name)
    {
        Name = name;
    }
}

/// <summary>
/// 函数表达式（用于处理Contains等字符串方法）
/// </summary>
public sealed class FunctionExpression : QueryExpression
{
    public override ExpressionType NodeType => ExpressionType.Call; // 使用Call类型表示方法调用
    public string FunctionName { get; }
    public QueryExpression Target { get; }
    public QueryExpression Argument { get; }

    public FunctionExpression(string functionName, QueryExpression target, QueryExpression argument)
    {
        FunctionName = functionName;
        Target = target;
        Argument = argument;
    }
}
