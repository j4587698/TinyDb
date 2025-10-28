using System.Collections;
using System.Linq.Expressions;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using SimpleDb.Query;

namespace SimpleDb.Query;

/// <summary>
/// SimpleDb 可查询对象实现
/// </summary>
/// <typeparam name="T">元素类型</typeparam>
[RequiresDynamicCode("LINQ query expressions require dynamic code generation")]
public sealed class Queryable<T> : IQueryable<T>
    where T : class
{
    private readonly QueryExecutor _executor;
    private readonly string _collectionName;
    private readonly Expression? _expression;

    /// <summary>
    /// 获取元素类型
    /// </summary>
    public Type ElementType => typeof(T);

    /// <summary>
    /// 获取表达式
    /// </summary>
    public Expression Expression { get; }

    /// <summary>
    /// 获取查询提供程序
    /// </summary>
    public IQueryProvider Provider { get; }

    /// <summary>
    /// 初始化可查询对象
    /// </summary>
    /// <param name="executor">查询执行器</param>
    /// <param name="collectionName">集合名称</param>
    /// <param name="expression">查询表达式</param>
    public Queryable(QueryExecutor executor, string collectionName, Expression? expression = null)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentNullException(nameof(collectionName));
        _collectionName = collectionName;
        _expression = expression;

        // 创建表达式树
        if (expression == null)
        {
            // 创建根表达式：collection.AsQueryable()
            Expression = Expression.Constant(this);
        }
        else
        {
            Expression = expression;
        }

        Provider = new QueryProvider<T>(_executor, _collectionName);
    }

    /// <summary>
    /// 获取枚举器
    /// </summary>
    /// <returns>枚举器</returns>
    public IEnumerator<T> GetEnumerator()
    {
        // 简化处理：对于复杂查询，直接返回所有数据并在内存中过滤
        try
        {
            var allData = _executor.Execute<T>(_collectionName).ToList();

            // 尝试在内存中应用Where条件
            if (Expression is MethodCallExpression methodCall && methodCall.Method.Name == "Where")
            {
                var whereExpression = methodCall.Arguments[1];
                if (whereExpression is LambdaExpression lambda)
                {
                    var compiled = lambda.Compile();
                    var filtered = allData.Where(item => (bool)compiled.DynamicInvoke(item)!);
                    return filtered.GetEnumerator();
                }
            }

            return allData.GetEnumerator();
        }
        catch
        {
            // 如果失败，返回所有数据
            return _executor.Execute<T>(_collectionName).GetEnumerator();
        }
    }

    /// <summary>
    /// 获取枚举器
    /// </summary>
    /// <returns>枚举器</returns>
    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }

    /// <summary>
    /// 添加 OrderBy 条件
    /// </summary>
    /// <param name="keySelector">键选择器</param>
    /// <returns>排序后的查询</returns>
    public IOrderedQueryable<T> OrderBy<TKey>(Expression<Func<T, TKey>> keySelector)
    {
        if (keySelector == null) throw new ArgumentNullException(nameof(keySelector));

        // 直接在内存中执行排序，返回一个包装了排序结果的IOrderedQueryable
        var allData = _executor.Execute<T>(_collectionName).ToList();
        var compiledKeySelector = keySelector.Compile();
        var sortedData = allData.OrderBy(compiledKeySelector);

        // 创建一个新的查询提供者，它直接返回排序后的数据
        var sortedProvider = new SortedQueryProvider<T>(sortedData);
        return new SortedQueryable<T>(sortedProvider);
    }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        return $"Queryable<{typeof(T).Name}>[{_collectionName}]";
    }
}

/// <summary>
/// SimpleDb 查询提供程序
/// </summary>
/// <typeparam name="T">元素类型</typeparam>
public sealed class QueryProvider<T> : IQueryProvider
    where T : class
{
    private readonly QueryExecutor _executor;
    private readonly string _collectionName;

    /// <summary>
    /// 初始化查询提供程序
    /// </summary>
    /// <param name="executor">查询执行器</param>
    /// <param name="collectionName">集合名称</param>
    public QueryProvider(QueryExecutor executor, string collectionName)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        _collectionName = collectionName ?? throw new ArgumentNullException(nameof(collectionName));
    }

    /// <summary>
    /// 创建查询
    /// </summary>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询结果</returns>
    public IQueryable CreateQuery(Expression expression)
    {
        var elementType = expression.Type.GetGenericArguments()[0];

        if (!elementType.IsClass)
            throw new ArgumentException("Only reference types are supported");

        // 直接创建Queryable实例，避免无限递归
        var queryableType = typeof(Queryable<>).MakeGenericType(elementType);
        var constructor = queryableType.GetConstructor(new[] { typeof(QueryExecutor), typeof(string), typeof(Expression) });

        if (constructor == null)
            throw new InvalidOperationException($"Cannot find suitable constructor for Queryable<{elementType.Name}>");

        return (IQueryable)constructor.Invoke(new object[] { _executor, _collectionName, expression })!;
    }

    /// <summary>
    /// 创建查询
    /// </summary>
    /// <typeparam name="TElement">元素类型</typeparam>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询结果</returns>
    [RequiresDynamicCode("Generic query creation requires dynamic code generation")]
    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        where TElement : class
    {
        return new Queryable<TElement>(_executor, _collectionName, expression);
    }

    /// <summary>
    /// 显式接口实现 - 创建查询
    /// </summary>
    /// <typeparam name="TElement">元素类型</typeparam>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询结果</returns>
    IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
    {
        if (!typeof(TElement).IsClass)
            throw new ArgumentException("TElement must be a reference type");

        var queryableType = typeof(Queryable<>).MakeGenericType(typeof(TElement));
        return (IQueryable<TElement>)Activator.CreateInstance(queryableType, _executor, _collectionName, expression)!;
    }

    /// <summary>
    /// 执行查询
    /// </summary>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询结果</returns>
    public object? Execute(Expression expression)
    {
        // 解析表达式并执行
        var translator = new QueryTranslator();
        var translatedExpression = translator.Translate(expression);

        // 执行查询
        return ExecuteInternal(expression);
    }

    /// <summary>
    /// 执行查询
    /// </summary>
    /// <typeparam name="TResult">结果类型</typeparam>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询结果</returns>
    public TResult Execute<TResult>(Expression expression)
    {
        return (TResult)ExecuteInternal(expression)!;
    }

    /// <summary>
    /// 内部执行方法
    /// </summary>
    /// <param name="expression">查询表达式</param>
    /// <returns>查询结果</returns>
    private object? ExecuteInternal(Expression expression)
    {
        // 如果是根查询，直接返回所有元素
        if (expression is System.Linq.Expressions.ConstantExpression constant && constant.Value is IQueryable)
        {
            return _executor.Execute<T>(_collectionName);
        }

        // 解析 Count/LongCount 调用
        if (TryExtractCountExpression(expression, out var whereExpression))
        {
            var data = GetFilteredData(whereExpression);
            // 检查是Count还是LongCount
            if (expression is System.Linq.Expressions.MethodCallExpression methodCall)
            {
                return methodCall.Method.Name == "LongCount" ? (object)(long)data.Count() : (object)data.Count();
            }
            return data.Count();
        }

        // 解析 First/FirstOrDefault 调用
        if (TryExtractFirstExpression(expression, out whereExpression))
        {
            var data = GetFilteredData(whereExpression);
            return data.FirstOrDefault();
        }

        // 解析 Any/Exists 调用
        if (TryExtractAnyExpression(expression, out whereExpression))
        {
            var data = GetFilteredData(whereExpression);
            return data.Any();
        }

        // 解析链式Where表达式 - 优先处理链式调用
        if (IsChainedWhereExpression(expression))
        {
            var filteredData = GetFilteredData(expression);
            return filteredData;
        }

        // 解析 Where 表达式 - 检查是否是直接的Where调用（来自Find方法）
        if (TryExtractWhereExpression(expression, out whereExpression))
        {
            // 如果这是根级别的Where表达式（如来自Find方法的调用），使用数据库查询
            var isRootWhere = IsRootWhereExpression(expression);

            if (isRootWhere && whereExpression is LambdaExpression lambda)
            {
                return _executor.Execute(_collectionName, (Expression<Func<T, bool>>)lambda);
            }
        }

        // 解析 ToList() 调用
        if (IsToListCall(expression))
        {
            // 提取源表达式并执行
            var sourceExpression = GetSourceExpression(expression);
            if (sourceExpression != null)
            {
                // 对于ToList()，直接使用GetFilteredData处理Where链
                var filteredData = GetFilteredData(sourceExpression);
                return filteredData.ToList();
            }
        }

        // 解析 OrderBy 调用
        if (TryExtractOrderByExpression(expression, out var orderBySourceExpression, out var keySelector))
        {
            var sourceResult = ExecuteInternal(orderBySourceExpression);
            if (sourceResult is IEnumerable<T> enumerable && keySelector != null)
            {
                var compiledKeySelector = keySelector.Compile();
                return enumerable.OrderBy<T, object>(x => compiledKeySelector.DynamicInvoke(x)!);
            }
        }

        // 检查是否是链式Where表达式
        if (IsChainedWhereExpression(expression))
        {
            var filteredData = GetFilteredData(expression);
            return filteredData;
        }

        // 对于其他操作，尝试在内存中执行
        return ExecuteInMemory(expression);
    }

    /// <summary>
    /// 获取过滤后的数据
    /// </summary>
    /// <param name="expression">查询表达式</param>
    /// <returns>过滤后的数据</returns>
    private IEnumerable<T> GetFilteredData(Expression? expression)
    {
        var allData = _executor.Execute<T>(_collectionName);

        if (expression == null)
        {
            return allData;
        }

        // 提取所有Where条件并组合它们
        var wherePredicates = ExtractAllWherePredicates(expression);

        if (wherePredicates.Count == 0)
        {
            // 如果没有找到Where谓词，尝试直接编译表达式
            if (expression is LambdaExpression lambda)
            {
                var compiledPredicate = (Func<T, bool>)lambda.Compile();
                return allData.Where(compiledPredicate);
            }
            return allData;
        }

        // 编译所有谓词并依次应用
        var result = allData;
        foreach (var predicate in wherePredicates)
        {
            var compiledPredicate = (Func<T, bool>)predicate.Compile();
            result = result.Where(compiledPredicate);
        }

        return result;
    }

    /// <summary>
    /// 检查是否为链式Where表达式
    /// </summary>
    /// <param name="expression">表达式</param>
    /// <returns>是否为链式Where调用</returns>
    private bool IsChainedWhereExpression(Expression expression)
    {
        if (expression is MethodCallExpression methodCall && methodCall.Method.Name == "Where")
        {
            // 检查源表达式是否也是Where调用
            if (methodCall.Arguments.Count > 0)
            {
                var sourceExpression = methodCall.Arguments[0];
                return sourceExpression is MethodCallExpression sourceCall &&
                       sourceCall.Method.Name == "Where";
            }
        }
        return false;
    }

    /// <summary>
    /// 提取所有Where谓词表达式
    /// </summary>
    /// <param name="expression">表达式</param>
    /// <returns>所有Where谓词的列表，按调用顺序排列</returns>
    private IList<LambdaExpression> ExtractAllWherePredicates(Expression expression)
    {
        var predicates = new List<LambdaExpression>();
        ExtractWherePredicatesRecursive(expression, predicates);
        return predicates;
    }

    /// <summary>
    /// 递归提取Where谓词
    /// </summary>
    /// <param name="expression">当前表达式</param>
    /// <param name="predicates">谓词列表</param>
    private void ExtractWherePredicatesRecursive(Expression expression, IList<LambdaExpression> predicates)
    {
        if (expression is MethodCallExpression methodCall && methodCall.Method.Name == "Where")
        {
            // Where(source, predicate) -> 提取 predicate
            if (methodCall.Arguments.Count == 2)
            {
                var quote = methodCall.Arguments[1] as UnaryExpression;
                if (quote?.NodeType == ExpressionType.Quote && quote.Operand is LambdaExpression lambda)
                {
                    // 将谓词添加到列表前面，保持正确的调用顺序
                    predicates.Insert(0, lambda);
                }
            }

            // 递归检查源表达式
            if (methodCall.Arguments.Count > 0)
            {
                ExtractWherePredicatesRecursive(methodCall.Arguments[0], predicates);
            }
        }
    }

    /// <summary>
    /// 尝试提取 Count 表达式
    /// </summary>
    private static bool TryExtractCountExpression(Expression expression, out Expression? whereExpression)
    {
        whereExpression = null;

        if (expression is MethodCallExpression methodCall)
        {
            if (methodCall.Method.Name == "LongCount" || methodCall.Method.Name == "Count")
            {
                if (methodCall.Arguments.Count == 2)
                {
                    // 直接的 Count(source, predicate) 调用
                    whereExpression = ExtractLambdaFromQuote(methodCall.Arguments[1]);
                    return true;
                }
                else if (methodCall.Arguments.Count == 1)
                {
                    // 可能是 Where().Count() 的链式调用
                    if (TryExtractWhereExpression(methodCall.Arguments[0], out whereExpression))
                    {
                        return true;
                    }
                    else
                    {
                        whereExpression = null;
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// <summary>
    /// 尝试提取 First/FirstOrDefault 表达式
    /// </summary>
    private static bool TryExtractFirstExpression(Expression expression, out Expression? whereExpression)
    {
        whereExpression = null;

        if (expression is MethodCallExpression methodCall)
        {
            if (methodCall.Method.Name == "FirstOrDefault" || methodCall.Method.Name == "First")
            {
                if (methodCall.Arguments.Count == 2)
                {
                    // 直接的 First(source, predicate) 调用
                    whereExpression = ExtractLambdaFromQuote(methodCall.Arguments[1]);
                    return true;
                }
                else if (methodCall.Arguments.Count == 1)
                {
                    // 可能是 Where().First() 的链式调用
                    if (TryExtractWhereExpression(methodCall.Arguments[0], out whereExpression))
                    {
                        return true;
                    }
                    else
                    {
                        whereExpression = null;
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// <summary>
    /// 尝试提取 Any 表达式
    /// </summary>
    private static bool TryExtractAnyExpression(Expression expression, out Expression? whereExpression)
    {
        whereExpression = null;

        if (expression is MethodCallExpression methodCall)
        {
            if (methodCall.Method.Name == "Any")
            {
                if (methodCall.Arguments.Count == 2)
                {
                    // 直接的 Any(source, predicate) 调用
                    whereExpression = ExtractLambdaFromQuote(methodCall.Arguments[1]);
                    return true;
                }
                else if (methodCall.Arguments.Count == 1)
                {
                    // 可能是 Where().Any() 的链式调用
                    if (TryExtractWhereExpression(methodCall.Arguments[0], out whereExpression))
                    {
                        return true;
                    }
                    else
                    {
                        whereExpression = null;
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /// <summary>
    /// 尝试提取 OrderBy 表达式
    /// </summary>
    private static bool TryExtractOrderByExpression(Expression expression, out Expression? sourceExpression, out LambdaExpression? keySelector)
    {
        sourceExpression = null;
        keySelector = null;

        if (expression is MethodCallExpression methodCall)
        {
            if (methodCall.Method.Name == "OrderBy")
            {
                sourceExpression = methodCall.Arguments[0];
                keySelector = ExtractLambdaFromQuote(methodCall.Arguments[1]) as LambdaExpression;
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// 从 Quote 表达式中提取 Lambda 表达式
    /// </summary>
    private static Expression ExtractLambdaFromQuote(Expression expression)
    {
        if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Quote)
        {
            return unary.Operand;
        }
        return expression;
    }

    /// <summary>
    /// 检查是否是根级别的Where表达式（来自Find方法的直接调用）
    /// </summary>
    private static bool IsRootWhereExpression(Expression expression)
    {
        if (expression is MethodCallExpression methodCall && methodCall.Method.Name == "Where")
        {
            // 检查Where的源是否是ConstantExpression（根查询）
            if (methodCall.Arguments.Count > 0)
            {
                var sourceExpression = methodCall.Arguments[0];
                if (sourceExpression is ConstantExpression)
                {
                    return true;
                }
            }
        }
        return false;
    }

    /// <summary>
    /// 尝试提取 Where 表达式
    /// </summary>
    /// <param name="expression">查询表达式</param>
    /// <param name="whereExpression">提取的 Where 表达式</param>
    /// <returns>是否成功提取</returns>
    private static bool TryExtractWhereExpression(Expression expression, out Expression whereExpression)
    {
        whereExpression = null!;

        // 查找 MethodCallExpression
        if (expression is MethodCallExpression methodCall)
        {
            // 检查是否是 Where 方法
            if (methodCall.Method.Name == "Where")
            {
                // Where(source, predicate) -> 提取 predicate
                if (methodCall.Arguments.Count == 2)
                {
                    var quote = methodCall.Arguments[1] as UnaryExpression;
                    if (quote?.NodeType == ExpressionType.Quote)
                    {
                        whereExpression = quote.Operand;
                        return true;
                    }
                }
            }

            // 递归检查源表达式
            if (methodCall.Arguments.Count > 0)
            {
                return TryExtractWhereExpression(methodCall.Arguments[0], out whereExpression);
            }
        }

        return false;
    }

    /// <summary>
    /// 检查是否为 ToList() 调用
    /// </summary>
    private static bool IsToListCall(Expression expression)
    {
        if (expression is MethodCallExpression methodCall)
        {
            return methodCall.Method.Name == "ToList" &&
                   (methodCall.Method.DeclaringType == typeof(System.Linq.Enumerable) ||
                    methodCall.Method.DeclaringType == typeof(System.Linq.Queryable));
        }
        return false;
    }

    /// <summary>
    /// 获取 ToList() 调用的源表达式
    /// </summary>
    private static Expression? GetSourceExpression(Expression expression)
    {
        if (expression is MethodCallExpression methodCall &&
            methodCall.Method.Name == "ToList" &&
            methodCall.Arguments.Count > 0)
        {
            return methodCall.Arguments[0];
        }
        return null;
    }

    /// <summary>
    /// 在内存中执行查询
    /// </summary>
    [RequiresDynamicCode("In-memory query execution requires dynamic code generation")]
    private object? ExecuteInMemory(Expression expression)
    {
        // 获取所有数据
        var allData = _executor.Execute<T>(_collectionName).ToList();

        try
        {
            // 使用编译后的Lambda表达式执行查询，避免创建新的Queryable
            if (expression is LambdaExpression lambda)
            {
                var compiled = lambda.Compile();
                var result = compiled.DynamicInvoke(allData);
                return result;
            }

            // 对于方法调用表达式，需要特殊处理
            if (expression is MethodCallExpression methodCall)
            {
                // 处理简单的操作
                switch (methodCall.Method.Name)
                {
                    case "Count":
                        return allData.Count;
                    case "LongCount":
                        return (long)allData.Count;
                    case "First":
                    case "FirstOrDefault":
                        return allData.FirstOrDefault();
                    case "Any":
                        return allData.Any();
                    default:
                        // 对于其他复杂操作，返回所有数据
                        return allData;
                }
            }

            // 默认返回所有数据
            return allData;
        }
        catch
        {
            // 如果失败，返回所有数据
            return allData;
        }
    }
}

/// <summary>
/// 查询翻译器
/// </summary>
internal sealed class QueryTranslator : ExpressionVisitor
{
    /// <summary>
    /// 翻译表达式
    /// </summary>
    /// <param name="expression">要翻译的表达式</param>
    /// <returns>翻译后的表达式</returns>
    public Expression Translate(Expression expression)
    {
        return Visit(expression);
    }

    /// <summary>
    /// 访问方法调用表达式
    /// </summary>
    /// <param name="node">方法调用表达式</param>
    /// <returns>访问后的表达式</returns>
    protected override Expression VisitMethodCall(MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(System.Linq.Queryable))
        {
            // 处理 Queryable 方法
            switch (node.Method.Name)
            {
                case "Where":
                    return TranslateWhere(node);
                case "Select":
                    return TranslateSelect(node);
                case "OrderBy":
                    return TranslateOrderBy(node);
                case "OrderByDescending":
                    return TranslateOrderByDescending(node);
                case "FirstOrDefault":
                case "First":
                case "SingleOrDefault":
                case "Single":
                case "Count":
                case "LongCount":
                case "Any":
                case "All":
                    return TranslateScalarMethod(node);
                default:
                    throw new NotSupportedException($"Queryable method '{node.Method.Name}' is not supported");
            }
        }

        return base.VisitMethodCall(node);
    }

    /// <summary>
    /// 翻译 Where 方法
    /// </summary>
    /// <param name="node">Where 方法调用表达式</param>
    /// <returns>翻译后的表达式</returns>
    private Expression TranslateWhere(MethodCallExpression node)
    {
        if (node.Arguments.Count != 2)
            throw new InvalidOperationException("Where method must have exactly 2 arguments");

        var source = Visit(node.Arguments[0]);
        var predicate = Visit(node.Arguments[1]);

        // 使用具体的方法信息而不是字符串
        var whereMethod = typeof(System.Linq.Queryable).GetMethod("Where", new[] { typeof(IQueryable), typeof(Expression) })
            .MakeGenericMethod(node.Method.GetGenericArguments()[0]);

        return Expression.Call(whereMethod, source, predicate);
    }

    /// <summary>
    /// 翻译 Select 方法
    /// </summary>
    /// <param name="node">Select 方法调用表达式</param>
    /// <returns>翻译后的表达式</returns>
    private Expression TranslateSelect(MethodCallExpression node)
    {
        if (node.Arguments.Count != 2)
            throw new InvalidOperationException("Select method must have exactly 2 arguments");

        var source = Visit(node.Arguments[0]);
        var selector = Visit(node.Arguments[1]);

        // 使用具体的方法信息而不是字符串
        var selectMethod = typeof(System.Linq.Queryable).GetMethod("Select", new[] { typeof(IQueryable), typeof(Expression) })
            .MakeGenericMethod(node.Method.GetGenericArguments()[0], node.Method.GetGenericArguments()[1]);

        return Expression.Call(selectMethod, source, selector);
    }

    /// <summary>
    /// 翻译 OrderBy 方法
    /// </summary>
    /// <param name="node">OrderBy 方法调用表达式</param>
    /// <returns>翻译后的表达式</returns>
    private Expression TranslateOrderBy(MethodCallExpression node)
    {
        if (node.Arguments.Count != 2)
            throw new InvalidOperationException("OrderBy method must have exactly 2 arguments");

        var source = Visit(node.Arguments[0]);
        var keySelector = Visit(node.Arguments[1]);

        // 使用具体的方法信息而不是字符串
        var orderByMethod = typeof(System.Linq.Queryable).GetMethod("OrderBy", new[] { typeof(IQueryable), typeof(Expression) })
            .MakeGenericMethod(node.Method.GetGenericArguments()[0]);

        return Expression.Call(orderByMethod, source, keySelector);
    }

    /// <summary>
    /// 翻译 OrderByDescending 方法
    /// </summary>
    /// <param name="node">OrderByDescending 方法调用表达式</param>
    /// <returns>翻译后的表达式</returns>
    private Expression TranslateOrderByDescending(MethodCallExpression node)
    {
        if (node.Arguments.Count != 2)
            throw new InvalidOperationException("OrderByDescending method must have exactly 2 arguments");

        var source = Visit(node.Arguments[0]);
        var keySelector = Visit(node.Arguments[1]);

        return Expression.Call(typeof(System.Linq.Queryable), "OrderByDescending", new[] { node.Method.GetGenericArguments()[0] }, source, keySelector);
    }

    /// <summary>
    /// 翻译标量方法（First, Single, Count 等）
    /// </summary>
    /// <param name="node">方法调用表达式</param>
    /// <returns>翻译后的表达式</returns>
    private Expression TranslateScalarMethod(MethodCallExpression node)
    {
        var source = Visit(node.Arguments[0]);
        var arguments = new List<Expression> { source };

        // 添加谓词参数（如果有）
        if (node.Arguments.Count > 1)
        {
            arguments.Add(Visit(node.Arguments[1]));
        }

        return Expression.Call(typeof(System.Linq.Queryable), node.Method.Name, node.Method.GetGenericArguments(), arguments.ToArray());
    }
}

/// <summary>
/// 参数替换访问器，用于替换表达式树中的参数引用
/// </summary>
internal class ParameterReplacementVisitor : System.Linq.Expressions.ExpressionVisitor
{
    private readonly System.Linq.Expressions.ParameterExpression _parameter;

    public ParameterReplacementVisitor(System.Linq.Expressions.Expression parameter)
    {
        _parameter = (System.Linq.Expressions.ParameterExpression)parameter;
    }

    protected override System.Linq.Expressions.Expression VisitParameter(System.Linq.Expressions.ParameterExpression node)
    {
        // 替换所有参数引用为我们的参数
        return _parameter;
    }
}

/// <summary>
/// 已排序查询提供者
/// </summary>
/// <typeparam name="T">元素类型</typeparam>
internal sealed class SortedQueryProvider<T> : IQueryProvider
    where T : class
{
    internal readonly IEnumerable<T> _sortedData;

    public SortedQueryProvider(IEnumerable<T> sortedData)
    {
        _sortedData = sortedData ?? throw new ArgumentNullException(nameof(sortedData));
    }

    public IQueryable CreateQuery(Expression expression)
    {
        throw new NotSupportedException("Sorted query provider does not support creating new queries");
    }

    public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    {
        throw new NotSupportedException("Sorted query provider does not support creating new queries");
    }

    public object Execute(Expression expression)
    {
        // 对于已排序的数据，直接在内存中执行
        return _sortedData.AsQueryable().Provider.Execute(expression);
    }

    public TResult Execute<TResult>(Expression expression)
    {
        // 对于已排序的数据，直接在内存中执行
        return _sortedData.AsQueryable().Provider.Execute<TResult>(expression);
    }
}

/// <summary>
/// 已排序可查询对象
/// </summary>
/// <typeparam name="T">元素类型</typeparam>
internal sealed class SortedQueryable<T> : IOrderedQueryable<T>
    where T : class
{
    private readonly SortedQueryProvider<T> _provider;
    private readonly IEnumerable<T> _data;

    public SortedQueryable(SortedQueryProvider<T> provider)
    {
        _provider = provider ?? throw new ArgumentNullException(nameof(provider));
        _data = ((SortedQueryProvider<T>)provider)._sortedData; // 通过反射或内部访问获取数据
    }

    public Type ElementType => typeof(T);
    public Expression Expression => Expression.Constant(this, typeof(SortedQueryable<T>));
    public IQueryProvider Provider => _provider;

    public IEnumerator<T> GetEnumerator()
    {
        return _data.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return GetEnumerator();
    }
}