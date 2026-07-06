using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using TinyDb.Bson;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

public class Queryable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource, TData> : IOrderedQueryable<TData>
    where TSource : class
{
    private readonly QueryExecutor _executor;
    private readonly string _collectionName;

    public Queryable(QueryExecutor executor, string collectionName, LinqExp.Expression? expression = null)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentNullException(nameof(collectionName));

        _collectionName = collectionName;
        Expression = expression ?? LinqExp.Expression.Constant(this);
        Provider = new QueryProvider<TSource, TData>(_executor, _collectionName);
    }

    internal Queryable(QueryExecutor executor, string collectionName, IQueryProvider provider, LinqExp.Expression? expression = null)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentNullException(nameof(collectionName));
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));

        _collectionName = collectionName;
        Expression = expression ?? LinqExp.Expression.Constant(this);
    }

    public Type ElementType => typeof(TData);

    public LinqExp.Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public IEnumerator<TData> GetEnumerator()
    {
        var result = Provider.Execute<IEnumerable<TData>>(Expression);
        return (result ?? Enumerable.Empty<TData>()).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public override string ToString() => $"Queryable<{typeof(TSource).Name}->{typeof(TData).Name}>[{_collectionName}]";
}

// Helper for initial creation
public sealed class Queryable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] T> : Queryable<T, T>
    where T : class
{
    public Queryable(QueryExecutor executor, string collectionName) 
        : base(executor, collectionName)
    {
    }
}

internal sealed class UntypedQueryable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource> : IOrderedQueryable
    where TSource : class
{
    private readonly Type _elementType;

    public UntypedQueryable(IQueryProvider provider, LinqExp.Expression expression, Type elementType)
    {
        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        _elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
    }

    public Type ElementType => _elementType;

    public LinqExp.Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public IEnumerator GetEnumerator()
    {
        var result = Provider.Execute(Expression);
        return (result as IEnumerable ?? Enumerable.Empty<object>()).GetEnumerator();
    }
}

internal sealed class QueryProvider<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource, TData> : IQueryProvider
    where TSource : class
{
    private readonly QueryExecutor _executor;
    private readonly string _collectionName;

    public QueryProvider(QueryExecutor executor, string collectionName)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentNullException(nameof(collectionName));

        _collectionName = collectionName;
    }

    public IQueryable CreateQuery(LinqExp.Expression expression)
    {
        var elementType = expression.Type switch
        {
            var t when t.IsGenericType => t.GetGenericArguments()[0],
            var t when t.IsArray => t.GetElementType()!,
            _ => typeof(object)
        };

        return new UntypedQueryable<TSource>(this, expression, elementType);
    }

    public IQueryable<TElement> CreateQuery<TElement>(LinqExp.Expression expression)
    {
        return new Queryable<TSource, TElement>(_executor, _collectionName, expression);
    }

    public object? Execute(LinqExp.Expression expression)
    {
        return QueryPipeline.Execute<TSource>(_executor, _collectionName, expression);
    }

    public TResult Execute<TResult>(LinqExp.Expression expression)
    {
        var result = QueryPipeline.Execute<TSource>(_executor, _collectionName, expression);

        if (typeof(TResult) == typeof(IEnumerable<TData>))
        {
            var enumerable = result as IEnumerable;
            return (TResult)(object)(enumerable == null ? Enumerable.Empty<TData>() : enumerable.Cast<TData>());
        }

        return ConvertResult<TResult>(result);
    }

    private static TResult ConvertResult<TResult>(object? result)
    {
        if (result == null) return default!;
        if (result is TResult typed) return typed;

        var targetType = typeof(TResult);
        var nonNullableTarget = Nullable.GetUnderlyingType(targetType) ?? targetType;

        try
        {
            if (nonNullableTarget.IsEnum)
            {
                return (TResult)Enum.ToObject(nonNullableTarget, result);
            }

            return (TResult)Convert.ChangeType(result, nonNullableTarget, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException or ArgumentException)
        {
            throw new InvalidCastException(
                $"Query result of type '{result.GetType().FullName}' cannot be converted to '{targetType.FullName}'.",
                ex);
        }
    }
}

internal static class QueryPipeline
{
    public static object? Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource>(QueryExecutor executor, string collectionName, LinqExp.Expression expression)
        where TSource : class
    {
        // 1. 提取可下推到数据库的查询条件 (Where)
        // Note: PredicateExtractor.Extract may use Expression.Lambda for combining predicates,
        // but the actual execution path in AOT uses ExecuteAot which doesn't require dynamic code.
        var (shape, sourceConstant) = QueryShapeExtractor.Extract<TSource>(expression);

        if (sourceConstant != null &&
            TryExecuteCountTerminal(executor, collectionName, expression, shape, out var countResult))
        {
            return countResult;
        }

        // 2. 执行数据库查询 (直接调用泛型方法，无需反射)
        // Predicate is Expression<Func<TSource, bool>>?
        var queryResult = executor.ExecuteShaped(collectionName, shape, out var pushdown);

        // 3. 重写表达式树：将 Queryable 转换为 Enumerable 调用
        if (sourceConstant != null)
        {
            // 优先使用 AOT 兼容路径，确保 AOT 代码路径得到充分测试
            // 只有在 AOT 路径明确不支持的情况下才回退到动态编译路径
            try
            {
                return ExecuteAot(expression, queryResult, pushdown);
            }
            catch (NotSupportedException)
            {
                // AOT 路径不支持此操作，回退到动态编译路径（仅在非 AOT 环境下）
                throw;
            }
        }
        
        return queryResult;
    }

    private static bool TryExecuteCountTerminal<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource>(
        QueryExecutor executor,
        string collectionName,
        LinqExp.Expression expression,
        QueryShape<TSource> shape,
        out object? result)
        where TSource : class
    {
        result = null;
        if (shape.HasTypeShapingOperator ||
            expression is not LinqExp.MethodCallExpression methodCall ||
            methodCall.Method.DeclaringType != typeof(System.Linq.Queryable))
        {
            return false;
        }

        var methodName = methodCall.Method.Name;
        if (methodName is not ("Count" or "LongCount"))
        {
            return false;
        }

        if (!IsCountFastPathShapeSafe(methodCall, shape))
        {
            return false;
        }

        if (!TryBuildCountPredicate(methodCall, shape.Predicate, out var predicate))
        {
            return false;
        }

        var count = executor.Count(collectionName, predicate);
        if (shape.Skip is { } skip && skip > 0)
        {
            count = Math.Max(0, count - skip);
        }

        if (shape.Take is { } take)
        {
            count = Math.Min(count, Math.Max(0, take));
        }

        if (methodName == "LongCount")
        {
            result = count;
        }
        else
        {
            result = checked((int)count);
        }

        return true;
    }

    private static bool IsCountFastPathShapeSafe<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource>(
        LinqExp.MethodCallExpression terminalCall,
        QueryShape<TSource> shape)
        where TSource : class
    {
        if (terminalCall.Arguments.Count == 2 &&
            (shape.Skip.HasValue || shape.Take.HasValue))
        {
            return false;
        }

        var stack = new Stack<LinqExp.MethodCallExpression>();
        var current = terminalCall.Arguments[0];
        while (current is LinqExp.MethodCallExpression call && call.Method.DeclaringType == typeof(System.Linq.Queryable))
        {
            stack.Push(call);
            current = call.Arguments[0];
        }

        var stage = CountFastPathStage.BeforePagination;
        var seenSkip = false;
        var seenTake = false;

        foreach (var call in stack)
        {
            switch (call.Method.Name)
            {
                case "Where":
                    if (stage != CountFastPathStage.BeforePagination ||
                        !TryGetPredicateLambda<TSource>(call, out _))
                    {
                        return false;
                    }
                    break;

                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":
                    if (stage != CountFastPathStage.BeforePagination)
                    {
                        return false;
                    }
                    break;

                case "Skip":
                    if (stage != CountFastPathStage.BeforePagination ||
                        seenSkip ||
                        seenTake ||
                        !TryGetIntConstantArgument(call))
                    {
                        return false;
                    }

                    seenSkip = true;
                    stage = CountFastPathStage.AfterSkip;
                    break;

                case "Take":
                    if (seenTake ||
                        !TryGetIntConstantArgument(call))
                    {
                        return false;
                    }

                    seenTake = true;
                    stage = CountFastPathStage.AfterTake;
                    break;

                default:
                    return false;
            }
        }

        return true;
    }

    private static bool TryGetPredicateLambda<TSource>(
        LinqExp.MethodCallExpression call,
        [NotNullWhen(true)] out LinqExp.LambdaExpression? lambda)
        where TSource : class
    {
        lambda = null;
        if (call.Arguments.Count < 2)
        {
            return false;
        }

        if (call.Arguments[1] is LinqExp.UnaryExpression unary &&
            unary.Operand is LinqExp.LambdaExpression quotedLambda)
        {
            lambda = quotedLambda;
        }
        else if (call.Arguments[1] is LinqExp.LambdaExpression directLambda)
        {
            lambda = directLambda;
        }
        else
        {
            return false;
        }

        return lambda.Parameters.Count == 1 &&
               lambda.Parameters[0].Type == typeof(TSource) &&
               lambda.ReturnType == typeof(bool);
    }

    private static bool TryGetIntConstantArgument(LinqExp.MethodCallExpression call)
    {
        return call.Arguments.Count >= 2 &&
               call.Arguments[1] is LinqExp.ConstantExpression { Value: int };
    }

    private enum CountFastPathStage
    {
        BeforePagination,
        AfterSkip,
        AfterTake
    }

    private static bool TryBuildCountPredicate<TSource>(
        LinqExp.MethodCallExpression methodCall,
        LinqExp.Expression<Func<TSource, bool>>? shapePredicate,
        out LinqExp.Expression<Func<TSource, bool>>? predicate)
        where TSource : class
    {
        predicate = shapePredicate;
        if (methodCall.Arguments.Count == 1)
        {
            return true;
        }

        if (methodCall.Arguments.Count != 2 ||
            methodCall.Arguments[1] is not LinqExp.UnaryExpression unary ||
            unary.Operand is not LinqExp.LambdaExpression lambda ||
            lambda.Parameters.Count != 1 ||
            lambda.Parameters[0].Type != typeof(TSource) ||
            lambda.ReturnType != typeof(bool))
        {
            return false;
        }

        var terminalPredicate = (LinqExp.Expression<Func<TSource, bool>>)lambda;
        predicate = shapePredicate == null
            ? terminalPredicate
            : AndAlso(shapePredicate, terminalPredicate);
        return true;
    }

    private static LinqExp.Expression<Func<TSource, bool>> AndAlso<TSource>(
        LinqExp.Expression<Func<TSource, bool>> left,
        LinqExp.Expression<Func<TSource, bool>> right)
        where TSource : class
    {
        var parameter = LinqExp.Expression.Parameter(typeof(TSource), "x");
        var leftBody = new ParameterReplaceVisitor(left.Parameters[0], parameter).Visit(left.Body)!;
        var rightBody = new ParameterReplaceVisitor(right.Parameters[0], parameter).Visit(right.Body)!;
        return LinqExp.Expression.Lambda<Func<TSource, bool>>(
            LinqExp.Expression.AndAlso(leftBody, rightBody),
            parameter);
    }

    private sealed class ParameterReplaceVisitor : LinqExp.ExpressionVisitor
    {
        private readonly LinqExp.ParameterExpression _from;
        private readonly LinqExp.ParameterExpression _to;

        public ParameterReplaceVisitor(LinqExp.ParameterExpression from, LinqExp.ParameterExpression to)
        {
            _from = from;
            _to = to;
        }

        protected override LinqExp.Expression VisitParameter(LinqExp.ParameterExpression node)
        {
            return ReferenceEquals(node, _from) ? _to : node;
        }
    }

    private static object? ExecuteAot<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TSource>(LinqExp.Expression expression, IEnumerable<TSource> queryResult, QueryPushdownInfo pushdown)
        where TSource : class
    {
        var stack = new Stack<LinqExp.MethodCallExpression>();
        var current = expression;
        
        while (current is LinqExp.MethodCallExpression m && m.Method.DeclaringType == typeof(System.Linq.Queryable))
        {
            stack.Push(m);
            current = m.Arguments[0];
        }

        IEnumerable source = queryResult;
        Type currentType = typeof(TSource);
        bool isTyped = true;

        var remainingWhereSkips = pushdown.WherePushedCount;
        var remainingOrderSkips = pushdown.OrderPushedCount;
        var remainingSkipSkips = pushdown.SkipPushedCount;
        var remainingTakeSkips = pushdown.TakePushedCount;
        
        foreach (var m in stack)
        {
            if (IsTerminal(m.Method.Name))
            {
                return ExecuteTerminal(source, m);
            }

            var methodName = m.Method.Name;

            if (methodName == "Where")
            {
                if (remainingWhereSkips > 0)
                {
                    remainingWhereSkips--;
                    continue;
                }

                if (isTyped && currentType == typeof(TSource))
                    source = ExecuteWhereGeneric<TSource>((IEnumerable<TSource>)source, m);
                else
                    source = ExecuteWhere(source, m);
            }
            else if (methodName == "Select")
            {
                var selector = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
                source = ExecuteSelect(source, selector);
                currentType = selector.ReturnType;
                isTyped = false; // Type changed, lost generic context
            }
            else if (methodName == "Skip")
            {
                if (remainingSkipSkips > 0)
                {
                    remainingSkipSkips--;
                    continue;
                }

                if (m.Arguments[1] is LinqExp.ConstantExpression s)
                {
                    var count = (int)s.Value!;
                    if (isTyped && currentType == typeof(TSource))
                        source = ExecuteSkipGeneric<TSource>((IEnumerable<TSource>)source, count);
                    else
                        source = ExecuteSkip(source, count);
                }
            }
            else if (methodName == "Take")
            {
                if (remainingTakeSkips > 0)
                {
                    remainingTakeSkips--;
                    continue;
                }

                if (m.Arguments[1] is LinqExp.ConstantExpression t)
                {
                    var count = (int)t.Value!;
                    if (isTyped && currentType == typeof(TSource))
                        source = ExecuteTakeGeneric<TSource>((IEnumerable<TSource>)source, count);
                    else
                        source = ExecuteTake(source, count);
                }
            }
            else if (methodName == "Distinct")
            {
                if (isTyped && currentType == typeof(TSource))
                    source = ExecuteDistinctGeneric<TSource>((IEnumerable<TSource>)source);
                else
                    source = ExecuteDistinct(source);
            }
            else if (methodName == "OrderBy" || methodName == "OrderByDescending" || methodName == "ThenBy" || methodName == "ThenByDescending")
            {
                if (remainingOrderSkips > 0)
                {
                    remainingOrderSkips--;
                    continue;
                }

                if (isTyped && currentType == typeof(TSource))
                    source = ExecuteOrderByGeneric<TSource>((IEnumerable<TSource>)source, m);
                else
                    source = ExecuteOrderBy(source, m);
            }
            else if (methodName == "GroupBy")
            {
                // GroupBy in AOT mode - returns AotGrouping objects
                if (isTyped && currentType == typeof(TSource))
                    source = ExecuteGroupByGeneric<TSource>((IEnumerable<TSource>)source, m);
                else
                    source = ExecuteGroupBy(source, m);
                currentType = typeof(AotGrouping);
                isTyped = false;
            }
            else if (methodName == "Sum" || methodName == "Average" || methodName == "Min" || methodName == "Max")
            {
                // These are terminal operations when called on a grouped source
                return ExecuteAggregation(source, m);
            }
            else
            {
                throw new NotSupportedException($"Operation {methodName} is not supported in AOT mode.");
            }
        }

        // GroupBy 直接返回的结果类型是 AotGrouping，无法转换为用户期望的 IGrouping<TKey, TElement>
        // 这种情况需要回退到非 AOT 路径处理
        if (currentType == typeof(AotGrouping))
        {
            throw new NotSupportedException("GroupBy result enumeration requires dynamic code generation. Use Select after GroupBy for AOT compatibility.");
        }

        return source;
    }

    internal static object? ExecuteAotForTests<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TSource>(LinqExp.Expression expression, IEnumerable<TSource> queryResult, LinqExp.Expression? extractedPredicate)
        where TSource : class
    {
        var pushdown = new QueryPushdownInfo { WherePushedCount = extractedPredicate != null ? int.MaxValue : 0 };
        return ExecuteAot(expression, queryResult, pushdown);
    }

    internal static IEnumerable<T> ExecuteWhereGenericForTests<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
        where T : class
    {
        return ExecuteWhereGeneric(source, m);
    }

    internal static IEnumerable ExecuteWhereLambdaForTests(IEnumerable source, LinqExp.LambdaExpression lambda)
    {
        return ExecuteWhereLambda(source, lambda);
    }

    internal static bool IsTerminalForTests(string name) => IsTerminal(name);

    // ... IsTerminal, ExecuteTerminal ...

    private static IEnumerable<T> ExecuteWhereGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
        where T : class
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var queryExpr = parser.ParseExpression(lambda.Body);
        
        return source.Where(item => 
        {
            if (item == null) return false;
            var result = ExpressionEvaluator.EvaluateValue<T>(queryExpr, item);
            return result is bool b && b;
        });
    }

    private static IEnumerable<T> ExecuteOrderByGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
        where T : class
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var keyExpr = parser.ParseExpression(lambda.Body);
        
        Func<T, object> keySelector = item => ExpressionEvaluator.EvaluateValue<T>(keyExpr, item!)!;
        var comparer = new ObjectComparer();

        if (m.Method.Name == "OrderBy")
            return source.OrderBy(keySelector, comparer);
        if (m.Method.Name == "OrderByDescending")
            return source.OrderByDescending(keySelector, comparer);
        
        if (source is IOrderedEnumerable<T> ordered)
        {
            if (m.Method.Name == "ThenBy")
                return ordered.ThenBy(keySelector, comparer);
            if (m.Method.Name == "ThenByDescending")
                return ordered.ThenByDescending(keySelector, comparer);
        }
        
        return source.OrderBy(keySelector, comparer);
    }

    private static IEnumerable<T> ExecuteSkipGeneric<T>(IEnumerable<T> source, int count)
    {
        return source.Skip(count);
    }

    private static IEnumerable<T> ExecuteTakeGeneric<T>(IEnumerable<T> source, int count)
    {
        return source.Take(count);
    }

    private static IEnumerable<T> ExecuteDistinctGeneric<T>(IEnumerable<T> source)
    {
        return source.Distinct();
    }

    // ... existing ExecuteWhere, ExecuteOrderBy (non-generic), ExecuteSelect, etc. ...

    private static readonly HashSet<string> TerminalMethods = new(StringComparer.Ordinal)
    {
        "Count",
        "LongCount",
        "Any",
        "All",
        "First",
        "FirstOrDefault",
        "Single",
        "SingleOrDefault",
        "Last",
        "LastOrDefault",
        "ElementAt",
        "ElementAtOrDefault"
    };

    private static bool IsTerminal(string name) => TerminalMethods.Contains(name);

    private static object? ExecuteTerminal(IEnumerable source, LinqExp.MethodCallExpression m)
    {
         // Special handling for All - need to evaluate predicate on each item, not filter
         if (m.Method.Name == "All" && m.Arguments.Count == 2 && m.Arguments[1] is LinqExp.UnaryExpression uAll && uAll.Operand is LinqExp.LambdaExpression lambdaAll)
         {
             var parser = new ExpressionParser();
             var queryExpr = parser.ParseExpression(lambdaAll.Body);
             
             foreach (var item in source)
             {
                 if (item == null) continue;
                 var result = ExpressionEvaluator.EvaluateValue(queryExpr, item);
                 if (!(result is bool b && b))
                 {
                     return false; // Found an item that doesn't satisfy the predicate
                 }
             }
             return true; // All items satisfy the predicate
         }

         // For other methods with predicates (Any, First, etc.), filter first
         if (m.Arguments.Count == 2 && m.Arguments[1] is LinqExp.UnaryExpression u && u.Operand is LinqExp.LambdaExpression lambda)
         {
             source = ExecuteWhereLambda(source, lambda);
         }

         var typedSource = source.Cast<object>();

         var methodName = m.Method.Name;
         if (methodName == "Count") return typedSource.Count();
         if (methodName == "LongCount") return typedSource.LongCount();
         if (methodName == "Any") return typedSource.Any();
         if (methodName == "First") return typedSource.First();
         if (methodName == "FirstOrDefault") return typedSource.FirstOrDefault();
         if (methodName == "Single") return typedSource.Single();
         if (methodName == "SingleOrDefault") return typedSource.SingleOrDefault();
         if (methodName == "Last") return typedSource.Last();
         if (methodName == "LastOrDefault") return typedSource.LastOrDefault();
         if (methodName == "ElementAt") return typedSource.ElementAt((int)((LinqExp.ConstantExpression)m.Arguments[1]).Value!);
         if (methodName == "ElementAtOrDefault") return typedSource.ElementAtOrDefault((int)((LinqExp.ConstantExpression)m.Arguments[1]).Value!);
         return null;
    }

    private static IEnumerable ExecuteWhere(IEnumerable source, LinqExp.MethodCallExpression m)
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        return ExecuteWhereLambda(source, lambda);
    }

    private static IEnumerable ExecuteWhereLambda(IEnumerable source, LinqExp.LambdaExpression lambda)
    {
        var parser = new ExpressionParser();
        var queryExpr = parser.ParseExpression(lambda.Body);
        
        foreach (var item in source)
        {
            if (item == null) continue;
            var result = ExpressionEvaluator.EvaluateValue(queryExpr, item);
            if (result is bool b && b)
            {
                yield return item;
            }
        }
    }

    private static IEnumerable ExecuteSelect(IEnumerable source, LinqExp.LambdaExpression selector)
    {
        var parser = new ExpressionParser();
        var queryExpr = parser.ParseExpression(selector.Body);
        
        foreach (var item in source)
        {
            yield return ConvertValueToType(ExpressionEvaluator.EvaluateValue(queryExpr, item!), selector.ReturnType);
        }
    }

    private static IEnumerable ExecuteSkip(IEnumerable source, int count)
    {
        var e = source.GetEnumerator();
        while (count > 0 && e.MoveNext()) count--;
        while (e.MoveNext()) yield return e.Current;
    }

    private static IEnumerable ExecuteTake(IEnumerable source, int count)
    {
        var e = source.GetEnumerator();
        while (count > 0 && e.MoveNext())
        {
            yield return e.Current;
            count--;
        }
    }

    private static IEnumerable ExecuteDistinct(IEnumerable source)
    {
        return source.Cast<object>().Distinct();
    }

    private static IEnumerable ExecuteOrderBy(IEnumerable source, LinqExp.MethodCallExpression m)
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var keyExpr = parser.ParseExpression(lambda.Body);
        
        Func<object, object> keySelector = item => ExpressionEvaluator.EvaluateValue(keyExpr, item!)!;
        var comparer = new ObjectComparer();
        var typedSource = source.Cast<object>();

        if (m.Method.Name == "OrderBy")
            return typedSource.OrderBy(keySelector, comparer);
        if (m.Method.Name == "OrderByDescending")
            return typedSource.OrderByDescending(keySelector, comparer);
        
        if (source is IOrderedEnumerable<object> ordered)
        {
            if (m.Method.Name == "ThenBy")
                return ordered.ThenBy(keySelector, comparer);
            if (m.Method.Name == "ThenByDescending")
                return ordered.ThenByDescending(keySelector, comparer);
        }

        // Fallback
        return typedSource.OrderBy(keySelector, comparer);
    }

    /// <summary>
    /// AOT-compatible grouping that implements IGrouping interface
    /// </summary>
    internal sealed class AotGrouping : IGrouping<object, object>, IEnumerable<object>
    {
        private readonly object? _key;
        private readonly List<object> _elements;

        public AotGrouping(object? key, IEnumerable<object> elements)
        {
            _key = key;
            _elements = elements.ToList();
        }

        public object Key => _key!;
        public int Count => _elements.Count;
        public IEnumerator<object> GetEnumerator() => _elements.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public decimal Sum(Func<object, object> selector)
        {
            decimal sum = 0;
            foreach (var item in _elements)
            {
                sum = AddAggregateValue(sum, selector(item));
            }
            return sum;
        }

        public decimal Average(Func<object, object> selector)
        {
            if (_elements.Count == 0) return 0;
            return Sum(selector) / _elements.Count;
        }

        public object? Min(Func<object, object> selector)
        {
            object? min = null;
            foreach (var item in _elements)
            {
                var value = selector(item);
                if (value == null) continue;
                if (min == null || QueryValueComparer.Compare(value, min) < 0)
                    min = value;
            }
            return min;
        }

        public object? Max(Func<object, object> selector)
        {
            object? max = null;
            foreach (var item in _elements)
            {
                var value = selector(item);
                if (value == null) continue;
                if (max == null || QueryValueComparer.Compare(value, max) > 0)
                    max = value;
            }
            return max;
        }
    }

    private readonly struct GroupKey : IEquatable<GroupKey>
    {
        public GroupKey(object? value)
        {
            Value = value;
        }

        public object? Value { get; }

        public bool Equals(GroupKey other)
        {
            return QueryValueComparer.Compare(Value, other.Value) == 0;
        }

        public override bool Equals(object? obj)
        {
            return obj is GroupKey other && Equals(other);
        }

        public override int GetHashCode()
        {
            return QueryValueComparer.GetHashCode(Value);
        }
    }

    private static IEnumerable<AotGrouping> ExecuteGroupByGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
        where T : class
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var keyExpr = parser.ParseExpression(lambda.Body);

        var groups = new Dictionary<GroupKey, List<object>>();

        foreach (var item in source)
        {
            if (item == null) continue;
            var key = new GroupKey(ExpressionEvaluator.EvaluateValue<T>(keyExpr, item));

            if (!groups.TryGetValue(key, out var list))
            {
                list = new List<object>();
                groups[key] = list;
            }
            list.Add(item);
        }

        foreach (var kvp in groups)
        {
            yield return new AotGrouping(kvp.Key.Value, kvp.Value);
        }
    }

    private static IEnumerable<AotGrouping> ExecuteGroupBy(IEnumerable source, LinqExp.MethodCallExpression m)
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var keyExpr = parser.ParseExpression(lambda.Body);

        var groups = new Dictionary<GroupKey, List<object>>();

        foreach (var item in source)
        {
            if (item == null) continue;
            var key = new GroupKey(ExpressionEvaluator.EvaluateValue(keyExpr, item));

            if (!groups.TryGetValue(key, out var list))
            {
                list = new List<object>();
                groups[key] = list;
            }
            list.Add(item);
        }

        foreach (var kvp in groups)
        {
            yield return new AotGrouping(kvp.Key.Value, kvp.Value);
        }
    }

    private static object? ExecuteAggregation(IEnumerable source, LinqExp.MethodCallExpression m)
    {
        // Get the selector lambda if present
        LinqExp.LambdaExpression? selectorLambda = null;
        QueryExpression? selectorExpr = null;

        if (m.Arguments.Count >= 2 && m.Arguments[1] is LinqExp.UnaryExpression u && u.Operand is LinqExp.LambdaExpression lambda)
        {
            selectorLambda = lambda;
            var parser = new ExpressionParser();
            selectorExpr = parser.ParseExpression(lambda.Body);
        }

        Func<object, object> selector = selectorExpr != null
            ? (item => ExpressionEvaluator.EvaluateValue(selectorExpr, item)!)
            : (item => item);

        var items = source.Cast<object>().ToList();

        return m.Method.Name switch
        {
            "Sum" => Sum(items, selector, m.Method.ReturnType),
            "Average" => Average(items, selector, m.Method.ReturnType),
            "Min" => Min(items, selector),
            "Max" => Max(items, selector),
            _ => throw new NotSupportedException($"Aggregation {m.Method.Name} is not supported")
        };
    }

    private static object? Sum(IEnumerable<object> items, Func<object, object> selector, Type returnType)
    {
        decimal sum = 0;
        foreach (var item in items)
        {
            sum = AddAggregateValue(sum, selector(item));
        }

        return ConvertValueToType(sum, returnType);
    }

    private static object? Average(IReadOnlyCollection<object> items, Func<object, object> selector, Type returnType)
    {
        if (items.Count == 0) return ConvertValueToType(0m, returnType);

        decimal sum = 0;
        foreach (var item in items)
        {
            sum = AddAggregateValue(sum, selector(item));
        }

        return ConvertValueToType(sum / items.Count, returnType);
    }

    private static object? Min(IEnumerable<object> items, Func<object, object> selector)
    {
        object? min = null;
        foreach (var item in items)
        {
            var value = selector(item);
            if (value == null) continue;
            if (min == null || QueryValueComparer.Compare(value, min) < 0) min = value;
        }

        return min;
    }

    private static object? Max(IEnumerable<object> items, Func<object, object> selector)
    {
        object? max = null;
        foreach (var item in items)
        {
            var value = selector(item);
            if (value == null) continue;
            if (max == null || QueryValueComparer.Compare(value, max) > 0) max = value;
        }

        return max;
    }

    private static decimal AddAggregateValue(decimal current, object? value)
    {
        if (value == null) return current;

        try
        {
            return checked(current + Convert.ToDecimal(value));
        }
        catch (Exception ex) when (ex is OverflowException or InvalidCastException or FormatException)
        {
            throw new InvalidOperationException("Numeric aggregate could not be evaluated without overflow or conversion loss.", ex);
        }
    }

    private static object? ConvertValueToType(object? value, Type targetType)
    {
        if (value == null) return null;

        var nonNullableTarget = Nullable.GetUnderlyingType(targetType) ?? targetType;
        if (nonNullableTarget.IsInstanceOfType(value)) return value;

        try
        {
            if (nonNullableTarget.IsEnum)
            {
                return Enum.ToObject(nonNullableTarget, value);
            }

            return Convert.ChangeType(value, nonNullableTarget, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException or ArgumentException)
        {
            throw new InvalidOperationException(
                $"Unable to convert query value of type '{value.GetType().FullName}' to '{targetType.FullName}'.",
                ex);
        }
    }

    private sealed class ObjectComparer : IComparer<object>
    {
        public int Compare(object? x, object? y)
        {
            return QueryValueComparer.Compare(x, y);
        }

        private static int CompareLegacy(object? x, object? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;

            // 处理 BsonValue 的特殊比较
            if (x is BsonValue bx && y is BsonValue by)
            {
                return bx.CompareTo(by);
            }
            
            // 快速路径：数值比较
            if (IsNumeric(x) && IsNumeric(y))
            {
                return ToDouble(x).CompareTo(ToDouble(y));
            }

            if (x is string xs && y is string ys)
            {
                return string.Compare(xs, ys, StringComparison.Ordinal);
            }

            // 日期比较
            if (x is DateTime d1 && y is DateTime d2) return d1.CompareTo(d2);

            if (x is IComparable cx && x.GetType() == y.GetType()) return cx.CompareTo(y);
            
            return string.Compare(x.ToString(), y.ToString(), StringComparison.Ordinal);
        }
        
        private static bool IsNumeric(object x) => 
            x is int || x is long || x is double || x is float || x is decimal || 
            x is short || x is byte || x is BsonDouble || x is BsonInt32 || x is BsonInt64;

        private static double ToDouble(object val)
        {
            if (val is double d) return d;
            if (val is int i) return i;
            if (val is long l) return l;
            if (val is float f) return f;
            if (val is decimal dec) return (double)dec;
            if (val is BsonDouble bd) return bd.Value;
            if (val is BsonInt32 bi) return bi.Value;
            if (val is BsonInt64 bl) return bl.Value;

            try
            {
                return Convert.ToDouble(val, System.Globalization.CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or InvalidCastException or OverflowException)
            {
                throw new InvalidOperationException($"Unable to convert value '{val}' ({val.GetType().FullName}) to double.", ex);
            }
        }
    }

}

internal static class PredicateExtractor
{
    public static (LinqExp.Expression? Predicate, LinqExp.ConstantExpression? Source, bool HasMultiplePredicates) ExtractAot(LinqExp.Expression expression, Type entityType)
    {
        var visitor = new AotExtractionVisitor(entityType);
        visitor.Visit(expression);
        return (visitor.Predicate, visitor.SourceQueryable, visitor.HasMultiplePredicates);
    }

    internal class AotExtractionVisitor : LinqExp.ExpressionVisitor
    {
        private readonly Type _entityType;
        public LinqExp.Expression? Predicate { get; private set; }
        public LinqExp.ConstantExpression? SourceQueryable { get; private set; }
        public bool HasMultiplePredicates { get; private set; }

        public AotExtractionVisitor(Type entityType)
        {
            _entityType = entityType;
        }

        public override LinqExp.Expression? Visit(LinqExp.Expression? node)
        {
            if (node is LinqExp.MethodCallExpression m)
            {
                if (m.Method.Name == "Where" && m.Method.DeclaringType == typeof(System.Linq.Queryable))
                {
                    if (m.Arguments[1] is LinqExp.UnaryExpression u && u.Operand is LinqExp.LambdaExpression l)
                    {
                        if (Predicate == null)
                        {
                            Predicate = l;
                        }
                        else
                        {
                            HasMultiplePredicates = true;
                        }
                    }
                }
            }
            else if (node is LinqExp.ConstantExpression c && typeof(IQueryable).IsAssignableFrom(c.Type))
            {
                SourceQueryable = c;
            }

            return base.Visit(node);
        }
    }
}
