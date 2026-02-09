using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

public class Queryable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource, TData> : IOrderedQueryable<TData>
    where TSource : class, new()
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
    where T : class, new()
{
    public Queryable(QueryExecutor executor, string collectionName) 
        : base(executor, collectionName)
    {
    }
}

internal sealed class UntypedQueryable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource> : IOrderedQueryable
    where TSource : class, new()
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
    where TSource : class, new()
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
            var t when t.IsArray => t.GetElementType() ?? typeof(object),
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

        return (TResult)result!;
    }
}

internal static class QueryPipeline
{
    public static object? Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicMethods)] TSource>(QueryExecutor executor, string collectionName, LinqExp.Expression expression)
        where TSource : class, new()
    {
        // 1. 提取可下推到数据库的查询条件 (Where)
        // Note: PredicateExtractor.Extract may use Expression.Lambda for combining predicates,
        // but the actual execution path in AOT uses ExecuteAot which doesn't require dynamic code.
        LinqExp.Expression? predicate = null;
        LinqExp.ConstantExpression? sourceConstant = null;
        var result = PredicateExtractor.ExtractAot(expression, typeof(TSource));
        predicate = result.Predicate;
        sourceConstant = result.Source;
        if (result.HasMultiplePredicates)
        {
            predicate = null;
        }

        // 2. 执行数据库查询 (直接调用泛型方法，无需反射)
        // Predicate is Expression<Func<TSource, bool>>?
        var queryResult = executor.Execute<TSource>(collectionName, (LinqExp.Expression<Func<TSource, bool>>?)predicate);

        // 3. 重写表达式树：将 Queryable 转换为 Enumerable 调用
        if (sourceConstant != null)
        {
            // 优先使用 AOT 兼容路径，确保 AOT 代码路径得到充分测试
            // 只有在 AOT 路径明确不支持的情况下才回退到动态编译路径
            try
            {
                return ExecuteAot(expression, queryResult, predicate);
            }
            catch (NotSupportedException)
            {
                // AOT 路径不支持此操作，回退到动态编译路径（仅在非 AOT 环境下）
                throw;
            }
        }
        
        return queryResult;
    }

    private static object? ExecuteAot<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] TSource>(LinqExp.Expression expression, IEnumerable<TSource> queryResult, LinqExp.Expression? extractedPredicate)
        where TSource : class, new()
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
        
        foreach (var m in stack)
        {
            if (IsTerminal(m.Method.Name))
            {
                return ExecuteTerminal(source, m);
            }

            var methodName = m.Method.Name;

            if (methodName == "Where")
            {
                // If the database extracted a predicate, we assume it handles ALL Where clauses.
                // We skip all in-memory filtering to avoid AOT reflection issues.
                if (extractedPredicate != null)
                {
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
        where TSource : class, new()
    {
        return ExecuteAot(expression, queryResult, extractedPredicate);
    }

    internal static IEnumerable<T> ExecuteWhereGenericForTests<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
        where T : class, new()
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
        where T : class, new()
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
        where T : class, new()
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
            yield return ExpressionEvaluator.EvaluateValue(queryExpr, item!);
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
        private readonly object _key;
        private readonly List<object> _elements;

        public AotGrouping(object key, IEnumerable<object> elements)
        {
            _key = key;
            _elements = elements.ToList();
        }

        public object Key => _key;
        public int Count => _elements.Count;
        public IEnumerator<object> GetEnumerator() => _elements.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public decimal Sum(Func<object, object> selector)
        {
            decimal sum = 0;
            foreach (var item in _elements)
            {
                var value = selector(item);
                if (value != null)
                {
                    sum += Convert.ToDecimal(value);
                }
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
                if (min == null || Comparer<object>.Default.Compare(value, min) < 0)
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
                if (max == null || Comparer<object>.Default.Compare(value, max) > 0)
                    max = value;
            }
            return max;
        }
    }

    private static IEnumerable<AotGrouping> ExecuteGroupByGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
        where T : class, new()
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var keyExpr = parser.ParseExpression(lambda.Body);

        var groups = new Dictionary<object, List<object>>();

        foreach (var item in source)
        {
            if (item == null) continue;
            var key = ExpressionEvaluator.EvaluateValue<T>(keyExpr, item) ?? "";

            if (!groups.TryGetValue(key, out var list))
            {
                list = new List<object>();
                groups[key] = list;
            }
            list.Add(item);
        }

        foreach (var kvp in groups)
        {
            yield return new AotGrouping(kvp.Key, kvp.Value);
        }
    }

    private static IEnumerable<AotGrouping> ExecuteGroupBy(IEnumerable source, LinqExp.MethodCallExpression m)
    {
        var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
        var parser = new ExpressionParser();
        var keyExpr = parser.ParseExpression(lambda.Body);

        var groups = new Dictionary<object, List<object>>();

        foreach (var item in source)
        {
            if (item == null) continue;
            var key = ExpressionEvaluator.EvaluateValue(keyExpr, item) ?? "";

            if (!groups.TryGetValue(key, out var list))
            {
                list = new List<object>();
                groups[key] = list;
            }
            list.Add(item);
        }

        foreach (var kvp in groups)
        {
            yield return new AotGrouping(kvp.Key, kvp.Value);
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
            "Sum" => items.Sum(x => Convert.ToDecimal(selector(x))),
            "Average" => items.Count > 0 ? items.Average(x => Convert.ToDecimal(selector(x))) : 0m,
            "Min" => items.Count > 0 ? items.Min(selector) : null,
            "Max" => items.Count > 0 ? items.Max(selector) : null,
            _ => throw new NotSupportedException($"Aggregation {m.Method.Name} is not supported")
        };
    }

    private sealed class ObjectComparer : IComparer<object>
    {
        public int Compare(object? x, object? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;
            
            // Try numeric
            if (IsNumeric(x) && IsNumeric(y))
            {
                try { return Convert.ToDouble(x).CompareTo(Convert.ToDouble(y)); } catch { }
            }

            if (x is string xs && y is string ys)
            {
                return string.Compare(xs, ys, StringComparison.Ordinal);
            }

            if (x is IComparable cx && x.GetType() == y.GetType()) return cx.CompareTo(y);
            
            return string.Compare(x.ToString(), y.ToString(), StringComparison.Ordinal);
        }
        
        private static bool IsNumeric(object x) => x is int || x is long || x is double || x is float || x is decimal || x is short || x is byte;
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
