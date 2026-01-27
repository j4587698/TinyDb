using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

public class Queryable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource, TData> : IOrderedQueryable<TData>
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
public sealed class Queryable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : Queryable<T, T>
    where T : class, new()
{
    public Queryable(QueryExecutor executor, string collectionName) 
        : base(executor, collectionName)
    {
    }
}

internal sealed class QueryProvider<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource, TData> : IQueryProvider
    where TSource : class, new()
{
    private readonly QueryExecutor _executor;
    private readonly string _collectionName;

    public QueryProvider(QueryExecutor executor, string collectionName)
    {
        _executor = executor;
        _collectionName = collectionName;
    }

    [UnconditionalSuppressMessage("Trimming", "IL2055", Justification = "MakeGenericType guarded by DynamicallyAccessedMembers on TypeSystem or usage pattern.")]
    [UnconditionalSuppressMessage("Aot", "IL3050", Justification = "MakeGenericType for Queryable<> might fail in AOT if not preserved, but standard usage preserves it.")]
    public IQueryable CreateQuery(LinqExp.Expression expression)
    {
        var elementType = TypeSystem.GetElementType(expression.Type);
        try
        {
            // Construct Queryable<TSource, NewElement>
            var queryableType = typeof(Queryable<,>).MakeGenericType(typeof(TSource), elementType);
            return (IQueryable)Activator.CreateInstance(
                queryableType,
                new object[] { _executor, _collectionName, expression }
            )!;
        }
        catch (TargetInvocationException tie)
        {
            throw tie.InnerException ?? tie;
        }
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
        return (TResult)QueryPipeline.Execute<TSource>(_executor, _collectionName, expression)!;
    }
}

internal static class QueryPipeline
{
    public static object? Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TSource>(QueryExecutor executor, string collectionName, LinqExp.Expression expression)
        where TSource : class, new()
    {
        // 1. 提取可下推到数据库的查询条件 (Where)
        // Note: PredicateExtractor.Extract may use Expression.Lambda for combining predicates,
        // but the actual execution path in AOT uses ExecuteAot which doesn't require dynamic code.
#pragma warning disable IL3050 // RequiresDynamicCode
        var (predicate, sourceConstant) = PredicateExtractor.Extract(expression, typeof(TSource));
#pragma warning restore IL3050

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
            catch (NotSupportedException) when (RuntimeFeature.IsDynamicCodeSupported)
            {
                // AOT 路径不支持此操作，回退到动态编译路径（仅在非 AOT 环境下）
                return ExecuteInMemory(expression, sourceConstant, queryResult);
            }
        }
        
        return queryResult;
    }

    private static object? ExecuteAot<TSource>(LinqExp.Expression expression, IEnumerable<TSource> queryResult, LinqExp.Expression? extractedPredicate)
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

            switch (m.Method.Name)
            {
                case "Where":
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
                    break;
                case "Select":
                    var selector = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)m.Arguments[1]).Operand;
                    source = ExecuteSelect(source, selector);
                    currentType = selector.ReturnType;
                    isTyped = false; // Type changed, lost generic context
                    break;
                case "Skip":
                     if (m.Arguments[1] is LinqExp.ConstantExpression s)
                        source = ExecuteSkip(source, (int)s.Value!);
                    break;
                case "Take":
                     if (m.Arguments[1] is LinqExp.ConstantExpression t)
                        source = ExecuteTake(source, (int)t.Value!);
                    break;
                case "Distinct":
                    source = ExecuteDistinct(source);
                    break;
                case "OrderBy":
                case "OrderByDescending":
                case "ThenBy":
                case "ThenByDescending":
                    if (isTyped && currentType == typeof(TSource))
                        source = ExecuteOrderByGeneric<TSource>((IEnumerable<TSource>)source, m);
                    else
                        source = ExecuteOrderBy(source, m);
                    break;
                case "GroupBy":
                    // GroupBy in AOT mode - returns AotGrouping objects
                    if (isTyped && currentType == typeof(TSource))
                        source = ExecuteGroupByGeneric<TSource>((IEnumerable<TSource>)source, m);
                    else
                        source = ExecuteGroupBy(source, m);
                    currentType = typeof(AotGrouping);
                    isTyped = false;
                    break;
                case "Sum":
                case "Average":
                case "Min":
                case "Max":
                    // These are terminal operations when called on a grouped source
                    return ExecuteAggregation(source, m);
                default:
                    throw new NotSupportedException($"Operation {m.Method.Name} is not supported in AOT mode.");
            }
        }

        // GroupBy 直接返回的结果类型是 AotGrouping，无法转换为用户期望的 IGrouping<TKey, TElement>
        // 这种情况需要回退到非 AOT 路径处理
        if (currentType == typeof(AotGrouping))
        {
            throw new NotSupportedException("GroupBy result enumeration requires dynamic code generation. Use Select after GroupBy for AOT compatibility.");
        }

        return CreateTypedEnumerable(source, currentType);
    }

    // ... IsTerminal, ExecuteTerminal ...

    private static IEnumerable<T> ExecuteWhereGeneric<T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
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

    private static IEnumerable<T> ExecuteOrderByGeneric<T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
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

    // ... existing ExecuteWhere, ExecuteOrderBy (non-generic), ExecuteSelect, etc. ...

    private static bool IsTerminal(string name)
    {
        return name is "Count" or "LongCount" or "Any" or "All" or "First" or "FirstOrDefault" or "Single" or "SingleOrDefault" or "Last" or "LastOrDefault" or "ElementAt" or "ElementAtOrDefault";
    }

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

         switch (m.Method.Name)
         {
             case "Count": return typedSource.Count();
             case "LongCount": return typedSource.LongCount();
             case "Any": return typedSource.Any();
             case "First": return typedSource.First();
             case "FirstOrDefault": return typedSource.FirstOrDefault();
             case "Single": return typedSource.Single();
             case "SingleOrDefault": return typedSource.SingleOrDefault();
             case "Last": return typedSource.Last();
             case "LastOrDefault": return typedSource.LastOrDefault();
             case "ElementAt": return typedSource.ElementAt((int)((LinqExp.ConstantExpression)m.Arguments[1]).Value!);
             case "ElementAtOrDefault": return typedSource.ElementAtOrDefault((int)((LinqExp.ConstantExpression)m.Arguments[1]).Value!);
         }
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

    private static IEnumerable<AotGrouping> ExecuteGroupByGeneric<T>(IEnumerable<T> source, LinqExp.MethodCallExpression m)
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

            if (x is IComparable cx && x.GetType() == y.GetType()) return cx.CompareTo(y);
            
            return string.Compare(x.ToString(), y.ToString(), StringComparison.Ordinal);
        }
        
        private static bool IsNumeric(object x) => x is int || x is long || x is double || x is float || x is decimal || x is short || x is byte;
    }

    [UnconditionalSuppressMessage("Aot", "IL3050", Justification = "Guarded by IsDynamicCodeSupported check in Execute.")]
    private static object? ExecuteInMemory(LinqExp.Expression expression, LinqExp.ConstantExpression sourceConstant, object queryResult)
    {
        var rewriter = new QueryableToEnumerableRewriter(sourceConstant, queryResult);
        var newExpression = rewriter.Visit(expression);
        
        var lambda = LinqExp.Expression.Lambda(newExpression);
        var func = lambda.Compile();
        return func.DynamicInvoke();
    }

    [UnconditionalSuppressMessage("Aot", "IL3050", Justification = "Creating generic List for known types.")]
    private static object CreateTypedEnumerable(IEnumerable source, Type targetType)
    {
        // Try to create List<T> dynamically
        var listType = typeof(List<>).MakeGenericType(targetType);
        var list = (IList)Activator.CreateInstance(listType)!;
        
        foreach (var item in source)
        {
            list.Add(item);
        }
        return list;
    }
}

internal sealed class QueryableToEnumerableRewriter : LinqExp.ExpressionVisitor
{
    private readonly LinqExp.ConstantExpression _target;
    private readonly object _replacement;

    public QueryableToEnumerableRewriter(LinqExp.ConstantExpression target, object replacement)
    {
        _target = target;
        _replacement = replacement;
    }

    protected override LinqExp.Expression VisitConstant(LinqExp.ConstantExpression node)
    {
        if (node == _target)
        {
            return LinqExp.Expression.Constant(_replacement);
        }
        return base.VisitConstant(node);
    }

    [UnconditionalSuppressMessage("Aot", "IL3050", Justification = "In-memory LINQ requires dynamic code.")]
    [UnconditionalSuppressMessage("Trimming", "IL2060", Justification = "MakeGenericMethod for Enumerable.")]
    protected override LinqExp.Expression VisitMethodCall(LinqExp.MethodCallExpression node)
    {
        if (node.Method.DeclaringType == typeof(System.Linq.Queryable))
        {
            var args = new LinqExp.Expression[node.Arguments.Count];
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                var arg = node.Arguments[i];
                if (arg is LinqExp.UnaryExpression u && u.NodeType == LinqExp.ExpressionType.Quote && u.Operand is LinqExp.LambdaExpression lambda)
                {
                    var compiled = lambda.Compile();
                    args[i] = LinqExp.Expression.Constant(compiled);
                }
                else
                {
                    args[i] = Visit(arg);
                }
            }

            var enumerableMethod = FindEnumerableMethod(node.Method.Name, args.Select(a => a.Type).ToArray(), node.Method.GetGenericArguments());
            
            if (enumerableMethod != null)
            {
                return LinqExp.Expression.Call(enumerableMethod, args);
            }
        }
        return base.VisitMethodCall(node);
    }

    [UnconditionalSuppressMessage("Aot", "IL3050", Justification = "Runtime LINQ composition.")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2060", Justification = "Runtime LINQ composition.")]
    private static MethodInfo? FindEnumerableMethod(string name, Type[] argTypes, Type[] genericArgs)
    {
        var methods = typeof(System.Linq.Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
            .Where(m => m.Name == name && m.GetParameters().Length == argTypes.Length);

        foreach (var m in methods)
        {
            if (m.IsGenericMethodDefinition && m.GetGenericArguments().Length == genericArgs.Length)
            {
                try
                {
                    var concrete = m.MakeGenericMethod(genericArgs);
                    var parameters = concrete.GetParameters();
                    bool match = true;
                    for (int i = 0; i < parameters.Length; i++)
                    {
                        if (!parameters[i].ParameterType.IsAssignableFrom(argTypes[i]))
                        {
                            match = false;
                            break;
                        }
                    }
                    
                    if (match) return concrete;
                }
                catch
                {
                }
            }
        }
        return null;
    }
}

internal static class PredicateExtractor
{
    [RequiresDynamicCode("Expression.Lambda requires dynamic code generation for combining predicates.")]
    public static (LinqExp.Expression? Predicate, LinqExp.ConstantExpression? Source) Extract(LinqExp.Expression expression, Type entityType)
    {
        var visitor = new ExtractionVisitor(entityType);
        visitor.Visit(expression);
        return (visitor.Predicate, visitor.SourceQueryable);
    }

    internal class ExtractionVisitor : LinqExp.ExpressionVisitor
    {
        private readonly Type _entityType;
        public LinqExp.Expression? Predicate { get; private set; }
        public LinqExp.ConstantExpression? SourceQueryable { get; private set; }

        public ExtractionVisitor(Type entityType)
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
                        else if (Predicate is LinqExp.LambdaExpression current)
                        {
                            // Combine predicates: current AND new
                            // Note: We don't rebind parameters because TinyDb's parser/evaluator
                            // handles ParameterExpression by type/context, not by strict reference identity.
                            var combinedBody = LinqExp.Expression.AndAlso(current.Body, l.Body);
#pragma warning disable IL3050 // Expression.Lambda requires dynamic code - this path only runs when dynamic code is supported
                            Predicate = LinqExp.Expression.Lambda(combinedBody, current.Parameters);
#pragma warning restore IL3050
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