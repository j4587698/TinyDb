using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Reflection;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

public sealed class Queryable<T> : IOrderedQueryable<T>
{
    private readonly QueryExecutor _executor;
    private readonly string _collectionName;
    private readonly Type _entityType;

    public Queryable(QueryExecutor executor, string collectionName, Type entityType, LinqExp.Expression? expression = null)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentNullException(nameof(collectionName));

        _collectionName = collectionName;
        _entityType = entityType;
        Expression = expression ?? LinqExp.Expression.Constant(this);
        Provider = new QueryProvider(_executor, _collectionName, _entityType);
    }

    public Queryable(QueryExecutor executor, string collectionName) 
        : this(executor, collectionName, typeof(T))
    {
    }

    public Type ElementType => typeof(T);

    public LinqExp.Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
    {
        var result = Provider.Execute<IEnumerable<T>>(Expression);
        return (result ?? Enumerable.Empty<T>()).GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public override string ToString() => $"Queryable<{typeof(T).Name}>[{_collectionName}]";

    private sealed class QueryProvider : IQueryProvider
    {
        private readonly QueryExecutor _executor;
        private readonly string _collectionName;
        private readonly Type _entityType;

        public QueryProvider(QueryExecutor executor, string collectionName, Type entityType)
        {
            _executor = executor;
            _collectionName = collectionName;
            _entityType = entityType;
        }

        public IQueryable CreateQuery(LinqExp.Expression expression)
        {
            var elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return (IQueryable)Activator.CreateInstance(
                    typeof(Queryable<>).MakeGenericType(elementType),
                    new object[] { _executor, _collectionName, _entityType, expression }
                )!;
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException ?? tie;
            }
        }

        public IQueryable<TElement> CreateQuery<TElement>(LinqExp.Expression expression)
        {
            return new Queryable<TElement>(_executor, _collectionName, _entityType, expression);
        }

        public object? Execute(LinqExp.Expression expression)
        {
            return QueryPipeline.Execute(_executor, _collectionName, _entityType, expression);
        }

        public TResult Execute<TResult>(LinqExp.Expression expression)
        {
            return (TResult)QueryPipeline.Execute(_executor, _collectionName, _entityType, expression)!;
        }
    }
}

internal static class QueryPipeline
{
    public static object? Execute(QueryExecutor executor, string collectionName, Type entityType, LinqExp.Expression expression)
    {
        // 1. 提取可下推到数据库的查询条件 (Where)
        var (predicate, sourceConstant) = PredicateExtractor.Extract(expression, entityType);

        // 2. 执行数据库查询
        var executeMethod = typeof(QueryExecutor).GetMethod(nameof(QueryExecutor.Execute))!
            .MakeGenericMethod(entityType);
        
        var queryResult = executeMethod.Invoke(executor, new object[] { collectionName, predicate });

        // 3. 重写表达式树：将 Queryable 转换为 Enumerable 调用
        if (sourceConstant != null)
        {
            var rewriter = new QueryableToEnumerableRewriter(sourceConstant, queryResult!);
            var newExpression = rewriter.Visit(expression);
            
            // 4. 编译并执行 (内存中)
            // newExpression 的结果是 IEnumerable<T> 或单值 (Count, First)
            var lambda = LinqExp.Expression.Lambda(newExpression);
            var func = lambda.Compile();
            return func.DynamicInvoke();
        }
        
        return queryResult;
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
        // 替换源 Queryable 为 IEnumerable 数据
        if (node == _target)
        {
            return LinqExp.Expression.Constant(_replacement);
        }
        return base.VisitConstant(node);
    }

    protected override LinqExp.Expression VisitMethodCall(LinqExp.MethodCallExpression node)
    {
        // 将 Queryable.* 调用转换为 Enumerable.* 调用
        if (node.Method.DeclaringType == typeof(System.Linq.Queryable))
        {
            var args = new LinqExp.Expression[node.Arguments.Count];
            for (int i = 0; i < node.Arguments.Count; i++)
            {
                var arg = node.Arguments[i];
                // 处理引用参数 (Expression<Func<...>>)
                if (arg is LinqExp.UnaryExpression u && u.NodeType == LinqExp.ExpressionType.Quote && u.Operand is LinqExp.LambdaExpression lambda)
                {
                    // 编译 Lambda 为委托
                    var compiled = lambda.Compile();
                    args[i] = LinqExp.Expression.Constant(compiled);
                }
                else
                {
                    args[i] = Visit(arg);
                }
            }

            // 查找对应的 Enumerable 方法
            var enumerableMethod = FindEnumerableMethod(node.Method.Name, args.Select(a => a.Type).ToArray(), node.Method.GetGenericArguments());
            
            if (enumerableMethod != null)
            {
                return LinqExp.Expression.Call(enumerableMethod, args);
            }
        }
        return base.VisitMethodCall(node);
    }

    private static MethodInfo? FindEnumerableMethod(string name, Type[] argTypes, Type[] genericArgs)
    {
        // 简单查找：名称匹配且参数数量匹配
        // 注意：Enumerable 方法通常是泛型定义
        var methods = typeof(System.Linq.Enumerable).GetMethods(BindingFlags.Static | BindingFlags.Public)
            .Where(m => m.Name == name && m.GetParameters().Length == argTypes.Length);

        foreach (var m in methods)
        {
            if (m.IsGenericMethodDefinition && m.GetGenericArguments().Length == genericArgs.Length)
            {
                try
                {
                    var concrete = m.MakeGenericMethod(genericArgs);
                    // 检查参数类型兼容性 (这里简化检查，假设 Queryable 和 Enumerable 签名一一对应)
                    // 实际应该检查 concrete.GetParameters()[i].ParameterType.IsAssignableFrom(argTypes[i])
                    return concrete;
                }
                catch
                {
                    // 泛型约束不匹配等
                }
            }
        }
        return null;
    }
}

internal static class PredicateExtractor
{
    public static (LinqExp.Expression? Predicate, LinqExp.ConstantExpression? Source) Extract(LinqExp.Expression expression, Type entityType)
    {
        var visitor = new ExtractionVisitor(entityType);
        visitor.Visit(expression);
        return (visitor.Predicate, visitor.Source);
    }

    private class ExtractionVisitor : LinqExp.ExpressionVisitor
    {
        private readonly Type _entityType;
        public LinqExp.Expression? Predicate { get; private set; }
        public LinqExp.ConstantExpression? Source { get; private set; }
        private bool _hitBarrier;

        public ExtractionVisitor(Type entityType)
        {
            _entityType = entityType;
        }

        protected override LinqExp.Expression VisitMethodCall(LinqExp.MethodCallExpression node)
        {
            if (_hitBarrier)
            {
                Visit(node.Arguments[0]);
                return node;
            }

            if (node.Method.Name == nameof(System.Linq.Queryable.Where) && node.Method.DeclaringType == typeof(System.Linq.Queryable))
            {
                Visit(node.Arguments[0]);

                if (!_hitBarrier && Source != null)
                {
                    var lambda = (LinqExp.LambdaExpression)((LinqExp.UnaryExpression)node.Arguments[1]).Operand;
                    if (lambda.Parameters[0].Type == _entityType)
                    {
                        if (Predicate == null)
                        {
                            Predicate = lambda;
                        }
                        else
                        {
                            var oldLambda = (LinqExp.LambdaExpression)Predicate;
                            var newBody = new ParameterReplacer(lambda.Parameters[0], oldLambda.Parameters[0]).Visit(lambda.Body);
                            Predicate = LinqExp.Expression.Lambda(LinqExp.Expression.AndAlso(oldLambda.Body, newBody!), oldLambda.Parameters[0]);
                        }
                    }
                    else
                    {
                        _hitBarrier = true;
                    }
                }
                return node;
            }

            _hitBarrier = true;
            Visit(node.Arguments[0]);
            return node;
        }

        protected override LinqExp.Expression VisitConstant(LinqExp.ConstantExpression node)
        {
            if (node.Type.IsGenericType && 
                node.Type.GetGenericTypeDefinition() == typeof(Queryable<>) &&
                node.Type.GetGenericArguments()[0] == _entityType)
            {
                Source = node;
                _hitBarrier = false;
            }
            return node;
        }
    }
}



internal static class TypeSystem
{
    internal static Type GetElementType(Type seqType)
    {
        var ienum = FindIEnumerable(seqType);
        if (ienum == null) return seqType;
        return ienum.GetGenericArguments()[0];
    }

    private static Type? FindIEnumerable(Type? seqType)
    {
        if (seqType == null || seqType == typeof(string)) return null;
        if (seqType.IsArray) return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType()!);
        if (seqType.IsGenericType)
        {
            foreach (var arg in seqType.GetGenericArguments())
            {
                var ienum = typeof(IEnumerable<>).MakeGenericType(arg);
                if (ienum.IsAssignableFrom(seqType)) return ienum;
            }
        }
        var ifaces = seqType.GetInterfaces();
        if (ifaces != null && ifaces.Length > 0)
        {
            foreach (var iface in ifaces)
            {
                var ienum = FindIEnumerable(iface);
                if (ienum != null) return ienum;
            }
        }
        if (seqType.BaseType != null && seqType.BaseType != typeof(object))
        {
            return FindIEnumerable(seqType.BaseType);
        }
        return null;
    }
}

internal sealed class ParameterReplacer : LinqExp.ExpressionVisitor
{
    private readonly LinqExp.ParameterExpression _source;
    private readonly LinqExp.ParameterExpression _target;

    public ParameterReplacer(LinqExp.ParameterExpression source, LinqExp.ParameterExpression target)
    {
        _source = source;
        _target = target;
    }

    protected override LinqExp.Expression VisitParameter(LinqExp.ParameterExpression node)
    {
        return node == _source ? _target : base.VisitParameter(node);
    }
}