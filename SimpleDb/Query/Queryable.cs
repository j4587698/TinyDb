using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;

namespace SimpleDb.Query;

public sealed class Queryable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T> : IQueryable<T>
    where T : class, new()
{
    private readonly QueryExecutor _executor;
    private readonly string _collectionName;

    public Queryable(QueryExecutor executor, string collectionName, Expression? expression = null)
    {
        _executor = executor ?? throw new ArgumentNullException(nameof(executor));
        if (string.IsNullOrWhiteSpace(collectionName)) throw new ArgumentNullException(nameof(collectionName));

        _collectionName = collectionName;
        Expression = expression ?? Expression.Constant(this);
        Provider = new QueryProvider(_executor, _collectionName);
    }

    public Type ElementType => typeof(T);

    public Expression Expression { get; }

    public IQueryProvider Provider { get; }

    public IEnumerator<T> GetEnumerator()
    {
        var sequence = Provider.Execute<IEnumerable<T>>(Expression) ?? Enumerable.Empty<T>();
        return sequence.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public override string ToString() => $"Queryable<{typeof(T).Name}>[{_collectionName}]";

    private sealed class QueryProvider : IQueryProvider
    {
        private readonly QueryExecutor _executor;
        private readonly string _collectionName;

        public QueryProvider(QueryExecutor executor, string collectionName)
        {
            _executor = executor;
            _collectionName = collectionName;
        }

        public IQueryable CreateQuery(Expression expression)
        {
            return new Queryable<T>(_executor, _collectionName, expression);
        }

        public object? Execute(Expression expression)
        {
            return QueryPipeline.Execute<T>(_executor, _collectionName, expression);
        }

        public TResult Execute<TResult>(Expression expression)
        {
            var result = QueryPipeline.Execute<T>(_executor, _collectionName, expression);

            if (result is TResult typed)
            {
                return typed;
            }

            if (result is null)
            {
                return default!;
            }

            return (TResult)result;
        }

        IQueryable<TElement> IQueryProvider.CreateQuery<TElement>(Expression expression)
        {
            if (typeof(TElement) != typeof(T))
            {
                throw new NotSupportedException("Only queries returning the same element type are supported.");
            }

            return (IQueryable<TElement>)CreateQuery(expression);
        }
    }
}

internal static class QueryPipeline
{
    public static object? Execute<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExecutor executor, string collectionName, Expression expression)
        where T : class, new()
    {
        var predicate = PredicateBuilder<T>.Build(expression);
        var data = predicate != null
            ? executor.Execute(collectionName, predicate)
            : executor.Execute<T>(collectionName);

        return QueryOperationResolver.Apply(expression, data);
    }
}

internal enum QueryOperation
{
    Sequence,
    ToList,
    Count,
    LongCount,
    Any,
    First,
    FirstOrDefault
}

internal static class QueryOperationResolver
{
    public static QueryOperation Determine(Expression expression)
    {
        if (expression is MethodCallExpression methodCall)
        {
            return methodCall.Method.Name switch
            {
                nameof(Queryable.Count) => QueryOperation.Count,
                nameof(Queryable.LongCount) => QueryOperation.LongCount,
                nameof(Queryable.Any) => QueryOperation.Any,
                nameof(Queryable.First) => QueryOperation.First,
                nameof(Queryable.FirstOrDefault) => QueryOperation.FirstOrDefault,
                "ToList" => QueryOperation.ToList,
                _ => QueryOperation.Sequence
            };
        }

        return QueryOperation.Sequence;
    }

    public static object? Apply<T>(Expression expression, IEnumerable<T> data)
    {
        return Determine(expression) switch
        {
            QueryOperation.ToList => ToList(data),
            QueryOperation.Count => Count(data),
            QueryOperation.LongCount => LongCount(data),
            QueryOperation.Any => Any(data),
            QueryOperation.First => First(data),
            QueryOperation.FirstOrDefault => FirstOrDefault(data),
            _ => data
        };
    }

    private static List<T> ToList<T>(IEnumerable<T> data)
    {
        var list = new List<T>();
        foreach (var item in data)
        {
            list.Add(item);
        }
        return list;
    }

    private static int Count<T>(IEnumerable<T> data)
    {
        var count = 0;
        foreach (var _ in data)
        {
            count++;
        }
        return count;
    }

    private static long LongCount<T>(IEnumerable<T> data)
    {
        long count = 0;
        foreach (var _ in data)
        {
            count++;
        }
        return count;
    }

    private static bool Any<T>(IEnumerable<T> data)
    {
        using var enumerator = data.GetEnumerator();
        return enumerator.MoveNext();
    }

    private static T First<T>(IEnumerable<T> data)
    {
        foreach (var item in data)
        {
            return item!;
        }
        throw new InvalidOperationException("Sequence contains no elements");
    }

    private static T? FirstOrDefault<T>(IEnumerable<T> data)
    {
        foreach (var item in data)
        {
            return item!;
        }
        return default;
    }
}

internal static class PredicateBuilder<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>
    where T : class, new()
{
    public static Expression<Func<T, bool>>? Build(Expression expression)
    {
        var predicates = new List<Expression<Func<T, bool>>>();
        CollectPredicates(expression, predicates);

        if (predicates.Count == 0)
        {
            return null;
        }

        return Combine(predicates);
    }

    private static void CollectPredicates(Expression expression, IList<Expression<Func<T, bool>>> predicates)
    {
        switch (expression)
        {
            case MethodCallExpression methodCall:
                if (methodCall.Arguments.Count > 0)
                {
                    CollectPredicates(methodCall.Arguments[0], predicates);
                }

                if (methodCall.Arguments.Count > 1)
                {
                    var lambda = ExtractLambda(methodCall.Arguments[1]);
                    if (lambda != null)
                    {
                        predicates.Add(lambda);
                    }
                }
                break;

            case UnaryExpression unary when unary.NodeType == ExpressionType.Quote:
                CollectPredicates(unary.Operand, predicates);
                break;
        }
    }

    private static Expression<Func<T, bool>>? ExtractLambda(Expression expression)
    {
        var lambda = expression switch
        {
            UnaryExpression { NodeType: ExpressionType.Quote, Operand: LambdaExpression inner } => inner,
            LambdaExpression direct => direct,
            _ => null
        };

        return lambda as Expression<Func<T, bool>>;
    }

    private static Expression<Func<T, bool>> Combine(IList<Expression<Func<T, bool>>> predicates)
    {
        var parameter = Expression.Parameter(typeof(T), "entity");
        Expression? body = null;

        foreach (var predicate in predicates)
        {
            var replaced = new ParameterReplacer(predicate.Parameters[0], parameter).Visit(predicate.Body)!;
            body = body == null ? replaced : Expression.AndAlso(body, replaced);
        }

        return Expression.Lambda<Func<T, bool>>(body!, parameter);
    }
}

internal sealed class ParameterReplacer : System.Linq.Expressions.ExpressionVisitor
{
    private readonly System.Linq.Expressions.ParameterExpression _source;
    private readonly System.Linq.Expressions.ParameterExpression _target;

    public ParameterReplacer(System.Linq.Expressions.ParameterExpression source, System.Linq.Expressions.ParameterExpression target)
    {
        _source = source;
        _target = target;
    }

    protected override System.Linq.Expressions.Expression VisitParameter(System.Linq.Expressions.ParameterExpression node)
    {
        return node == _source ? _target : base.VisitParameter(node);
    }
}
