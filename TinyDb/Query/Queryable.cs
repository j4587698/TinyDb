using System.Collections;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace TinyDb.Query;

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
    FirstOrDefault,
    Take,
    Skip
}

internal readonly record struct QueryOperationDescriptor(QueryOperation Operation, int? IntValue);

internal readonly record struct QueryPipelinePlan(
    IReadOnlyList<QueryOperationDescriptor> SequenceOperations,
    QueryOperation TerminalOperation);

internal static class QueryOperationResolver
{
    public static QueryPipelinePlan Analyze(Expression expression)
    {
        var sequenceOperations = new List<QueryOperationDescriptor>();
        var terminalOperation = QueryOperation.Sequence;
        var current = expression;

        while (current is MethodCallExpression methodCall)
        {
            switch (methodCall.Method.Name)
            {
                case nameof(Queryable.Take):
                    sequenceOperations.Add(new QueryOperationDescriptor(QueryOperation.Take, ExtractIntegerArgument(methodCall, 1)));
                    current = methodCall.Arguments[0];
                    continue;
                case nameof(Queryable.Skip):
                    sequenceOperations.Add(new QueryOperationDescriptor(QueryOperation.Skip, ExtractIntegerArgument(methodCall, 1)));
                    current = methodCall.Arguments[0];
                    continue;
                case nameof(Queryable.Count):
                    terminalOperation = QueryOperation.Count;
                    current = methodCall.Arguments[0];
                    continue;
                case nameof(Queryable.LongCount):
                    terminalOperation = QueryOperation.LongCount;
                    current = methodCall.Arguments[0];
                    continue;
                case nameof(Queryable.Any):
                    terminalOperation = QueryOperation.Any;
                    current = methodCall.Arguments[0];
                    continue;
                case nameof(Queryable.First):
                    terminalOperation = QueryOperation.First;
                    current = methodCall.Arguments[0];
                    continue;
                case nameof(Queryable.FirstOrDefault):
                    terminalOperation = QueryOperation.FirstOrDefault;
                    current = methodCall.Arguments[0];
                    continue;
                case "ToList":
                    terminalOperation = QueryOperation.ToList;
                    current = methodCall.Arguments[0];
                    continue;
                default:
                    current = methodCall.Arguments.FirstOrDefault();
                    continue;
            }
        }

        sequenceOperations.Reverse();
        return new QueryPipelinePlan(sequenceOperations, terminalOperation);
    }

    public static object? Apply<T>(Expression expression, IEnumerable<T> data)
    {
        var plan = Analyze(expression);
        IEnumerable<T> sequence = data;

        foreach (var descriptor in plan.SequenceOperations)
        {
            sequence = descriptor.Operation switch
            {
                QueryOperation.Take => Take(sequence, descriptor.IntValue ?? 0),
                QueryOperation.Skip => Skip(sequence, descriptor.IntValue ?? 0),
                _ => sequence
            };
        }

        return plan.TerminalOperation switch
        {
            QueryOperation.Sequence => sequence,
            QueryOperation.ToList => ToList(sequence),
            QueryOperation.Count => Count(sequence),
            QueryOperation.LongCount => LongCount(sequence),
            QueryOperation.Any => Any(sequence),
            QueryOperation.First => First(sequence),
            QueryOperation.FirstOrDefault => FirstOrDefault(sequence),
            _ => sequence
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

    private static IEnumerable<T> Take<T>(IEnumerable<T> data, int count)
    {
        if (count <= 0)
        {
            yield break;
        }

        var taken = 0;
        foreach (var item in data)
        {
            yield return item;
            taken++;
            if (taken >= count)
            {
                yield break;
            }
        }
    }

    private static IEnumerable<T> Skip<T>(IEnumerable<T> data, int count)
    {
        if (count <= 0)
        {
            foreach (var item in data)
            {
                yield return item;
            }
            yield break;
        }

        using var enumerator = data.GetEnumerator();
        while (count > 0 && enumerator.MoveNext())
        {
            count--;
        }

        while (enumerator.MoveNext())
        {
            yield return enumerator.Current;
        }
    }

    private static int ExtractIntegerArgument(MethodCallExpression methodCall, int index)
    {
        var argument = methodCall.Arguments[index];
        if (TryEvaluateExpression(argument, out var value) && value != null)
        {
            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        throw new InvalidOperationException($"无法解析表达式参数 {argument} 为整数。");
    }

    private static bool TryEvaluateExpression(Expression expression, out object? value)
    {
        switch (expression)
        {
            case global::System.Linq.Expressions.ConstantExpression constant:
                value = constant.Value;
                return true;

            case global::System.Linq.Expressions.MemberExpression memberExpression:
                object? instance = null;
                if (memberExpression.Expression != null)
                {
                    if (!TryEvaluateExpression(memberExpression.Expression, out instance))
                    {
                        value = null;
                        return false;
                    }
                }

                value = memberExpression.Member switch
                {
                    FieldInfo field => field.GetValue(instance),
                    PropertyInfo property => property.GetValue(instance),
                    _ => null
                };
                return value != null;

            case global::System.Linq.Expressions.UnaryExpression unary when unary.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked:
                if (TryEvaluateExpression(unary.Operand, out var operandValue) && operandValue != null)
                {
                    value = Convert.ChangeType(operandValue, unary.Type, CultureInfo.InvariantCulture);
                    return true;
                }
                break;
        }

        value = null;
        return false;
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
