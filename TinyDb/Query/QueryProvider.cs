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
