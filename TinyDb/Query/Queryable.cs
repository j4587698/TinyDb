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
