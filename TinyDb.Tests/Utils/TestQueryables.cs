using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace TinyDb.Tests.Utils;

internal static class TestQueryables
{
    public static IQueryable<T> InMemory<T>(IEnumerable<T> source)
    {
        if (source == null) throw new ArgumentNullException(nameof(source));

        return new TestQueryable<T>(new TestQueryProvider());
    }

    private sealed class TestQueryProvider : IQueryProvider
    {
        public IQueryable CreateQuery(Expression expression)
        {
            throw new NotSupportedException("TestQueryProvider only supports generic CreateQuery<T>.");
        }

        public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
        {
            return new TestQueryable<TElement>(this, expression);
        }

        public object? Execute(Expression expression)
        {
            throw new NotSupportedException("TestQueryable is expression-only and cannot execute queries.");
        }

        public TResult Execute<TResult>(Expression expression)
        {
            throw new NotSupportedException("TestQueryable is expression-only and cannot execute queries.");
        }
    }

    private sealed class TestQueryable<T> : IOrderedQueryable<T>
    {
        public TestQueryable(TestQueryProvider provider)
        {
            Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            Expression = Expression.Constant(this);
        }

        public TestQueryable(TestQueryProvider provider, Expression expression)
        {
            Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            Expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        public Type ElementType => typeof(T);
        public Expression Expression { get; }
        public IQueryProvider Provider { get; }

        public IEnumerator<T> GetEnumerator()
        {
            throw new NotSupportedException("TestQueryable is expression-only and cannot be enumerated.");
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
