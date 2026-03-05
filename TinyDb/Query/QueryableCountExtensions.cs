using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

public static class QueryableCountExtensions
{
    public static List<T> Count<T>(this IQueryable<T> query, out long totalCount)
    {
        if (query == null) throw new ArgumentNullException(nameof(query));

        if (!TryStripPagination(query.Expression, out var unpagedExpression, out var operations))
        {
            totalCount = query.LongCount();
            return query.ToList();
        }

        IQueryable<T> unpagedQuery = operations.Count > 0
            ? query.Provider.CreateQuery<T>(unpagedExpression)
            : query;

        var allItems = unpagedQuery.ToList();
        totalCount = allItems.LongCount();

        if (operations.Count == 0)
        {
            return allItems;
        }

        IEnumerable<T> paged = allItems;
        foreach (var operation in operations)
        {
            paged = operation.IsSkip
                ? paged.Skip(operation.Count)
                : paged.Take(operation.Count);
        }

        return paged.ToList();
    }

    private static bool TryStripPagination(
        LinqExp.Expression expression,
        out LinqExp.Expression unpagedExpression,
        out List<PaginationOperation> operations)
    {
        operations = new List<PaginationOperation>();
        if (!TryStripPaginationInternal(expression, operations, out unpagedExpression))
        {
            operations.Clear();
            unpagedExpression = expression;
            return false;
        }

        return true;
    }

    private static bool TryStripPaginationInternal(
        LinqExp.Expression expression,
        List<PaginationOperation> operations,
        out LinqExp.Expression rewritten)
    {
        if (expression is not LinqExp.MethodCallExpression methodCall
            || methodCall.Method.DeclaringType != typeof(Queryable))
        {
            rewritten = expression;
            return true;
        }

        var methodName = methodCall.Method.Name;
        if (methodName == "Skip" || methodName == "Take")
        {
            if (!TryGetIntValue(methodCall.Arguments[1], out var count))
            {
                rewritten = expression;
                return false;
            }

            if (!TryStripPaginationInternal(methodCall.Arguments[0], operations, out rewritten))
            {
                return false;
            }

            operations.Add(new PaginationOperation(methodName == "Skip", count));
            return true;
        }

        if (methodCall.Arguments.Count == 0)
        {
            rewritten = expression;
            return true;
        }

        if (!TryStripPaginationInternal(methodCall.Arguments[0], operations, out var sourceRewritten))
        {
            rewritten = expression;
            return false;
        }

        if (ReferenceEquals(sourceRewritten, methodCall.Arguments[0]))
        {
            rewritten = expression;
            return true;
        }

        var args = methodCall.Arguments.ToArray();
        args[0] = sourceRewritten;
        rewritten = methodCall.Update(methodCall.Object, args);
        return true;
    }

    private static bool TryGetIntValue(LinqExp.Expression expression, out int value)
    {
        value = default;

        switch (expression)
        {
            case LinqExp.ConstantExpression c when c.Value is int i:
                value = i;
                return true;

            case LinqExp.UnaryExpression { NodeType: LinqExp.ExpressionType.Convert or LinqExp.ExpressionType.ConvertChecked } u:
                return TryGetIntValue(u.Operand, out value);

            case LinqExp.MemberExpression member:
                if (!TryGetMemberValue(member, out var obj)) return false;
                if (obj is int m)
                {
                    value = m;
                    return true;
                }
                return false;

            default:
                return false;
        }
    }

    private static bool TryGetMemberValue(LinqExp.MemberExpression member, out object? value)
    {
        value = null;

        object? instance = null;
        if (member.Expression != null && !TryEvaluateObject(member.Expression, out instance))
        {
            return false;
        }

        switch (member.Member)
        {
            case FieldInfo field:
                value = field.GetValue(instance);
                return true;

            case PropertyInfo property:
                value = property.GetValue(instance);
                return true;

            default:
                return false;
        }
    }

    private static bool TryEvaluateObject(LinqExp.Expression expression, out object? value)
    {
        value = null;

        switch (expression)
        {
            case LinqExp.ConstantExpression c:
                value = c.Value;
                return true;

            case LinqExp.MemberExpression m:
                return TryGetMemberValue(m, out value);

            default:
                return false;
        }
    }

    private readonly record struct PaginationOperation(bool IsSkip, int Count);
}
