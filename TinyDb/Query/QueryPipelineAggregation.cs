using System.Collections;
using System.Globalization;
using LinqExp = System.Linq.Expressions;

namespace TinyDb.Query;

internal static class QueryPipelineAggregation
{
    public static object? Execute(IEnumerable source, LinqExp.MethodCallExpression methodCall)
    {
        QueryExpression? selectorExpr = null;

        if (methodCall.Arguments.Count >= 2 &&
            methodCall.Arguments[1] is LinqExp.UnaryExpression unary &&
            unary.Operand is LinqExp.LambdaExpression lambda)
        {
            var parser = new ExpressionParser();
            selectorExpr = parser.ParseExpression(lambda.Body);
        }

        Func<object?, object?> selector = selectorExpr != null
            ? item => ExpressionEvaluator.EvaluateValue(selectorExpr, item!)!
            : item => item;

        return methodCall.Method.Name switch
        {
            "Sum" => Sum(source, selector, methodCall.Method.ReturnType),
            "Average" => Average(source, selector, methodCall.Method.ReturnType),
            "Min" => Min(source, selector),
            "Max" => Max(source, selector),
            _ => throw new NotSupportedException($"Aggregation {methodCall.Method.Name} is not supported")
        };
    }

    public static decimal AddAggregateValue(decimal current, object? value)
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

    public static object? ConvertValueToType(object? value, Type targetType)
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

    private static object? Sum(IEnumerable items, Func<object?, object?> selector, Type returnType)
    {
        decimal sum = 0;
        foreach (var item in items)
        {
            sum = AddAggregateValue(sum, selector(item));
        }

        return ConvertValueToType(sum, returnType);
    }

    private static object? Average(IEnumerable items, Func<object?, object?> selector, Type returnType)
    {
        decimal sum = 0;
        var count = 0;
        foreach (var item in items)
        {
            sum = AddAggregateValue(sum, selector(item));
            count++;
        }

        return count == 0
            ? ConvertValueToType(0m, returnType)
            : ConvertValueToType(sum / count, returnType);
    }

    private static object? Min(IEnumerable items, Func<object?, object?> selector)
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

    private static object? Max(IEnumerable items, Func<object?, object?> selector)
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
}
