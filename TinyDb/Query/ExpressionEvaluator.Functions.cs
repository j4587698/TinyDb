using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Utils;

namespace TinyDb.Query;

public static partial class ExpressionEvaluator
{

    private static object?[] EvaluateArguments(IReadOnlyList<QueryExpression> arguments, object entity)
    {
        var values = new object?[arguments.Count];
        for (var i = 0; i < arguments.Count; i++)
        {
            values[i] = EvaluateValue(arguments[i], entity);
        }

        return values;
    }

    private static object? EvaluateFunctionExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(FunctionExpression expression, T entity)
        where T : class
    {
        var targetValue = expression.Target != null ? EvaluateValue(expression.Target, entity) : null;
        var argValues = EvaluateArguments(expression.Arguments, entity);

        return EvaluateFunction(expression.FunctionName, targetValue, argValues);
    }

    private static object? EvaluateFunctionExpression(FunctionExpression expression, object entity)
    {
        var targetValue = expression.Target != null ? EvaluateValue(expression.Target, entity) : null;
        var argValues = EvaluateArguments(expression.Arguments, entity);

        return EvaluateFunction(expression.FunctionName, targetValue, argValues);
    }

    private static object? EvaluateFunction(string functionName, object? targetValue, object?[] args)
    {
        if (targetValue == null && args.Length == 0)
        {
            if (functionName == RuntimeFunctionNames.DateTimeNow) return DateTime.Now;
            if (functionName == RuntimeFunctionNames.DateTimeUtcNow) return DateTime.UtcNow;
            if (functionName == RuntimeFunctionNames.DateTimeToday) return DateTime.Today;
        }

        if (targetValue == null && args.Length > 0 && args[0] is System.Collections.IEnumerable && IsEnumerableFunction(functionName))
        {
            targetValue = args[0];
            args = args.Skip(1).ToArray();
        }

        if (functionName == "ToString" && args.Length == 0 && targetValue != null)
        {
            return targetValue.ToString();
        }

        if (targetValue is string str)
        {
            if (functionName == "Contains") return EvaluateStringPredicate(args, str.Contains);
            if (functionName == "StartsWith") return EvaluateStringPredicate(args, str.StartsWith);
            if (functionName == "EndsWith") return EvaluateStringPredicate(args, str.EndsWith);
            if (functionName == "ToLower") return str.ToLowerInvariant();
            if (functionName == "ToUpper") return str.ToUpperInvariant();
            if (functionName == "Trim") return str.Trim();
            if (functionName == "Substring")
            {
                if (args.Length == 1) return str.Substring(Convert.ToInt32(args[0]));
                if (args.Length == 2) return str.Substring(Convert.ToInt32(args[0]), Convert.ToInt32(args[1]));
                throw new ArgumentException("Invalid arguments for Substring");
            }
            if (functionName == "Replace") return args.Length == 2 && args[0] is string o && args[1] is string n ? str.Replace(o, n) : str;

            throw new NotSupportedException($"String function '{functionName}' is not supported");
        }

        if (targetValue is System.Collections.IEnumerable enumerable)
        {
            if (functionName == "Contains" && args.Length == 1)
            {
                foreach (var item in enumerable)
                {
                    if (QueryValueComparer.EvaluateComparison(item, args[0], ExpressionType.Equal)) return true;
                }
                return false;
            }
            if (functionName == "Count")
            {
                if (targetValue is System.Collections.ICollection col) return col.Count;
                if (targetValue is BsonArray ba) return ba.Count;
                int count = 0;
                foreach (var _ in enumerable) count++;
                return count;
            }
            if (functionName == "Sum" || functionName == "Average" || functionName == "Min" || functionName == "Max")
            {
                var selector = BuildSelector(args);
                return EvaluateEnumerableAggregate(functionName, enumerable, selector);
            }
        }

        if (targetValue == null)
        {
            if (functionName == "Abs") return EvaluateMathOneArg(args, Math.Abs, Math.Abs);
            if (functionName == "Ceiling") return EvaluateMathOneArg(args, Math.Ceiling, Math.Ceiling);
            if (functionName == "Floor") return EvaluateMathOneArg(args, Math.Floor, Math.Floor);
            if (functionName == "Round")
            {
                if (args.Length == 1) return EvaluateMathOneArg(args, Math.Round, Math.Round);
                if (args.Length == 2) return EvaluateMathTwoArgs(args, (v, p) => Math.Round(v, (int)p), (v, p) => Math.Round(v, (int)p));
            }
            else if (functionName == "Min")
            {
                return EvaluateMathTwoArgs(args, Math.Min, Math.Min);
            }
            else if (functionName == "Max")
            {
                return EvaluateMathTwoArgs(args, Math.Max, Math.Max);
            }
            else if (functionName == "Pow")
            {
                return args.Length == 2 ? Math.Pow(ToDouble(args[0]), ToDouble(args[1])) : 0.0;
            }
            else if (functionName == "Sqrt")
            {
                return args.Length == 1 ? Math.Sqrt(ToDouble(args[0])) : 0.0;
            }
        }

        if (targetValue is DateTime dt)
        {
            if (functionName == "AddDays") return dt.AddDays(ToDouble(args[0]));
            if (functionName == "AddHours") return dt.AddHours(ToDouble(args[0]));
            if (functionName == "AddMinutes") return dt.AddMinutes(ToDouble(args[0]));
            if (functionName == "AddSeconds") return dt.AddSeconds(ToDouble(args[0]));
            if (functionName == "AddYears") return dt.AddYears(ToInt32(args[0]));
            if (functionName == "AddMonths") return dt.AddMonths(ToInt32(args[0]));
            throw new NotSupportedException($"DateTime function '{functionName}' is not supported");
        }

        throw new NotSupportedException($"Function '{functionName}' is not supported for type {targetValue?.GetType().Name ?? "null"}");
    }

    private static bool EvaluateStringPredicate(
        object?[] args,
        Func<string, StringComparison, bool> predicate)
    {
        if (args.Length == 1 && args[0] is string value)
        {
            return predicate(value, StringComparison.Ordinal);
        }

        if (args.Length == 2 &&
            args[0] is string valueWithComparison &&
            TryGetStringComparison(args[1], out var comparison))
        {
            return predicate(valueWithComparison, comparison);
        }

        return false;
    }

    private static bool TryGetStringComparison(object? value, out StringComparison comparison)
    {
        if (value is StringComparison stringComparison)
        {
            comparison = stringComparison;
            return true;
        }

        if (value is int intValue && Enum.IsDefined(typeof(StringComparison), intValue))
        {
            comparison = (StringComparison)intValue;
            return true;
        }

        comparison = default;
        return false;
    }

    private static bool IsEnumerableFunction(string functionName)
    {
        return EnumerableFunctionNames.Contains(functionName);
    }

    private static readonly HashSet<string> EnumerableFunctionNames = new(StringComparer.Ordinal)
    {
        "Contains",
        "Count",
        "Sum",
        "Average",
        "Min",
        "Max"
    };

    private static Func<object, object> BuildSelector(object?[] args)
    {
        if (args.Length == 0) return item => item;
        if (args.Length == 1 && args[0] is LambdaExpression lambda)
        {
            var parser = new ExpressionParser();
            var selectorExpr = parser.ParseExpression(lambda.Body);
            return item => EvaluateValue(selectorExpr, item)!;
        }

        return item => args[0] ?? item;
    }

    private static object? EvaluateEnumerableAggregate(string functionName, System.Collections.IEnumerable enumerable, Func<object, object> selector)
    {
        if (enumerable is QueryPipeline.AotGrouping aotGroup)
        {
            return functionName switch
            {
                "Sum" => aotGroup.Sum(selector),
                "Average" => aotGroup.Average(selector),
                "Min" => aotGroup.Min(selector),
                "Max" => aotGroup.Max(selector),
                _ => throw new NotSupportedException($"Aggregation {functionName} is not supported")
            };
        }

        var items = enumerable.Cast<object>().ToList();
        if (functionName == "Sum")
        {
            decimal sum = 0m;
            foreach (var item in items)
            {
                sum = AddAggregateValue(sum, selector(item));
            }
            return sum;
        }
        if (functionName == "Average")
        {
            if (items.Count == 0) return 0m;
            decimal sum = 0m;
            foreach (var item in items)
            {
                sum = AddAggregateValue(sum, selector(item));
            }
            return sum / items.Count;
        }
        if (functionName == "Min")
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
        if (functionName == "Max")
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

        throw new NotSupportedException($"Aggregation {functionName} is not supported");
    }

    private static object EvaluateMathOneArg(object?[] args, Func<double, double> doubleFunc, Func<decimal, decimal> decimalFunc)
    {
        if (args.Length != 1) throw new ArgumentException("Function requires 1 argument");
        var val = args[0];
        if (val is int i) return (int)doubleFunc(i);
        if (val is long l) return (long)doubleFunc(l);
        if (val is double d) return doubleFunc(d);
        if (val is decimal dec) return decimalFunc(dec);
        if (val is Decimal128 d128) return new Decimal128(decimalFunc(d128.ToDecimal()));

        return doubleFunc(ToDouble(val));
    }

    private static object EvaluateMathTwoArgs(object?[] args, Func<double, double, double> doubleFunc, Func<decimal, decimal, decimal> decimalFunc)
    {
        if (args.Length != 2) throw new ArgumentException("Function requires 2 arguments");
        var v1 = args[0];
        var v2 = args[1];

        if (v1 == null || v2 == null) return 0.0;

        if (v1 is decimal || v2 is decimal || v1 is Decimal128 || v2 is Decimal128)
            return decimalFunc(ToDecimal(v1), ToDecimal(v2));

        return doubleFunc(ToDouble(v1), ToDouble(v2));
    }

    private static decimal AddAggregateValue(decimal current, object? value)
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

    private static int ToInt32(object? value)
    {
        if (value == null) return 0;
        if (value is int i) return i;
        if (value is decimal dec) return Convert.ToInt32(dec, CultureInfo.InvariantCulture);
        if (value is Decimal128 d128) return Convert.ToInt32(d128.ToDecimal(), CultureInfo.InvariantCulture);
        if (value is BsonDecimal128 bsonDecimal128) return Convert.ToInt32(bsonDecimal128.Value.ToDecimal(), CultureInfo.InvariantCulture);

        return Convert.ToInt32(ToDouble(value));
    }

}
