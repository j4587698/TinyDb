using System;

namespace TinyDb.Query;

internal static partial class ExpressionConstantEvaluator
{
    /// <summary>
    /// 尝试求值二元表达式
    /// </summary>
    internal static object? TryEvaluateMethodCall(System.Linq.Expressions.MethodCallExpression methodCallExpr)
    {
        if (methodCallExpr.Object == null)
        {
            if (methodCallExpr.Method.DeclaringType == typeof(Guid) &&
                methodCallExpr.Method.Name == nameof(Guid.NewGuid) &&
                methodCallExpr.Arguments.Count == 0)
            {
                return Guid.NewGuid();
            }

            if (methodCallExpr.Method.DeclaringType == typeof(string))
            {
                if (methodCallExpr.Method.Name == nameof(string.IsNullOrEmpty) && methodCallExpr.Arguments.Count == 1)
                {
                    var value = TryEvaluateWithoutCompile(methodCallExpr.Arguments[0]) as string;
                    return string.IsNullOrEmpty(value);
                }

                if (methodCallExpr.Method.Name == nameof(string.IsNullOrWhiteSpace) && methodCallExpr.Arguments.Count == 1)
                {
                    var value = TryEvaluateWithoutCompile(methodCallExpr.Arguments[0]) as string;
                    return string.IsNullOrWhiteSpace(value);
                }
            }

            return null;
        }

        var target = TryEvaluateWithoutCompile(methodCallExpr.Object);
        if (methodCallExpr.Object is not System.Linq.Expressions.ConstantExpression { Value: null } && target == null)
        {
            return null;
        }

        var args = new object?[methodCallExpr.Arguments.Count];
        for (int i = 0; i < methodCallExpr.Arguments.Count; i++)
        {
            var argValue = TryEvaluateWithoutCompile(methodCallExpr.Arguments[i]);
            if (argValue == null && methodCallExpr.Arguments[i] is not System.Linq.Expressions.ConstantExpression { Value: null })
            {
                return null;
            }
            args[i] = argValue;
        }

        if (methodCallExpr.Method.Name == nameof(object.ToString) && args.Length == 0)
        {
            return target is null ? null : target.ToString();
        }

        if (target is string s)
        {
            var methodName = methodCallExpr.Method.Name;

            if (methodName == nameof(string.Contains) && args.Length == 1 && args[0] is string containsValue)
                return s.Contains(containsValue);
            if (methodName == nameof(string.StartsWith) && args.Length == 1 && args[0] is string startsWithValue)
                return s.StartsWith(startsWithValue);
            if (methodName == nameof(string.EndsWith) && args.Length == 1 && args[0] is string endsWithValue)
                return s.EndsWith(endsWithValue);

            if (methodName == nameof(string.ToLower) && args.Length == 0)
                return s.ToLowerInvariant();
            if (methodName == nameof(string.ToUpper) && args.Length == 0)
                return s.ToUpperInvariant();
            if (methodName == nameof(string.Trim) && args.Length == 0)
                return s.Trim();

            if (methodName == nameof(string.Substring) && args.Length == 1)
                return s.Substring(Convert.ToInt32(args[0]));
            if (methodName == nameof(string.Substring) && args.Length == 2)
                return s.Substring(Convert.ToInt32(args[0]), Convert.ToInt32(args[1]));

            if (methodName == nameof(string.Replace) && args.Length == 2 && args[0] is string oldValue && args[1] is string newValue)
                return s.Replace(oldValue, newValue);

            return null;
        }

        if (target is DateTime dt)
        {
            return methodCallExpr.Method.Name switch
            {
                nameof(DateTime.AddDays) when args.Length == 1 => dt.AddDays(Convert.ToDouble(args[0])),
                nameof(DateTime.AddHours) when args.Length == 1 => dt.AddHours(Convert.ToDouble(args[0])),
                nameof(DateTime.AddMinutes) when args.Length == 1 => dt.AddMinutes(Convert.ToDouble(args[0])),
                nameof(DateTime.AddSeconds) when args.Length == 1 => dt.AddSeconds(Convert.ToDouble(args[0])),
                nameof(DateTime.AddYears) when args.Length == 1 => dt.AddYears(Convert.ToInt32(args[0])),
                nameof(DateTime.AddMonths) when args.Length == 1 => dt.AddMonths(Convert.ToInt32(args[0])),
                _ => null
            };
        }

        return null;
    }

}
