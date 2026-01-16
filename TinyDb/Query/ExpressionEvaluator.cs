using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using TinyDb.Bson;
using TinyDb.Serialization;

namespace TinyDb.Query;

public static class ExpressionEvaluator
{
    /// <summary>
    /// 评估针对类型化实体的查询表达式。
    /// </summary>
    /// <typeparam name="T">实体类型。</typeparam>
    /// <param name="expression">要评估的查询表达式。</param>
    /// <param name="entity">实体实例。</param>
    /// <returns>如果实体匹配表达式则为 true；否则为 false。</returns>
    public static bool Evaluate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExpression expression, T entity)
        where T : class, new()
    {
        return expression switch
        {
            ConstantExpression constExpr => EvaluateConstantExpression(constExpr),
            BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, entity),
            MemberExpression memberExpr => EvaluateMemberExpression(memberExpr, entity),
            ParameterExpression paramExpr => true,
            FunctionExpression funcExpr => EvaluateFunctionExpression(funcExpr, entity),
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported")
        };
    }

    /// <summary>
    /// 评估针对 BSON 文档的查询表达式。
    /// </summary>
    /// <param name="expression">要评估的查询表达式。</param>
    /// <param name="document">BSON 文档。</param>
    /// <returns>如果文档匹配表达式则为 true；否则为 false。</returns>
    public static bool Evaluate(QueryExpression expression, BsonDocument document)
    {
        return expression switch
        {
            ConstantExpression constExpr => EvaluateConstantExpression(constExpr),
            BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, document),
            MemberExpression memberExpr => EvaluateMemberExpression(memberExpr, document),
            ParameterExpression paramExpr => true,
            FunctionExpression funcExpr => EvaluateFunctionExpression(funcExpr, document),
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported")
        };
    }

    private static bool EvaluateConstantExpression(ConstantExpression expression)
    {
        if (expression.Value is bool boolValue)
        {
            return boolValue;
        }

        throw new InvalidOperationException($"Constant expression must be boolean, got {expression.Value?.GetType().Name}");
    }

    private static bool EvaluateBinaryExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(BinaryExpression expression, T entity)
        where T : class, new()
    {
        var leftValue = EvaluateExpressionValue(expression.Left, entity);
        var rightValue = EvaluateExpressionValue(expression.Right, entity);

        return EvaluateBinary(expression.NodeType, leftValue, rightValue);
    }

    private static bool EvaluateBinaryExpression(BinaryExpression expression, BsonDocument document)
    {
        var leftValue = EvaluateExpressionValue(expression.Left, document);
        var rightValue = EvaluateExpressionValue(expression.Right, document);

        return EvaluateBinary(expression.NodeType, leftValue, rightValue);
    }

    private static bool EvaluateBinary(ExpressionType nodeType, object? leftValue, object? rightValue)
    {
        return nodeType switch
        {
            ExpressionType.Equal => Equals(leftValue, rightValue),
            ExpressionType.NotEqual => !Equals(leftValue, rightValue),
            ExpressionType.GreaterThan => Compare(leftValue, rightValue) > 0,
            ExpressionType.GreaterThanOrEqual => Compare(leftValue, rightValue) >= 0,
            ExpressionType.LessThan => Compare(leftValue, rightValue) < 0,
            ExpressionType.LessThanOrEqual => Compare(leftValue, rightValue) <= 0,
            ExpressionType.AndAlso => leftValue is bool leftBool && rightValue is bool rightBool && leftBool && rightBool,
            ExpressionType.OrElse => leftValue is bool leftBool2 && rightValue is bool rightBool2 && (leftBool2 || rightBool2),
            _ => throw new NotSupportedException($"Binary operation {nodeType} is not supported")
        };
    }

    private static bool EvaluateMemberExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(MemberExpression expression, T entity)
        where T : class, new()
    {
        var value = GetMemberValue(expression, entity);
        return value is bool boolValue ? boolValue : value != null;
    }

    private static bool EvaluateMemberExpression(MemberExpression expression, BsonDocument document)
    {
        var value = GetMemberValue(expression, document);
        return value is bool boolValue ? boolValue : value != null;
    }

    private static object? EvaluateExpressionValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExpression expression, T entity)
        where T : class, new()
    {
        return expression switch
        {
            ConstantExpression constExpr => constExpr.Value,
            MemberExpression memberExpr => GetMemberValue(memberExpr, entity),
            ParameterExpression => entity,
            BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, entity),
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported for value evaluation")
        };
    }

    private static object? EvaluateExpressionValue(QueryExpression expression, BsonDocument document)
    {
        return expression switch
        {
            ConstantExpression constExpr => constExpr.Value,
            MemberExpression memberExpr => GetMemberValue(memberExpr, document),
            ParameterExpression => document,
            BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, document),
            _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported for value evaluation")
        };
    }

    private static object? GetMemberValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(MemberExpression expression, T entity)
        where T : class, new()
    {
        if (AotHelperRegistry.TryGetAdapter<T>(out var adapter))
        {
            return adapter.GetPropertyValue(entity, expression.MemberName);
        }

        return EntityMetadata<T>.TryGetProperty(expression.MemberName, out var property)
            ? property.GetValue(entity)
            : null;
    }

    private static object? GetMemberValue(MemberExpression expression, BsonDocument document)
    {
        // Try camelCase first (convention), then exact match
        var name = expression.MemberName;
        var camelName = ToCamelCase(name);
        
        if (document.TryGetValue(camelName, out var val)) return val.RawValue;
        if (document.TryGetValue(name, out val)) return val.RawValue;
        
        // Handle _id specially if mapped from "Id"
        if (name == "Id" && document.TryGetValue("_id", out val)) return val.RawValue;

        return null;
    }

    private static bool EvaluateFunctionExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(FunctionExpression expression, T entity)
        where T : class, new()
    {
        var targetValue = EvaluateExpressionValue(expression.Target, entity);
        var argumentValue = EvaluateExpressionValue(expression.Argument, entity);
        return EvaluateFunction(expression.FunctionName, targetValue, argumentValue);
    }

    private static bool EvaluateFunctionExpression(FunctionExpression expression, BsonDocument document)
    {
        var targetValue = EvaluateExpressionValue(expression.Target, document);
        var argumentValue = EvaluateExpressionValue(expression.Argument, document);
        return EvaluateFunction(expression.FunctionName, targetValue, argumentValue);
    }

    private static bool EvaluateFunction(string functionName, object? targetValue, object? argumentValue)
    {
        return functionName switch
        {
            "Contains" => EvaluateContainsFunction(targetValue, argumentValue),
            "StartsWith" => EvaluateStartsWithFunction(targetValue, argumentValue),
            "EndsWith" => EvaluateEndsWithFunction(targetValue, argumentValue),
            _ => throw new NotSupportedException($"Function '{functionName}' is not supported")
        };
    }

    private static bool EvaluateContainsFunction(object? targetValue, object? argumentValue)
    {
        if (targetValue is string targetStr && argumentValue is string argStr)
        {
            return targetStr.Contains(argStr);
        }

        if (targetValue is System.Collections.IEnumerable targetEnum)
        {
            foreach (var item in targetEnum)
            {
                var actualItem = item is BsonValue bv ? bv.RawValue : item;
                if (Equals(actualItem, argumentValue))
                {
                    return true;
                }
            }
            return false;
        }

        throw new NotSupportedException($"Contains operation is not supported for types {targetValue?.GetType().Name} and {argumentValue?.GetType().Name}");
    }

    private static bool EvaluateStartsWithFunction(object? targetValue, object? argumentValue)
    {
        if (targetValue is string targetStr && argumentValue is string argStr)
        {
            return targetStr.StartsWith(argStr);
        }

        throw new NotSupportedException($"StartsWith operation is not supported for types {targetValue?.GetType().Name} and {argumentValue?.GetType().Name}");
    }

    private static bool EvaluateEndsWithFunction(object? targetValue, object? argumentValue)
    {
        if (targetValue is string targetStr && argumentValue is string argStr)
        {
            return targetStr.EndsWith(argStr);
        }

        throw new NotSupportedException($"EndsWith operation is not supported for types {targetValue?.GetType().Name} and {argumentValue?.GetType().Name}");
    }

    private static int Compare(object? left, object? right)
    {
        if (left == null && right == null) return 0;
        if (left == null) return -1;
        if (right == null) return 1;

        // Try to unify types if one is numeric
        if (IsNumericType(left) && IsNumericType(right))
        {
            if (left is decimal || right is decimal)
            {
                return Convert.ToDecimal(left).CompareTo(Convert.ToDecimal(right));
            }
            return Convert.ToDouble(left).CompareTo(Convert.ToDouble(right));
        }

        // Fix logic for different types but compatible (e.g. int vs long)
        // IsNumericType covers this above via ToDouble/ToDecimal

        if (left is IComparable leftComparable)
        {
            try
            {
                // Basic comparison
                return leftComparable.CompareTo(right);
            }
            catch (ArgumentException)
            {
                // Fallback for different types not handled by IsNumericType?
            }
        }

        if (left is string leftStr && right is string rightStr)
        {
            return string.Compare(leftStr, rightStr, StringComparison.Ordinal);
        }

        return string.Compare(left.ToString(), right.ToString(), StringComparison.Ordinal);
    }

    private static bool IsNumericType(object value)
    {
        return value is sbyte || value is byte || value is short || value is ushort ||
               value is int || value is uint || value is long || value is ulong ||
               value is float || value is double || value is decimal;
    }

    private static string ToCamelCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        if (name.Length == 1) return name.ToLowerInvariant();
        return char.ToLowerInvariant(name[0]) + name.Substring(1);
    }
}
