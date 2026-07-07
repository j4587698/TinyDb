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

    private static object? EvaluateBinaryExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(BinaryExpression expression, T entity)
        where T : class
    {
        if (expression.NodeType == ExpressionType.AndAlso)
        {
            var left = EvaluateValue(expression.Left, entity);
            if (left is not bool b || !b) return false;
            var right = EvaluateValue(expression.Right, entity);
            return right is bool rb && rb;
        }
        if (expression.NodeType == ExpressionType.OrElse)
        {
            var left = EvaluateValue(expression.Left, entity);
            if (left is bool b && b) return true;
            var right = EvaluateValue(expression.Right, entity);
            return right is bool rb && rb;
        }

        var leftValue = EvaluateValue(expression.Left, entity);
        var rightValue = EvaluateValue(expression.Right, entity);

        return EvaluateBinary(expression.NodeType, leftValue, rightValue);
    }

    private static object? EvaluateBinaryExpression(BinaryExpression expression, object entity)
    {
        if (expression.NodeType == ExpressionType.AndAlso)
        {
            var left = EvaluateValue(expression.Left, entity);
            if (left is not bool b || !b) return false;
            var right = EvaluateValue(expression.Right, entity);
            return right is bool rb && rb;
        }
        if (expression.NodeType == ExpressionType.OrElse)
        {
            var left = EvaluateValue(expression.Left, entity);
            if (left is bool b && b) return true;
            var right = EvaluateValue(expression.Right, entity);
            return right is bool rb && rb;
        }

        var leftValue = EvaluateValue(expression.Left, entity);
        var rightValue = EvaluateValue(expression.Right, entity);

        return EvaluateBinary(expression.NodeType, leftValue, rightValue);
    }

    private static object? EvaluateBinary(ExpressionType nodeType, object? leftValue, object? rightValue)
    {
        switch (nodeType)
        {
            case ExpressionType.Equal:
            case ExpressionType.NotEqual:
            case ExpressionType.GreaterThan:
            case ExpressionType.GreaterThanOrEqual:
            case ExpressionType.LessThan:
            case ExpressionType.LessThanOrEqual:
                return QueryValueComparer.EvaluateComparison(leftValue, rightValue, nodeType);

            case ExpressionType.Add:
                // Handle string concatenation
                if (leftValue is string || rightValue is string)
                {
                    return (leftValue?.ToString() ?? "") + (rightValue?.ToString() ?? "");
                }
                return EvaluateIntegralMathOp(leftValue, rightValue, (a, b) => a + b, (a, b) => a + b, (a, b) => a + b);
            case ExpressionType.Subtract: return EvaluateIntegralMathOp(leftValue, rightValue, (a, b) => a - b, (a, b) => a - b, (a, b) => a - b);
            case ExpressionType.Multiply: return EvaluateIntegralMathOp(leftValue, rightValue, (a, b) => a * b, (a, b) => a * b, (a, b) => a * b);
            case ExpressionType.Divide: return EvaluateIntegralMathOp(leftValue, rightValue, (a, b) => a / b, (a, b) => a / b, (a, b) => a / b);

            default: throw new NotSupportedException($"Binary operation {nodeType} is not supported");
        }
    }

    private static object? EvaluateIntegralMathOp(object? left, object? right, Func<double, double, double> doubleFunc, Func<decimal, decimal, decimal> decimalFunc, Func<long, long, long> integralFunc)
    {
        if (left == null || right == null) return null;

        if (left is decimal || right is decimal || left is Decimal128 || right is Decimal128)
        {
            return decimalFunc(ToDecimal(left), ToDecimal(right));
        }

        if (TryGetIntegral(left, out var leftLong, out var leftRequiresLong) &&
            TryGetIntegral(right, out var rightLong, out var rightRequiresLong))
        {
            var result = integralFunc(leftLong, rightLong);
            if (!leftRequiresLong &&
                !rightRequiresLong &&
                result >= int.MinValue &&
                result <= int.MaxValue)
            {
                return (int)result;
            }

            return result;
        }

        return EvaluateMathOp(left, right, doubleFunc, decimalFunc);
    }

    internal static object? EvaluateMathOp(object? left, object? right, Func<double, double, double> doubleFunc, Func<decimal, decimal, decimal> decimalFunc)
    {
        if (left == null || right == null) return null;

        if (left is decimal || right is decimal || left is Decimal128 || right is Decimal128)
        {
            return decimalFunc(ToDecimal(left), ToDecimal(right));
        }

        var dResult = doubleFunc(ToDouble(left), ToDouble(right));

        if ((left is int || left is long) && (right is int || right is long) && dResult == Math.Floor(dResult))
        {
            if (dResult >= int.MinValue && dResult <= int.MaxValue) return (int)dResult;
            return (long)dResult;
        }

        return dResult;
    }

    private static bool TryGetIntegral(object value, out long result, out bool requiresLongResult)
    {
        switch (value)
        {
            case BsonInt32 bsonInt32:
                result = bsonInt32.Value;
                requiresLongResult = false;
                return true;
            case BsonInt64 bsonInt64:
                result = bsonInt64.Value;
                requiresLongResult = true;
                return true;
            case byte byteValue:
                result = byteValue;
                requiresLongResult = false;
                return true;
            case sbyte sbyteValue:
                result = sbyteValue;
                requiresLongResult = false;
                return true;
            case short shortValue:
                result = shortValue;
                requiresLongResult = false;
                return true;
            case ushort ushortValue:
                result = ushortValue;
                requiresLongResult = false;
                return true;
            case int intValue:
                result = intValue;
                requiresLongResult = false;
                return true;
            case uint uintValue:
                result = uintValue;
                requiresLongResult = true;
                return true;
            case long longValue:
                result = longValue;
                requiresLongResult = true;
                return true;
            default:
                result = default;
                requiresLongResult = false;
                return false;
        }
    }

    private static decimal ToDecimal(object val)
    {
        if (val is Decimal128 d128) return d128.ToDecimal();
        return Convert.ToDecimal(val);
    }

    private static double ToDouble(object? val)
    {
        if (val == null) return 0.0;

        // 快速路径：直接匹配常见数值类型
        if (val is double d) return d;
        if (val is int i) return i;
        if (val is long l) return l;
        if (val is float f) return f;
        if (val is decimal dec) return (double)dec;
        if (val is Decimal128 d128) return (double)d128.ToDecimal(); // 关键修复：支持 Decimal128 结构体
        if (val is byte b) return b;
        if (val is short s) return s;

        // BsonValue 转换
        if (val is BsonDouble bd) return bd.Value;
        if (val is BsonInt32 bi) return bi.Value;
        if (val is BsonInt64 bl) return bl.Value;

        // 备选路径：字符串或其它类型
        try
        {
            return Convert.ToDouble(val, CultureInfo.InvariantCulture);
        }
        catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException or ArgumentException)
        {
            throw new InvalidOperationException($"Unable to convert value '{val}' ({val.GetType().FullName}) to double.", ex);
        }
    }

}
