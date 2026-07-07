using System;
using System.Linq.Expressions;

namespace TinyDb.Query;

internal static partial class ExpressionConstantEvaluator
{
    internal static object? TryEvaluateBinary(System.Linq.Expressions.BinaryExpression binaryExpr)
    {
        var nodeType = binaryExpr.NodeType;

        if (nodeType is ExpressionType.AndAlso or ExpressionType.OrElse)
        {
            var leftValue = TryEvaluateWithoutCompile(binaryExpr.Left);
            if (leftValue is not bool leftBool) return null;

            if (nodeType == ExpressionType.AndAlso && !leftBool) return false;
            if (nodeType == ExpressionType.OrElse && leftBool) return true;

            var rightValue = TryEvaluateWithoutCompile(binaryExpr.Right);
            return rightValue is bool rightBool ? rightBool : null;
        }

        var left = TryEvaluateWithoutCompile(binaryExpr.Left);
        var right = TryEvaluateWithoutCompile(binaryExpr.Right);

        if (left == null) return null;
        if (right == null) return null;

        try
        {
            if (nodeType == ExpressionType.Add) return EvaluateAdd(left, right);
            if (nodeType == ExpressionType.Subtract) return EvaluateSubtract(left, right);
            if (nodeType == ExpressionType.Multiply) return EvaluateMultiply(left, right);
            if (nodeType == ExpressionType.Divide) return EvaluateDivide(left, right);

            if (nodeType == ExpressionType.Equal)
            {
                return TryCompare(left, right, out var equalCompareResult)
                    ? equalCompareResult == 0
                    : Equals(left, right);
            }

            if (nodeType == ExpressionType.NotEqual)
            {
                return TryCompare(left, right, out var notEqualCompareResult)
                    ? notEqualCompareResult != 0
                    : !Equals(left, right);
            }

            if (TryCompare(left, right, out var compareResult))
            {
                if (nodeType == ExpressionType.GreaterThan) return compareResult > 0;
                if (nodeType == ExpressionType.GreaterThanOrEqual) return compareResult >= 0;
                if (nodeType == ExpressionType.LessThan) return compareResult < 0;
                if (nodeType == ExpressionType.LessThanOrEqual) return compareResult <= 0;
            }

            return null;
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException or OverflowException or DivideByZeroException)
        {
            return null;
        }
    }

    internal static bool TryCompare(object left, object right, out int result)
    {
        result = 0;

        if (IsNumericConstant(left) &&
            IsNumericConstant(right) &&
            QueryValueComparer.TryCompare(left, right, out result))
        {
            return true;
        }

        if (left is int li && right is int ri) { result = li.CompareTo(ri); return true; }
        if (left is int li2 && right is long rl2) { result = ((long)li2).CompareTo(rl2); return true; }
        if (left is long ll3 && right is int ri3) { result = ll3.CompareTo((long)ri3); return true; }
        if (left is long ll && right is long rl) { result = ll.CompareTo(rl); return true; }

        if (left is double ld && right is double rd) { result = ld.CompareTo(rd); return true; }
        if (left is double ld2 && right is int ri2) { result = ld2.CompareTo((double)ri2); return true; }
        if (left is int li4 && right is double rd2) { result = ((double)li4).CompareTo(rd2); return true; }
        if (left is double ld3 && right is long rl3) { result = ld3.CompareTo((double)rl3); return true; }
        if (left is long ll4 && right is double rd3) { result = ((double)ll4).CompareTo(rd3); return true; }

        if (left is decimal lm && right is decimal rm) { result = lm.CompareTo(rm); return true; }
        if (left is decimal lm2 && right is int ri5) { result = lm2.CompareTo((decimal)ri5); return true; }
        if (left is int li5 && right is decimal rm2) { result = ((decimal)li5).CompareTo(rm2); return true; }
        if (left is decimal lm3 && right is long rl4) { result = lm3.CompareTo((decimal)rl4); return true; }
        if (left is long ll5 && right is decimal rm3) { result = ((decimal)ll5).CompareTo(rm3); return true; }

        if (left is string ls && right is string rs) { result = string.CompareOrdinal(ls, rs); return true; }
        if (left is DateTime ldt && right is DateTime rdt) { result = ldt.CompareTo(rdt); return true; }
        if (left is TimeSpan lts && right is TimeSpan rts) { result = lts.CompareTo(rts); return true; }

        return false;
    }

    private static bool IsNumericConstant(object value)
    {
        return value is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal;
    }

    internal static object? EvaluateAdd(object left, object right) =>
        (left, right) switch
        {
            (int l, int r) => l + r,
            (long l, long r) => l + r,
            (double l, double r) => l + r,
            (decimal l, decimal r) => l + r,
            (int l, long r) => l + r,
            (long l, int r) => l + r,
            (DateTime l, TimeSpan r) => l + r,
            (TimeSpan l, TimeSpan r) => l + r,
            _ => null
        };

    internal static object? EvaluateSubtract(object left, object right) =>
        (left, right) switch
        {
            (int l, int r) => l - r,
            (long l, long r) => l - r,
            (double l, double r) => l - r,
            (decimal l, decimal r) => l - r,
            (int l, long r) => l - r,
            (long l, int r) => l - r,
            (DateTime l, TimeSpan r) => l - r,
            (DateTime l, DateTime r) => l - r,
            (TimeSpan l, TimeSpan r) => l - r,
            _ => null
        };

    internal static object? EvaluateMultiply(object left, object right) =>
        (left, right) switch
        {
            (int l, int r) => l * r,
            (long l, long r) => l * r,
            (double l, double r) => l * r,
            (decimal l, decimal r) => l * r,
            (int l, long r) => l * r,
            (long l, int r) => l * r,
            (TimeSpan l, double r) => TimeSpan.FromTicks(checked((long)(l.Ticks * r))),
            (double l, TimeSpan r) => TimeSpan.FromTicks(checked((long)(l * r.Ticks))),
            (TimeSpan l, int r) => TimeSpan.FromTicks(checked(l.Ticks * r)),
            (int l, TimeSpan r) => TimeSpan.FromTicks(checked((long)l * r.Ticks)),
            _ => null
        };

    internal static object? EvaluateDivide(object left, object right) =>
        (left, right) switch
        {
            (int l, int r) when r != 0 => l / r,
            (long l, long r) when r != 0 => l / r,
            (double l, double r) when r != 0 => l / r,
            (decimal l, decimal r) when r != 0 => l / r,
            (int l, long r) when r != 0 => l / r,
            (long l, int r) when r != 0 => l / r,
            (TimeSpan l, double r) when r != 0 => TimeSpan.FromTicks(checked((long)(l.Ticks / r))),
            (TimeSpan l, int r) when r != 0 => TimeSpan.FromTicks(l.Ticks / r),
            _ => null
        };

}
